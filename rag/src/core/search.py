import logging
import os
from numpy import dot
from numpy.linalg import norm
import dotenv
import numpy   as  np
from src.integrations.embedding import generate_response as generate_embedding
from src.integrations.llm import generate_response as generate_llm_response
from src.prompt import QUERY_SYSTEM_PROMPT, QUERY_USER_PROMPT_TEMPLATE
from src.storage.oceanbase import get_or_create_client
from src.util import Answer
import json
from typing import List, Dict, Any, Set
logger = logging.getLogger(__name__)

dotenv.load_dotenv()

# OceanBase connection parameters
OCEANBASE_URI = os.getenv("OCEANBASE_URI")
OCEANBASE_USER = os.getenv("OCEANBASE_USER")
OCEANBASE_PASSWORD = os.getenv("OCEANBASE_PASSWORD")
OCEANBASE_DBNAME = os.getenv("OCEANBASE_DBNAME")

# Table name for storing document chunks
TABLE_NAME = "rag_documents"

# Number of top results to retrieve
TOP_K = 20

QUERY_VARIATIONS = 5


def generate_query_variations(question: str) :
    try:
        # 使用LLM生成同义重写
        prompt = f"""请为以下问题生成{QUERY_VARIATIONS - 1}个不同但意义相同的表达方式，保持核心意思不变：

        原始问题：{question}

        要求：
        1. 每个表达方式都应与原始问题意思相同
        2. 使用不同的措辞和句式
        3. 保持专业性和准确性
        4. 用中文回答

        请以JSON数组格式输出，格式如下：
        ["问题变体1", "问题变体2", ...]"""

        messages = [
            {
                "role": "system",
                "content": "你是一个专业的同义改写助手，负责生成问题的不同表达方式。"
            },
            {"role": "user", "content": prompt}
        ]

        response = generate_llm_response(messages)

        # 解析响应
        try:
            variations = json.loads(response)
            if isinstance(variations, list) and len(variations) >= 1:
                # 确保包含原始问题，并去重
                all_queries = [question] + variations
                unique_queries = []
                seen = set()
                for q in all_queries:
                    if q not in seen:
                        seen.add(q)
                        unique_queries.append(q)
                return unique_queries[:QUERY_VARIATIONS]
        except json.JSONDecodeError:
            # 如果无法解析JSON，使用简单的方法生成变体
            logger.warning("Failed to parse LLM response for query variations")

    except Exception as e:
        logger.error(f"Error generating query variations: {e}")

    # 备用方案：返回原始问题和一些简单的变体
    variations = [question]

    # 添加一些简单的变体（可以根据需要扩展）
    simple_variants = [
        f"请问：{question}",
        f"我想了解：{question}",
        f"请解释：{question}"
    ]

    for variant in simple_variants:
        if len(variations) < QUERY_VARIATIONS:
            variations.append(variant)

    return variations


def deduplicate_results(results: List[Dict]) -> List[Dict]:
    """
    基于source_id去重结果。

    Args:
        results: 搜索结果列表

    Returns:
        去重后的结果列表
    """
    if not results:
        return results

    # 使用source_id进行去重
    seen_ids = set()
    unique_results = []

    for result in results:
        source_id = result.get('source_id')

        # 如果有source_id且未出现过，则保留
        if source_id and source_id not in seen_ids:
            seen_ids.add(source_id)
            unique_results.append(result)
        elif not source_id:
            # 如果没有source_id，直接保留（避免丢失数据）
            unique_results.append(result)

    logger.debug(f"Deduplication: {len(results)} -> {len(unique_results)} results")
    return unique_results


def search_with_query(query: str, question_embedding, client, top_k: int) -> List[Dict]:
        fts_query = {
            "bool": {
                "must": [
                    {
                        "query_string": {
                            "fields": ["content"],
                            "type": "best_fields",
                            "query": query,
                            "minimum_should_match": "20%",
                        }
                    }
                ],
            }
        }

        # Build the hybrid search request
        search_request = {
            "query": fts_query,
            "knn": {
                "field": "vector",
                "k": top_k * 2,
                "num_candidates": top_k * 4,
                "query_vector": question_embedding,
            },
            "from": 0,
            "size": top_k,
        }

        # Perform hybrid search
        search_results = client.search(index=TABLE_NAME, body=search_request)
        return search_results
def search(question: str) -> Answer:
    """
    Query the RAG system with a question and return an answer.

    Args:
        question: The question to answer

    Returns:
        Answer object containing the question, answer, filename, and page
    """
    answer = Answer(
        question=question,
    )

    logger.debug(f"Processing query: '{question[:100]}...'")

    try:
        # Generate embedding for the question
        logger.debug("Generating embedding for question...")
        question_embedding = generate_embedding(question)
        questions=generate_query_variations(question)
        # Initialize OceanBase client
        client = get_or_create_client()
        all_results = []

        # 为每个问题变体执行搜索
        for i, query_variant in enumerate(questions):
            logger.debug(f"Searching with query variant {i + 1}/{len(questions)}: '{query_variant[:50]}...'")

            # 为每个变体生成嵌入向量（第一个已经是原始问题的嵌入）
            if i == 0:
                query_embedding = question_embedding
            else:
                try:
                    query_embedding = generate_embedding(query_variant)
                except Exception as e:
                    logger.warning(f"Failed to generate embedding for variant, using original: {e}")
                    query_embedding = question_embedding

            # 执行搜索
            search_results = search_with_query(query_variant, query_embedding, client, TOP_K)

            logger.debug(f"Query variant {i + 1} returned {len(search_results)} results")
            all_results.extend(search_results)

        logger.debug(f"Total results from all query variants: {len(all_results)}")
        search_results = deduplicate_results(all_results)
        search_results.sort(key=lambda x: x["_score"], reverse=True)

        # Build hybrid search query
        # First, build a simple full-text search query

        # Build the hybrid search request

        # Perform hybrid search

        content1 = []
        for result in search_results:
            content1.append(result.get("content", ""))
        if not search_results or len(search_results) == 0:
            logger.warning(
                f"No search results found for question: '{question[:50]}...'"
            )
            answer.answer = "抱歉，我没有找到相关的信息来回答这个问题。"
            return answer

        logger.debug(f"Found {len(search_results)} search results")

        # Extract relevant context from search results
        contexts = []
        filenames = []
        pages = []
        for doc in search_results[:TOP_K]:
            if isinstance(doc['vector'], str):
                # 将字符串 JSON 转成 list
                doc['vector'] = json.loads(doc['vector'])
            # 转成 numpy float32
            doc['vector'] = np.array(doc['vector'], dtype=np.float32)

        def cosine(a, b):
            return dot(a, b) / (norm(a) * norm(b) + 1e-8)
        #
        # reranked = sorted(
        #     search_results,
        #     key=lambda doc: cosine(question_embedding, doc['vector']),
        #     reverse=True
        # )[:TOP_K]

        for result in search_results[:50]:
            content = result.get("content", "")
            filename = result.get("filename", "")
            page = result.get("page", 0)

            # if content:
            #     contexts.append(content)
            #     if filename:
            #         filenames.append(filename)
            #     if page:
            #         pages.append(page)
            contexts.append(
                {
                    "content": content,
                    "filename": filename,
                    "page": page,
                }
            )


        if not contexts:
            logger.warning(
                f"No valid contexts extracted from search results for question: '{question[:50]}...'"
            )
            answer.answer = "抱歉，我没有找到相关的信息来回答这个问题。"
            return answer
        context_blocks = []

        for i, c in enumerate(contexts, start=1):
            block = (
                f"【文档 {i}】\n"
                f"filename: {c['filename']}\n"
                f"page: {c['page']}\n"
                f"content:\n{c['content']}"
            )

            context_blocks.append(block)

        context_text = "\n\n".join(context_blocks)

        logger.debug(
            f"Extracted {len(contexts)} valid contexts, total length: {sum(len(c) for c in contexts)} chars"
        )

        # Combine contexts
        # context_text = "\n\n".join(contexts)

        # Use the most common filename and page from results
        # if filenames:
        #     answer.filename = max(set(filenames), key=filenames.count)
        # if pages:
        #     answer.page = max(set(pages), key=pages.count)
#todo 这个是导致有时候file 里面 没有这个page的原因
        # Generate answer using LLM
        logger.debug(f"Generating answer using LLM model ...")
        prompt = QUERY_USER_PROMPT_TEMPLATE.format(
            context_text=context_text,
            question=question,
        )

        messages = [
            {
                "role": "system",
                "content": QUERY_SYSTEM_PROMPT,
            },
            {"role": "user", "content": prompt},
        ]

        # answer.answer = generate_llm_response(messages)
        raw = generate_llm_response(messages)
        data = json.loads(raw)
        answer.answer = data["content"]
        answer.filename = data["filename"]
        answer.page = int(data["page"])
        logger.debug(
            f"Query completed: question='{question[:50]}...', filename='{answer.filename}', page={answer.page}"
        )

    except Exception as e:
        logger.error(f"Error in query: {e}", exc_info=True)
        answer.answer = f"查询过程中发生错误：{str(e)}"
    return answer
