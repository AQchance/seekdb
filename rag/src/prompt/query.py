"""Prompts for query/answering functionality."""

# System prompt for the document Q&A assistant
QUERY_SYSTEM_PROMPT = "你是一个专业的文档问答助手，能够基于提供的文档内容准确回答问题。"

# User prompt template for answering questions based on document content
# QUERY_USER_PROMPT_TEMPLATE = """基于以下文档内容回答问题。如果文档中没有相关信息，请说明无法从提供的文档中找到答案。
#
# 文档内容：
# {context_text}
#
# 问题：{question}
#
# 请基于上述文档内容回答问题，确保答案准确、完整。如果文档中没有相关信息，请明确说明。"""

QUERY_USER_PROMPT_TEMPLATE = """你将看到多个文档片段，每个片段都包含 filename、page 和 content。

文档片段：
{context_text}

问题：
{question}

任务：
- 直接回答问题本身
- 不要说明答案来源
- 不要解释推理过程
- 如果文档中没有足够信息回答问题，只根据能理解的内容做回答。
- 不要说根据文档内容等等额外的话。
- 比较类问题回答结合文档中提到的数值比较。
输出严格的 JSON，不要包含任何多余文本：
{{
  "content": "...",
  "filename": "...",
  "page": "...",
}}

说明：
- content：问题的直接答案，简洁准确
- filename：你觉得对回答贡献最大的单一文档路径
- page：以及与该文档匹配的页码
- filename 必须从文档片段中原样选择，不允许修改或编造。page 必须是对应文档片段中的 page，不允许猜测。

"""
