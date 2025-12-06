# SeekDB 倒排索引存储架构详解

## 概述

SeekDB 是 OceanBase 推出的轻量级、嵌入式、面向 AI 应用的原生搜索数据库，在单一引擎中统一支持向量、全文、JSON、关系型及 GIS 数据。本文档详细介绍 SeekDB 中倒排索引（Inverted Index）的内存存储和磁盘持久化架构。

## 一、倒排索引架构概览

### 1.1 核心概念

SeekDB 的全文索引基于**倒排索引**架构实现，主要包含以下核心概念：

- **Token（词项）**: 通过分词器从文本中提取的最小检索单元
- **Doc ID（文档ID）**: 每个文档的唯一标识符，支持多种类型（自增主键、Tablet Sequence等）
- **Domain ID（域ID）**: 用于组织倒排索引数据的标识符
- **Posting List（倒排列表）**: 包含某个 Token 在所有文档中出现信息的列表
- **BM25 相关性评分**: 用于计算文档与查询相关性的排序算法

### 1.2 索引表结构

SeekDB 的全文索引通过**4张辅助表**来组织倒排索引数据：

```
主表 (items1)
├── id (主键)
├── base_id (标量列)
├── docid_col (文档标识)
└── fulltext_col (全文列)

辅助索引表：
1. fts_rowkey_doc: 主键 -> Doc ID 映射
   - 结构: (rowkey) -> (doc_id, word_segment, word_count, doc_length)
   
2. fts_doc_rowkey: Doc ID -> 主键 映射
   - 结构: (doc_id) -> (rowkey)
   
3. fts_index: Token -> Doc ID 倒排索引（词-文档表）
   - 结构: (token, doc_id) -> (word_count, doc_length)
   
4. fts_doc_word: Doc ID -> Token 正排索引（文档-词表）
   - 结构: (doc_id, token) -> (word_count)
```

**关键说明：**
- `fts_index` 表是核心倒排索引表，存储 Token 到 Doc ID 的映射
- `fts_doc_word` 表是正排索引，用于文档级别的 Token 聚合查询
- 两个映射表 (`rowkey_doc` / `doc_rowkey`) 用于在主键和 Doc ID 之间转换

## 二、内存存储结构

### 2.1 核心数据结构

#### 2.1.1 Token 与词项结构

**文件**: `src/storage/fts/ob_fts_struct.h`

```cpp
class ObFTWord {
    ObDatum word_;           // Token 内容（字符串）
    ObObjMeta meta_;         // 元数据（包含字符集、排序规则等）
    
    // 核心功能：
    // - 哈希计算：用于 HashMap 索引
    // - 相等性比较：考虑字符集和排序规则
};

// Token 到频次的映射
typedef hash::ObHashMap<ObFTWord, int64_t> ObFTWordMap;
```

**设计要点：**
- `ObFTWord` 封装了 Token 字符串和字符集信息
- 支持基于排序规则的字符串比较（区分大小写/不区分大小写）
- 使用 HashMap 结构实现快速的 Token 查找

#### 2.1.2 文档标识符（Doc ID）

**文件**: `src/sql/das/ob_das_ir_define.h`

```cpp
// Doc ID 扩展结构
struct ObDocIdExt {
    union {
        int64_t int_val_;        // 整数类型的 Doc ID
        ObString string_val_;    // 字符串类型的 Doc ID
    };
    ObDocIDType type_;           // Doc ID 类型标识
    
    // 支持的 Doc ID 类型：
    // - HIDDEN_INC_PK: 隐藏自增主键
    // - TABLET_SEQUENCE: Tablet 序列号
};
```

**设计要点：**
- 灵活支持多种 Doc ID 类型
- 使用 union 优化内存占用
- 提供类型安全的访问接口

#### 2.1.3 Token 缓存结构

**文件**: `src/storage/retrieval/ob_text_retrieval_token_iter.h`

```cpp
class ObTextRetrievalTokenIter {
    // Token 迭代器 - 查询时的核心组件
    
    // 扫描参数
    ObTableScanParam *inv_idx_scan_param_;      // 倒排索引扫描参数
    ObTableScanParam *inv_idx_agg_param_;       // 倒排索引聚合参数
    ObTableScanParam *fwd_idx_scan_param_;      // 正排索引扫描参数
    
    // 迭代器
    ObDASScanIter *inv_idx_scan_iter_;          // 倒排索引扫描迭代器
    ObDASScanIter *inv_idx_agg_iter_;           // 倒排索引聚合迭代器
    ObDASScanIter *fwd_idx_agg_iter_;           // 正排索引聚合迭代器
    
    // 表达式
    ObExpr *relevance_expr_;                    // BM25 相关性评分表达式
    ObExpr *inv_scan_doc_length_col_;          // 文档长度列
    ObExpr *inv_scan_domain_id_col_;           // Domain ID 列
        
    // 统计信息
    int64_t token_doc_cnt_;                     // Token 出现在多少个文档中
    double max_token_relevance_;                // 该 Token 的最大相关性分数
};
```

**关键设计：**
1. **三级扫描机制**：
   - 倒排索引扫描（`inv_idx_scan`）：读取 Token -> Doc ID 映射
   - 倒排索引聚合（`inv_idx_agg`）：计算 Token 的统计信息（文档频率）
   - 正排索引聚合（`fwd_idx_agg`）：计算文档中 Token 的出现次数

2. **缓存优化**：
   - 使用 KV Cache 缓存热点 Token 的倒排列表
   - 避免重复扫描相同的 Token
   - 提升高频查询的性能

#### 2.1.4 相关性计算结构

**文件**: `src/storage/retrieval/ob_sparse_utils.h`

```cpp
// BM25 相关性累加器
struct ObSRDaaTInnerProductRelevanceCollector {
    double total_relevance_;      // 累计相关性分数
    int64_t matched_cnt_;         // 匹配的 Token 数量
    int64_t should_match_;        // 应该匹配的最少 Token 数
};

// 布尔检索累加器（支持 AND/OR/NOT 逻辑）
struct ObSRDaaTBooleanRelevanceCollector {
    ObFtsEvalNode *boolean_compute_node_;  // 布尔表达式树
    ObFixedArray<double> boolean_relevances_; // 每个维度的相关性
};
```

### 2.2 查询时的内存数据流

#### 查询执行流程

```
1. SQL 解析与优化
   ↓
2. 构建 Token 列表（分词）
   ↓
3. 对每个 Token:
   a. 检查 Token 缓存
   b. 如果未命中，扫描倒排索引表 (fts_index)
   c. 构建内存中的 Posting List
   d. 缓存结果到 Token Cache
   ↓
4. 文档级聚合（需要时）:
   - 从正排索引 (fts_doc_word) 读取文档中 Token 出现次数
   ↓
5. BM25 相关性计算:
   - 使用倒排列表中的词频（TF）
   - 文档长度
   - 全局文档数量和 Token 文档频率（IDF）
   ↓
6. 结果排序与返回
   - 按相关性分数排序
   - 应用 LIMIT 限制
```

#### 批量处理优化

```cpp
// 批量获取倒排列表数据
int ObTextRetrievalTokenIter::get_next_batch(
    const int64_t capacity,  // 批量大小
    int64_t &count          // 实际返回数量
) {
    // 1. 从倒排索引表批量读取 Doc IDs
    // 2. 批量计算 BM25 相关性
    // 3. 填充到输出缓冲区
}

// 跳跃到指定 Doc ID（用于合并多个 Token 的结果）
int ObTextRetrievalTokenIter::advance_to(
    const ObDatum &id_datum  // 目标 Doc ID
) {
    // 在有序倒排列表中快速定位到指定 Doc ID
}
```

### 2.3 内存优化策略

1. **临时内存上下文管理**
   - 文件: `src/observer/mysql/obmp_query.cpp`
   - 使用临时内存上下文处理大查询和重试场景
   - 避免内存动态泄漏

```cpp
int ObMPQuery::process_with_tmp_context(...) {
    // 创建临时内存上下文
    lib::ContextParam param;
    param.set_mem_attr(MTL_ID(), ObModIds::OB_SQL_EXECUTOR, ...);
    
    CREATE_WITH_TEMP_CONTEXT(param) {
        ret = do_process(...);  // 在临时上下文中执行查询
        ctx_.clear();           // 自动释放内存
    }
}
```

2. **缓存策略**
   - Token 级缓存：缓存热点 Token 的倒排列表
   - 使用 LRU 策略管理缓存淘汰
   - 支持可配置的缓存大小

3. **向量化处理**
   - 批量读取和计算，提升 CPU 缓存命中率
   - 使用 SIMD 指令加速相关性计算（见性能优化模块）

## 三、磁盘存储结构

### 3.1 索引表的物理存储

SeekDB 使用 **LSM-Tree** 架构存储倒排索引数据：

```
内存层 (MemTable)
├── 活跃 MemTable: 接收新写入的 Token-DocID 对
└── 冻结 MemTable: 等待刷盘的数据

磁盘层 (SSTable)
├── Level 0: 刚刷盘的 SSTable（可能有重叠）
├── Level 1-N: 合并后的有序 SSTable
└── 每个 SSTable 包含:
    ├── Data Blocks: 存储实际的 (Token, DocID) 键值对
    ├── Index Blocks: 索引 Data Blocks 的位置
    ├── Bloom Filter: 快速判断 Token 是否存在
    └── Macro Block Meta: 元数据信息
```

### 3.2 倒排索引表的行格式

#### 3.2.1 fts_index 表（倒排索引）

**Schema**:
```sql
CREATE TABLE fts_index (
    token VARCHAR,          -- Token（分词结果）
    doc_id BIGINT,          -- 文档ID
    word_count INT,         -- Token 在该文档中出现的次数
    doc_length INT,         -- 文档总词数（用于 BM25）
    PRIMARY KEY (token, doc_id)
);
```

**存储格式**:
- **键**: `(token, doc_id)` - 复合主键，按 Token 字典序排序
- **值**: `(word_count, doc_length)` - 词频和文档长度
- **索引**: Token 维度建立索引，支持快速定位所有包含特定 Token 的文档

**物理布局**:
```
Macro Block 1:
  Micro Block 1: [token="apple", doc_id=1, count=3, len=100]
                 [token="apple", doc_id=5, count=2, len=80]
                 [token="apple", doc_id=8, count=1, len=120]
  Micro Block 2: [token="banana", doc_id=2, count=5, len=90]
                 [token="banana", doc_id=7, count=3, len=110]
                 ...

Macro Block Meta:
  - Min Key: "apple"
  - Max Key: "banana"
  - Bloom Filter: Token 集合
```

#### 3.2.2 fts_doc_word 表（正排索引）

**Schema**:
```sql
CREATE TABLE fts_doc_word (
    doc_id BIGINT,          -- 文档ID
    token VARCHAR,          -- Token
    word_count INT,         -- Token 在该文档中出现的次数
    PRIMARY KEY (doc_id, token)
);
```

**用途**:
- 支持文档级别的 Token 聚合查询
- 计算文档中查询词的总出现次数
- 用于优化某些查询模式

### 3.3 SSTable 数据结构

**文件路径**: `src/storage/blocksstable/`

虽然倒排索引使用标准的 SSTable 存储，但有以下特点：

1. **Macro Block 结构**
   - 每个 Macro Block 通常为 2MB
   - 包含多个 Micro Block（默认 16KB）
   - Macro Block Header 存储元数据和 Bloom Filter

2. **Micro Block 结构**
   - 行式存储 Token-DocID 对
   - 支持压缩（Dictionary、RLE、ZStd 等）
   - 每个 Micro Block 有独立的索引

3. **跳表索引（Skip Index）**
   - 文件: `src/storage/retrieval/ob_block_stat_collector.h`
   ```cpp
   struct ObTextBlockMaxSpec {
       ObFixedArray<ObSkipIndexColType> col_types_;
       ObFixedArray<int32_t> col_store_idxes_;
       int32_t min_id_idx_;          // 最小 Doc ID 索引
       int32_t max_id_idx_;          // 最大 Doc ID 索引
       int32_t token_freq_idx_;      // Token 频率索引
       int32_t doc_length_idx_;      // 文档长度索引
   };
   ```
   - **Block Max 优化**: 记录每个 Block 的最大相关性分数，跳过不可能进入 TopK 的 Block

### 3.4 索引构建与持久化流程

#### 3.4.1 索引构建任务

**文件**: `src/rootserver/ddl_task/ob_fts_index_build_task.h`

```cpp
class ObFtsIndexBuildTask : public ObDDLTask {
    // 构建流程状态：
    enum Status {
        PREPARE,                        // 准备阶段
        LOAD_DICTIONARY,                // 加载分词词典（如需要）
        WAIT_TRANS_END,                 // 等待事务结束
        BUILD_ROWKEY_DOC,               // 构建主键-DocID映射表
        BUILD_DOC_ROWKEY,               // 构建DocID-主键映射表
        BUILD_INDEX,                    // 构建倒排索引（fts_index）
        BUILD_DOC_WORD,                 // 构建正排索引（fts_doc_word）
        WAIT_COMPLEMENT,                // 等待补全
        VALIDATE_CHECKSUM,              // 校验Checksum
        SUCCESS                         // 成功
    };
    
    // 4张辅助表的表ID
    uint64_t rowkey_doc_aux_table_id_;
    uint64_t doc_rowkey_aux_table_id_;
    uint64_t domain_index_aux_table_id_;      // fts_index
    uint64_t fts_doc_word_aux_table_id_;
};
```

#### 3.4.2 分布式构建流程

```
1. DDL 调度器（ObDDLScheduler）
   ↓
2. 创建 FTS 索引构建任务
   ↓
3. 并行扫描主表数据
   - 使用 /*+ parallel(N) */ 提示控制并行度
   - 每个 Worker 处理一部分数据分区
   ↓
4. 对每个文档:
   a. 使用配置的分词器分词（IK/N-gram/Whitespace）
   b. 生成 Doc ID（自增主键或 Tablet Sequence）
   c. 构建 (Token, DocID, WordCount, DocLength) 元组
   ↓
5. 写入 MemTable
   - 批量写入优化
   - 自动触发 MemTable 刷盘
   ↓
6. MemTable 刷盘 -> SSTable
   - 排序并去重
   - 应用压缩算法
   - 生成 Bloom Filter 和索引块
   ↓
7. 后台 Compaction
   - 合并多个 SSTable
   - 优化存储布局
   - 回收空间
```

### 3.5 压缩与编码

SeekDB 对倒排索引数据应用多种压缩技术：

1. **字典压缩**
   - 对高频 Token 使用字典编码
   - 减少重复字符串的存储开销

2. **差分编码（Delta Encoding）**
   - Doc ID 序列使用差分编码
   - 相邻 Doc ID 之间的差值通常较小

3. **变长整数编码（VarInt）**
   - 小整数用更少的字节表示
   - 适用于 word_count、doc_length 等字段

4. **通用压缩算法**
   - ZStd: 高压缩比，适合冷数据
   - LZ4: 快速压缩/解压，适合热数据

## 四、查询处理流程

### 4.1 全文检索查询

**SQL 示例**:
```sql
SELECT docid_col, MATCH(fulltext_col) AGAINST('apple banana') as score
FROM items1
WHERE MATCH(fulltext_col) AGAINST('apple banana')
  AND base_id IN ('id1', 'id2')
  AND id < 1000
ORDER BY score DESC
LIMIT 10;
```

#### 4.1.1 查询优化器的策略选择

SeekDB 优化器会根据查询条件的**选择性**（Selectivity）选择不同的执行策略：

**策略 A：倒排索引优先（全文条件选择性高）**

适用场景：查询词罕见，或标量条件覆盖大部分数据

```
1. 扫描倒排索引 (fts_index)
   - 对每个 Token 读取倒排列表
   - 使用 Block Max WAND 跳过低分 Block
   - 在倒排索引中直接计算 BM25（已包含 word_count, doc_length）
   ↓
2. 应用标量过滤
   - 在内存中过滤 base_id、id 等条件
   - 或通过 Doc ID -> 主键映射后在主表索引上过滤
   ↓
3. Top-K 排序
   - 维护最相关的 K 个文档
   ↓
4. 回表获取完整数据
   - 通过 fts_doc_rowkey 映射获取主键
   - 读取主表完整行
```

**策略 B：标量索引优先（标量条件选择性高）**

适用场景：标量条件能过滤掉 99% 以上数据

```
1. 扫描标量索引 (idx_base_id, 主键索引等)
   - base_id IN (...) 快速定位少量文档
   - id < 1000 范围扫描
   ↓
2. 获取候选文档的 Doc IDs
   - 通过主键 -> Doc ID 映射 (fts_rowkey_doc)
   ↓
3. 从正排索引计算 BM25
   【关键】仍然需要倒排索引系统：
   a. 扫描 fts_doc_word (doc_id, token) 获取文档的所有 Token
   b. 从全局统计信息获取 IDF（存储在倒排索引元数据中）
   c. 使用预计算的 doc_length（存储在 fts_rowkey_doc 或 fts_index 中）
   d. 计算 BM25 分数
   ↓
4. Top-K 排序与返回
```

**关键洞察：无论哪种策略，计算 BM25 都离不开倒排索引数据**

原因：
1. **全局统计信息**：IDF 需要知道每个 Token 在多少文档中出现（Document Frequency）
2. **文档级统计**：doc_length、token_count 等预计算信息存储在索引表中
3. **分词结果复用**：`fts_doc_word` 正排索引存储了文档的分词结果，避免重复分词

如果不使用倒排索引系统，就需要：
- 实时对每个候选文档的 `fulltext_col` 分词（开销巨大）
- 扫描所有文档统计全局 IDF（不可行）
- 每次都重新计算文档长度（效率低）

#### 4.1.2 混合查询的优化器代价估算

```cpp
// 优化器选择策略（伪代码）
double cost_inverted_first = 
    token_doc_count * cost_per_posting +        // 倒排扫描
    matched_docs * cost_scalar_filter +          // 标量过滤
    topk * cost_lookup;                          // 回表

double cost_scalar_first = 
    scalar_filtered_docs * cost_rowkey_to_docid + // 映射查找
    scalar_filtered_docs * cost_fwd_scan +        // 正排扫描
    scalar_filtered_docs * cost_bm25 +            // BM25 计算
    topk * cost_lookup;                           // 回表

// 选择代价更低的策略
if (cost_inverted_first < cost_scalar_first) {
    use_inverted_index_first();
} else {
    use_scalar_index_first();
}
```

**实际执行计划示例**:

```
-- 查询：WHERE MATCH(...) AND base_id IN (1,2,3) AND id < 1000 LIMIT 10

EXPLAIN 输出:
┌─ Limit (10)
│  └─ Sort (BM25 score DESC)
│     └─ TableScan (items1)
│        ├─ Index: fts_index (倒排索引)
│        │  ├─ Token Filter: ["apple", "banana"]
│        │  └─ BM25 Calculation (在倒排索引中完成)
│        └─ Filter: base_id IN (1,2,3) AND id < 1000
│           (标量过滤下推到倒排索引扫描中)
```

### 4.2 Block Max WAND 优化

**文件**: `src/storage/retrieval/ob_block_max_iter.h`

```cpp
class ObBlockMaxScoreIterator {
    // Block Max WAND 算法实现
    
    struct BlockMaxScanParam {
        int64_t doc_freq_;              // 文档频率
        int64_t total_doc_cnt_;         // 总文档数
        double avg_doc_token_cnt_;      // 平均文档长度
        int64_t token_freq_col_idx_;    // Token频率列索引
        int64_t doc_length_col_idx_;    // 文档长度列索引
    };
    
    // 关键优化：
    // - 记录每个 Block 的最大可能 BM25 分数
    // - 查询时跳过不可能进入 Top-K 的 Block
    // - 大幅减少需要评分的文档数量
};
```

**优化效果**:
- 对于 Top-K 查询（如 LIMIT 10），可以跳过 90% 以上的数据块
- 查询延迟降低 5-10 倍

### 4.3 缓存机制

**三层缓存架构**:

```
1. Token 倒排列表缓存 (ObTextIRTokenCacheValue)
   - 缓存完整的 Token Posting List
   - 避免重复磁盘扫描
   - LRU 淘汰策略
   
2. Micro Block 缓存
   - 缓存热点 Micro Block
   - 提升随机读性能
   
3. Bloom Filter 缓存
   - 缓存 Token Bloom Filter
   - 快速判断 Token 是否存在
```

### 4.4 为什么 BM25 计算必须依赖倒排索引？

#### BM25 算法公式回顾

```
BM25(D, Q) = Σ IDF(qi) × (f(qi, D) × (k1 + 1)) / (f(qi, D) + k1 × (1 - b + b × |D| / avgdl))

其中：
- qi: 查询中的第 i 个词项（Token）
- f(qi, D): qi 在文档 D 中的词频（Term Frequency）
- |D|: 文档 D 的长度
- avgdl: 所有文档的平均长度
- IDF(qi) = log((N - n(qi) + 0.5) / (n(qi) + 0.5))
  - N: 总文档数
  - n(qi): 包含 qi 的文档数量
```

#### 必需的倒排索引数据

| 数据项 | 存储位置 | 说明 |
|--------|---------|------|
| **f(qi, D)** | `fts_index` 或 `fts_doc_word` 的 `word_count` 列 | 词项在文档中的出现次数 |
| **\|D\|** | `fts_index` 或 `fts_rowkey_doc` 的 `doc_length` 列 | 文档总词数 |
| **n(qi)** | 倒排索引元数据或聚合查询 | 包含该词项的文档数（用于 IDF） |
| **N** | 系统全局统计 | 总文档数 |
| **avgdl** | 系统全局统计或预计算 | 平均文档长度 |

#### 四种可能的实现方式对比

**方式 1：完全依赖倒排索引（SeekDB 当前方式）✅**
```sql
-- 从 fts_index 获取所有需要的信息
SELECT doc_id, word_count, doc_length
FROM fts_index
WHERE token IN ('apple', 'banana')
```
- ✅ 所有数据预计算，查询最快
- ✅ 支持 Block Max WAND 等高级优化
- ✅ 数据局部性好，缓存友好

**方式 2：标量索引 + 正排索引（策略B）✅**
```sql
-- 1. 标量过滤
SELECT id, fulltext_col FROM items1 
WHERE base_id IN (...);  -- 假设得到 100 个文档

-- 2. 对每个文档，从正排索引获取分词信息
SELECT token, word_count FROM fts_doc_word
WHERE doc_id IN (doc_id_1, doc_id_2, ..., doc_id_100);

-- 3. 查询全局 IDF（需要倒排索引元数据）
SELECT token, COUNT(*) as doc_freq FROM fts_index
WHERE token IN ('apple', 'banana')
GROUP BY token;
```
- ✅ 适合标量条件选择性极高的场景
- ⚠️ 仍然需要访问 `fts_doc_word` 和 `fts_index` 表
- ⚠️ 需要多次表查询，I/O 开销大

**方式 3：标量索引 + 实时分词（理论方案）❌**
```sql
-- 1. 标量过滤
SELECT id, fulltext_col FROM items1 
WHERE base_id IN (...);

-- 2. 对 fulltext_col 实时分词并计算 TF
-- （在应用层或 UDF 中实现）

-- 3. 全局 IDF 仍需扫描所有文档
SELECT COUNT(DISTINCT id) as doc_freq
FROM items1
WHERE fulltext_col LIKE '%apple%';  -- 低效的近似方法
```
- ❌ 实时分词开销巨大（每个文档几千个 Token）
- ❌ 无法准确计算 IDF（LIKE 不准确，全扫描不可行）
- ❌ 无法复用分词结果，每次查询都重新分词
- ❌ 无法应用任何查询优化

**方式 4：混合缓存方式（优化方向）✅**
```cpp
// 标量过滤后，从缓存获取文档的预计算 BM25 特征
struct DocFeatureCache {
    int64_t doc_id;
    int64_t doc_length;
    // Token -> word_count 映射（稀疏向量）
    HashMap<Token, int64_t> term_frequencies;
};

// 查询时：
// 1. 标量索引过滤 -> 得到候选 Doc IDs
// 2. 批量从缓存加载文档特征
// 3. 与查询 Token 计算点积得到 BM25
```
- ✅ 结合两种策略的优势
- ✅ 缓存热点文档特征，减少 I/O
- ⚠️ 需要大量内存存储文档特征
- ⚠️ IDF 仍需从倒排索引获取

#### 结论

**无论采用哪种查询策略，BM25 计算都无法完全脱离倒排索引系统**，原因：

1. **全局统计依赖**：IDF 计算需要知道每个 Token 的文档频率，这是倒排索引的核心统计信息
2. **预计算数据**：doc_length、word_count 等信息在索引构建时预先计算并存储
3. **分词复用**：避免重复分词，正排索引 `fts_doc_word` 存储分词结果
4. **查询优化**：Block Max、跳表等优化技术都基于倒排索引的数据组织

**SeekDB 的智能之处**：
- 优化器根据查询特征**动态选择**倒排优先或标量优先策略
- 但两种策略都充分利用倒排索引系统的数据和优化
- 标量条件可以**下推到倒排索引扫描中**，而不是分离执行

## 五、性能优化技术

### 5.1 SIMD 加速 BM25 计算

**文件**: `src/sql/engine/expr/ob_expr_bm25.h`

SeekDB 使用 SIMD 指令加速 BM25 相关性计算：

```cpp
// 批量计算 BM25 分数（伪代码）
void batch_compute_bm25_simd(
    const int64_t *doc_ids,
    const int64_t *term_freqs,
    const int64_t *doc_lengths,
    double *scores,
    int64_t count
) {
    // 使用 AVX2/AVX-512 指令并行计算多个文档的 BM25 分数
    // 一次处理 4 个（AVX2）或 8 个（AVX-512）文档
}
```

### 5.2 并行查询执行

- **并行倒排列表扫描**: 多个 Token 的倒排列表并行读取
- **并行相关性计算**: 多个文档的 BM25 分数并行计算
- **并行回表**: 批量回表请求并行执行

### 5.3 自适应查询优化

```cpp
// 根据查询模式自动选择算法
if (topk_limit < 100 && token_count > 2) {
    use_block_max_wand();    // 使用 Block Max WAND
} else if (token_count == 1) {
    use_simple_scan();        // 单Token直接扫描
} else {
    use_daat_merge();         // 使用 DAAT 合并
}
```

## 六、配置参数

### 6.1 索引构建参数

```sql
-- 设置索引构建并行度
CREATE /*+ parallel(90) */ FULLTEXT INDEX ft_idx ON table(col);

-- 分词器选择
CREATE FULLTEXT INDEX ft_idx ON table(col) 
WITH PARSER ngram;  -- 可选: ngram, ik, whitespace

-- N-gram 分词参数
SET GLOBAL ngram_token_size = 2;  -- N-gram 大小
```

### 6.2 查询优化参数

```sql
-- 启用 Block Max 优化
SET ob_enable_block_max_scan = 1;

-- Token 缓存大小（内部参数）
-- 可通过代码修改缓存容量
```

## 七、关键代码文件索引

### 存储层
- `src/storage/fts/` - 全文索引核心实现
  - `ob_fts_struct.h` - 基础数据结构（Token、Flag等）
  - `ob_fts_parser_property.h` - 分词器配置
  - `ik/` - IK 分词器实现
  
- `src/storage/retrieval/` - 检索算法实现
  - `ob_text_retrieval_token_iter.h/cpp` - Token 迭代器和缓存
  - `ob_block_max_iter.h` - Block Max WAND 算法
  - `ob_sparse_daat_iter.h` - DAAT 合并算法
  - `ob_sparse_taat_iter.h` - TAAT 合并算法

### SQL 层
- `src/sql/das/` - DAS (Data Access Service) 层
  - `ob_das_ir_define.h` - IR 扫描定义
  - `iter/ob_das_text_retrieval_iter.h` - 文本检索迭代器
  
- `src/sql/engine/expr/` - 表达式计算
  - `ob_expr_bm25.h` - BM25 相关性评分

### DDL 层
- `src/rootserver/ddl_task/` - DDL 任务管理
  - `ob_fts_index_build_task.h/cpp` - 全文索引构建任务
  - `ob_drop_fts_index_task.h` - 删除索引任务

### Schema 层
- `src/share/ob_fts_index_builder_util.h` - 索引构建工具类
- `src/share/schema/ob_table_schema.h` - 表 Schema 定义

## 八、全文索引查询缓存机制详解

### 8.1 问题背景

用户在执行全文索引查询时，数据需要从存储层读取。关键问题：
- **每次查询是否都需要访问磁盘？**
- **缓存机制如何加速查询？**
- **缓存命中与磁盘读取的性能差异？**

### 8.2 SeekDB 多级缓存架构

SeekDB 继承了 OceanBase 完善的缓存体系，全文索引查询涉及以下缓存层次：

```
┌─────────────────────────────────────────────────────────────────┐
│                    缓存层次结构 (由快到慢)                        │
├─────────────────────────────────────────────────────────────────┤
│  Level 0: CPU Cache (L1/L2/L3)    ← 硬件自动管理               │
├─────────────────────────────────────────────────────────────────┤
│  Level 1: MemTable (内存表)        ← 最新数据，无需磁盘访问      │
├─────────────────────────────────────────────────────────────────┤
│  Level 2: KVCache 系列             ← 软件缓存，命中率关键        │
│    ├─ Index Block Cache           (索引块缓存)                  │
│    ├─ Data Block Cache            (数据块缓存)                  │
│    ├─ Row Cache                   (行缓存)                      │
│    ├─ BloomFilter Cache           (布隆过滤器缓存)              │
│    ├─ Fuse Row Cache              (融合行缓存)                  │
│    └─ Dict Cache                  (分词字典缓存)                │
├─────────────────────────────────────────────────────────────────┤
│  Level 3: Page Cache (OS)          ← 操作系统文件缓存           │
├─────────────────────────────────────────────────────────────────┤
│  Level 4: SSD/HDD (磁盘)           ← 最慢，需要物理 I/O         │
└─────────────────────────────────────────────────────────────────┘
```

### 8.3 核心缓存类详解

#### 8.3.1 ObStorageCacheSuite (存储缓存套件)

```cpp
// src/storage/blocksstable/ob_storage_cache_suite.h
class ObStorageCacheSuite {
private:
  ObIndexMicroBlockCache index_block_cache_;   // 索引块缓存
  ObDataMicroBlockCache user_block_cache_;     // 数据块缓存 (user_block_cache)
  ObRowCache user_row_cache_;                  // 行缓存
  ObBloomFilterCache bf_cache_;                // 布隆过滤器缓存
  ObFuseRowCache fuse_row_cache_;              // 融合行缓存
  ObStorageMetaCache storage_meta_cache_;      // 存储元数据缓存
};
```

#### 8.3.2 ObMicroBlockCache (微块缓存)

这是全文索引查询最核心的缓存，倒排索引数据以 Micro Block 为单位存储和缓存：

```cpp
// src/storage/blocksstable/ob_micro_block_cache.h

// 缓存键：通过 (tenant_id, macro_block_id, offset, size) 唯一标识
class ObMicroBlockCacheKey : public common::ObIKVCacheKey {
  uint64_t tenant_id_;
  ObMicroBlockId block_id_;             // 物理模式
  ObLogicMicroBlockId logic_micro_id_;  // 逻辑模式
};

// 缓存值：存储解压后的微块数据
class ObMicroBlockCacheValue : public common::ObIKVCacheValue {
  ObMicroBlockData block_data_;     // 实际的块数据
};

// 数据块缓存实现
class ObDataMicroBlockCache : public ObIMicroBlockCache {
  // 缓存命中统计
  void cache_hit(int64_t &hit_cnt) { EVENT_INC(DATA_BLOCK_CACHE_HIT); }
  void cache_miss(int64_t &miss_cnt) { EVENT_INC(DATA_BLOCK_CACHE_MISS); }
};
```

#### 8.3.3 ObDictCache (分词字典缓存)

专门为 IK 分词器的 DAT (Double Array Trie) 字典设计：

```cpp
// src/storage/fts/dict/ob_ft_cache.h

class ObDictCache : public common::ObKVCache<ObDictCacheKey, ObDictCacheValue> {
  // 获取字典：首次访问加载到缓存，后续直接使用
  int get_dict(const ObDictCacheKey &key,
               const ObDictCacheValue *&value,
               common::ObKVCacheHandle &handle);
               
  // 放入缓存并获取
  int put_and_fetch_dict(const ObDictCacheKey &key,
                         const ObDictCacheValue &value,
                         const ObDictCacheValue *&pvalue,
                         common::ObKVCacheHandle &handle);
};
```

### 8.4 缓存命中 vs 磁盘读取的性能路径

#### 8.4.1 缓存命中路径 (快速路径)

```cpp
// src/storage/blocksstable/ob_micro_block_cache.cpp

int ObIMicroBlockCache::get_cache_block(
    const ObMicroBlockCacheKey &key,
    ObMicroBlockBufferHandle &handle)
{
  // 1. 直接从内存中的 KVCache 查找
  if (OB_FAIL(cache->get(key, handle.micro_block_, handle.handle_))) {
    // 缓存未命中
    EVENT_INC(ObStatEventIds::BLOCK_CACHE_MISS);
    inc_cache_miss();
  } else {
    // ✅ 缓存命中！直接返回内存中的数据
    EVENT_INC(ObStatEventIds::BLOCK_CACHE_HIT);
    // 耗时：~100ns 量级 (纯内存访问)
  }
}
```

#### 8.4.2 磁盘读取路径 (慢速路径)

```cpp
// src/storage/blocksstable/ob_micro_block_cache.cpp

int ObIMicroBlockIOCallback::process_block(...) {
  // 1. 从磁盘读取原始数据（已完成 I/O）
  // 2. 反序列化 MicroBlockHeader
  if (OB_FAIL(header.deserialize(buffer, size, pos))) { ... }
  
  // 3. 校验数据完整性
  if (OB_FAIL(header.check_and_get_record(...))) { ... }
  
  // 4. 解压数据
  if (use_block_cache_) {
    // 5. 尝试放入缓存供后续使用
    if (OB_SUCCESS == kvcache->get(key, micro_block, cache_handle)) {
      // 已有其他线程放入，直接使用
    } else {
      // 放入缓存
      cache_->put_cache_block(des_meta, buffer, size, key, reader, 
                              allocator, micro_block, cache_handle);
    }
  }
  // 耗时：~1-10ms 量级 (包含磁盘 I/O + 解压 + 缓存操作)
}
```

### 8.5 性能对比

| 访问方式 | 典型延迟 | 吞吐量 | 说明 |
|---------|---------|--------|------|
| **缓存命中 (KVCache)** | ~100ns - 1μs | 极高 | 纯内存哈希表查找 |
| **MemTable 访问** | ~1-10μs | 高 | 内存 SkipList 遍历 |
| **Page Cache 命中** | ~10-100μs | 中高 | OS 缓存，无需磁盘 I/O |
| **SSD 磁盘读取** | ~100μs - 1ms | 中 | NVMe SSD 随机读 |
| **HDD 磁盘读取** | ~5-10ms | 低 | 机械硬盘寻道时间 |

**性能差距：缓存命中比磁盘读取快 3-4 个数量级！**

### 8.6 全文索引查询的缓存使用流程

当执行全文索引查询时，缓存机制的完整流程：

```
┌─────────────────────────────────────────────────────────────────┐
│                   MATCH AGAINST 查询执行流程                     │
├─────────────────────────────────────────────────────────────────┤
│  1. 分词阶段                                                     │
│     └─→ 查询 Dict Cache (分词字典)                              │
│         └─→ 命中: 直接使用缓存的 DAT 字典                        │
│         └─→ 未命中: 从磁盘加载字典 → 放入缓存                    │
├─────────────────────────────────────────────────────────────────┤
│  2. Token 统计查询 (IDF 计算)                                   │
│     └─→ 查询 fts_index 表统计信息                               │
│         └─→ 先查 Index Block Cache (索引块)                     │
│         └─→ 再查 Data Block Cache (数据块)                      │
│         └─→ 缓存未命中则发起磁盘 I/O                            │
├─────────────────────────────────────────────────────────────────┤
│  3. 倒排列表扫描 (TF 获取)                                      │
│     └─→ 逐 Token 扫描 fts_index 表                              │
│         └─→ BloomFilter Cache: 快速判断 Token 是否存在          │
│         └─→ Data Block Cache: 缓存已访问的微块                  │
│         └─→ 热门 Token 的数据会持续命中缓存                     │
├─────────────────────────────────────────────────────────────────┤
│  4. 文档长度获取                                                 │
│     └─→ 从 fts_index 或 fts_doc_word 获取                       │
│         └─→ Row Cache: 缓存热门文档的长度信息                   │
├─────────────────────────────────────────────────────────────────┤
│  5. BM25 评分计算                                                │
│     └─→ 纯 CPU 计算，使用上述缓存的数据                         │
└─────────────────────────────────────────────────────────────────┘
```

### 8.7 inv_idx_agg_cache_mode 优化

代码中的 `inv_idx_agg_cache_mode_` 标志用于优化多 Token 查询：

```cpp
// src/storage/retrieval/ob_text_retrieval_token_iter.cpp

void ObTextRetrievalTokenIter::reuse() {
  if (inv_idx_agg_cache_mode_ && !inv_idx_agg_param_->need_switch_param_) {
    // 缓存模式：复用已计算的统计信息，避免重复查询
    // do nothing - 保留 token_doc_cnt_calculated_ 状态
  } else {
    token_doc_cnt_calculated_ = false;
  }
}

// 在 function_lookup_mode_ 且非 TAAT 模式下启用
// src/sql/das/iter/sparse_retrieval/ob_das_tr_merge_iter.cpp
iter_param.inv_idx_agg_cache_mode_ = function_lookup_mode_ && !taat_mode_;
```

启用后可以：
- 复用 Token 的 doc_cnt 统计
- 减少对 fts_index 聚合表的重复扫描

### 8.8 缓存配置与调优

相关配置参数：

```sql
-- 查看缓存配置
SHOW PARAMETERS LIKE '%cache%priority%';

-- 关键参数
index_block_cache_priority  -- 索引块缓存优先级 (默认 10)
user_block_cache_priority   -- 数据块缓存优先级 (默认 1)
user_row_cache_priority     -- 行缓存优先级
bf_cache_priority           -- 布隆过滤器缓存优先级

-- 查看缓存命中统计
SELECT * FROM oceanbase.__all_virtual_kvcache_info WHERE tenant_id = xxx;
```

### 8.9 结论

**回答用户问题：**

1. **全文索引查询有没有缓存机制？**
   - ✅ **有完善的多级缓存机制**
   - 包括：MicroBlockCache、DictCache、BloomFilterCache、RowCache 等

2. **是否每次都需要访问磁盘？**
   - ❌ **不是**
   - 热点数据会缓存在内存中，后续查询直接命中缓存
   - 只有冷数据或首次访问才需要磁盘 I/O

3. **Cache 查找和磁盘查找的速度差异？**
   - **缓存命中**：100ns - 1μs
   - **磁盘读取**：100μs - 10ms
   - **差距**：**3-4 个数量级 (1000x - 10000x)**

## 九、总结

SeekDB 的倒排索引采用了现代搜索引擎的经典架构，并针对数据库场景做了优化：

### 优势
1. **统一存储引擎**: 全文索引与关系数据共享存储层，简化架构
2. **标量过滤下推**: 支持在倒排索引扫描时应用标量过滤，减少数据量
3. **多种优化技术**: Block Max WAND、SIMD、缓存等提升性能
4. **LSM-Tree 架构**: 适合写多读少场景，支持高效更新
5. **多级缓存体系**: 热点数据缓存命中率高，查询延迟低

### 优化方向
1. **缓存策略优化**: 进一步优化 Token 缓存命中率
2. **压缩算法选择**: 根据数据特征自适应选择压缩算法
3. **查询并行度**: 提升多 Token 查询的并行度
4. **索引合并优化**: 优化 Compaction 策略，减少读放大

---

**文档版本**: v1.1  
**更新日期**: 2025-12-04  
**相关竞赛**: 2025 数据库大赛决赛 - 内核赛题  
**代码分支**: 2025-final-competition
