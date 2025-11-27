/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef OCEANBASE_SQL_OB_QUERY_RESULT_CACHE_H_
#define OCEANBASE_SQL_OB_QUERY_RESULT_CACHE_H_

#include <chrono>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/object/ob_object.h"
#include "common/row/ob_row.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_string.h"
#include <iostream>

namespace oceanbase {
namespace sql {

// 缓存的单行数据 - 使用 std::vector 存储 ObObj
struct ObCachedRow {
  ObCachedRow() : cells_() {}
  ~ObCachedRow() { reset(); }

  void reset() { cells_.clear(); }

  int deep_copy(const common::ObNewRow &row);

  int64_t get_cell_count() const { return static_cast<int64_t>(cells_.size()); }

  std::vector<common::ObObj> cells_;
};

// 缓存的查询结果 - 使用 std::vector 存储行
struct ObCachedQueryResult {
  ObCachedQueryResult() : rows_() {}
  int add_row(const common::ObNewRow &row) {
    int ret = OB_SUCCESS;
    ObCachedRow cached_row;
    if (OB_FAIL(cached_row.deep_copy(row))) {
      std::cout << "deep copy row failed, ret=" << ret << std::endl;
    } else {
      rows_.push_back(std::move(cached_row));
    }
    return ret;
  }

  std::vector<ObCachedRow> rows_;
};

// 缓存键：SQL + 数据库ID + 租户ID - 使用 std::string
struct ObQueryCacheKey {
  ObQueryCacheKey() : sql_() {}
  ObQueryCacheKey(const std::string &sql) : sql_(sql) {}
  bool operator==(const ObQueryCacheKey &other) const {
    return sql_ == other.sql_;
  }

  std::string sql_;
};

// Hash 函数用于 unordered_map
struct ObQueryCacheKeyHash {
  size_t operator()(const ObQueryCacheKey &key) const {
    return std::hash<std::string>()(key.sql_);
  }
};

// 查询结果缓存管理器
class ObQueryResultCache {
public:
  static ObQueryResultCache &get_instance();

  void destroy();

  int init();
  bool is_inited() const { return inited_; }

  // 查找缓存
  // 返回值：OB_SUCCESS 表示命中，OB_ENTRY_NOT_EXIST 表示未命中
  int get(const ObQueryCacheKey &key, ObCachedQueryResult *&result);

  // 添加缓存
  int put(const ObQueryCacheKey &key, ObCachedQueryResult *result);

  // 删除指定缓存
  int remove(const ObQueryCacheKey &key);

  // 清空所有缓存
  int clear();

  // 检查SQL是否可缓存（只缓存SELECT语句，不缓存带变量的语句等）
  static bool is_cacheable_sql(const std::string &sql);

  // 分配缓存结果对象
  ObCachedQueryResult *alloc_result();
  void free_result(ObCachedQueryResult *result);

private:
  ObQueryResultCache();
  ~ObQueryResultCache();

private:
  bool inited_;

  // 缓存存储 - 使用 std::unordered_map
  typedef std::unordered_map<ObQueryCacheKey, ObCachedQueryResult *,
                             ObQueryCacheKeyHash>
      CacheMap;
  CacheMap cache_map_;

  // 禁止拷贝
  ObQueryResultCache(const ObQueryResultCache &) = delete;
  ObQueryResultCache &operator=(const ObQueryResultCache &) = delete;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_QUERY_RESULT_CACHE_H_
