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

#define USING_LOG_PREFIX SQL

#include "sql/ob_query_result_cache.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include <set>

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {

int ObCachedRow::deep_copy(const ObNewRow &row) {
  int ret = OB_SUCCESS;
  if (row.get_count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row", K(ret), K(row));
  } else {
    cells_.clear();
    cells_.reserve(row.get_count());

    for (int64_t i = 0; i < row.get_count(); ++i) {
      const ObObj &src = row.get_cell(i);
      ObObj dst;

      // 对于字符串类型，需要深拷贝数据
      if (src.is_string_type()) {
        ObString str = src.get_string();
        if (str.length() > 0) {
          char *buf = new (std::nothrow) char[str.length()];
          if (buf == nullptr) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret), K(str.length()));
            break;
          }
          memcpy(buf, str.ptr(), str.length());
          dst.set_string(src.get_type(), buf, str.length());
          dst.set_collation_type(src.get_collation_type());
        } else {
          dst = src;
        }
      } else {
        // 非字符串类型直接拷贝
        dst = src;
      }

      cells_.push_back(dst);
    }

    if (OB_FAIL(ret)) {
      // 清理已分配的内存
      for (auto &cell : cells_) {
        if (cell.is_string_type() && cell.get_string_len() > 0) {
          delete[] cell.get_string_ptr();
        }
      }
      cells_.clear();
    }
  }
  return ret;
}

ObQueryResultCache &ObQueryResultCache::get_instance() {
  static ObQueryResultCache instance;
  return instance;
}

ObQueryResultCache::ObQueryResultCache() : inited_(false) {}
ObQueryResultCache::~ObQueryResultCache() { destroy(); }

int ObQueryResultCache::init() {
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("query result cache already inited", K(ret));
  } else {
    inited_ = true;
    LOG_INFO("query result cache inited");
  }
  return ret;
}

void ObQueryResultCache::destroy() {
  if (inited_) {
    clear();
    inited_ = false;
  }
}

int ObQueryResultCache::get(const ObQueryCacheKey &key,
                            ObCachedQueryResult *&result) {
  int ret = OB_SUCCESS;
  result = nullptr;

  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    auto it = cache_map_.find(key);
    if (it == cache_map_.end()) {
      ret = OB_ENTRY_NOT_EXIST;
    } else if (it->second == nullptr) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cached result is null", K(ret));
    } else {
      result = it->second;
    }
  }
  return ret;
}

int ObQueryResultCache::put(const ObQueryCacheKey &key,
                            ObCachedQueryResult *result) {
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (result == nullptr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("result is null", K(ret));
  } else {
    // 检查是否已存在
    if (cache_map_.size() >= oceanbase::sql::MAX_CACHE_SIZE) {
      LOG_WARN("query result cache size overflow", K(ret),
               K(cache_map_.size()));
    } else {
      cache_map_[key] = result;
    }
  }
  return ret;
}

int ObQueryResultCache::remove(const ObQueryCacheKey &key) {
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    auto it = cache_map_.find(key);
    if (it != cache_map_.end()) {
      if (it->second != nullptr) {
        delete it->second;
      }
      cache_map_.erase(it);
    }
  }
  return ret;
}

int ObQueryResultCache::clear() {
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    // 释放所有缓存的结果
    for (auto &pair : cache_map_) {
      if (pair.second != nullptr) {
        delete pair.second;
      }
    }

    cache_map_.clear();
    LOG_INFO("query result cache cleared");
  }
  return ret;
}

ObCachedQueryResult *ObQueryResultCache::alloc_result() {
  return new (std::nothrow) ObCachedQueryResult();
}

void ObQueryResultCache::free_result(ObCachedQueryResult *result) {
  if (result != nullptr) {
    delete result;
  }
}

bool ObQueryResultCache::is_cacheable_sql(const std::string &sql) {
  bool cacheable = false;

  if (sql.length() > 0 && sql.length() < 4096) { // 限制SQL长度
    // 跳过前导空白
    size_t pos = 0;
    while (pos < sql.length() && (sql[pos] == ' ' || sql[pos] == '\t' ||
                                  sql[pos] == '\n' || sql[pos] == '\r')) {
      ++pos;
    }

    // 检查是否是SELECT语句（不区分大小写）
    if (pos + 6 <= sql.length()) {
      char c0 = sql[pos], c1 = sql[pos + 1], c2 = sql[pos + 2];
      char c3 = sql[pos + 3], c4 = sql[pos + 4], c5 = sql[pos + 5];

      if ((c0 == 'S' || c0 == 's') && (c1 == 'E' || c1 == 'e') &&
          (c2 == 'L' || c2 == 'l') && (c3 == 'E' || c3 == 'e') &&
          (c4 == 'C' || c4 == 'c') && (c5 == 'T' || c5 == 't')) {
        cacheable = true;

        // 转换为大写进行关键字检查
        std::string upper_sql = sql;
        std::set<std::string> set = {
            "select v",

            "select 1",

            "select * from __all_virtual_mem_leak_checker_info",

            "select sum(alloc_size) from __all_virtual_mem_leak_checker_info "
            "into result_tmp",

            "select @mysqltest_mode into @my_mysqltest_mode",

            "into result_tmp",

            "select count(*) from oceanbase.DBA_OB_SERVERS group by zone limit "
            "1 into num",

            "select zone from (select zone, count(*) as a from "
            "oceanbase.DBA_OB_ZONES group by region order by a desc limit 1) "

            "into zone_name",
            "select value from oceanbase.CDB_OB_SYS_VARIABLES where name = "
            "'recyclebin' and tenant_id=1 into recyclebin_value",

            "select memory_limit from GV$OB_SERVERS limit 1 into mem",

        };
        if (set.find(sql) != set.end()) {
          return false;
        }

        for (auto &c : upper_sql) {
          if (c >= 'a' && c <= 'z') {
            c = c - 'a' + 'A';
          }
        }

        // 检查是否包含不可缓存的关键字
        if (upper_sql.find("NOW(") != std::string::npos ||
            upper_sql.find("RAND(") != std::string::npos ||
            upper_sql.find("UUID(") != std::string::npos ||
            upper_sql.find("FOR UPDATE") != std::string::npos ||
            upper_sql.find("SQL_NO_CACHE") != std::string::npos ||
            upper_sql.find("SYSDATE") != std::string::npos ||
            upper_sql.find("CURRENT_TIMESTAMP") != std::string::npos) {
          cacheable = false;
        }
      }
    }
  }
  if (cacheable) {
    int ret = OB_SUCCESS;
    LOG_DEBUG("sql is cacheable", K(sql.c_str()), K(ret));
  }

  return cacheable;
}

} // namespace sql
} // namespace oceanbase
