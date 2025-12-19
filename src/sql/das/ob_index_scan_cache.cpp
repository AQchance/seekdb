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

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/ob_index_scan_cache.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace sql
{

//------------------------------------------------------------------------------
// ObIndexScanCacheValue Implementation
//------------------------------------------------------------------------------

int ObIndexScanCacheValue::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = common::OB_SUCCESS;
  const int64_t copy_size = size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < copy_size)) {
    ret = common::OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument for index scan cache value deep copy",
               K(ret), K(buf_len), K(copy_size), K(count_));
  } else {
    ObIndexScanCacheValue *new_value = new (buf) ObIndexScanCacheValue();
    new_value->count_ = count_;
    if (count_ > 0) {
      MEMCPY(new_value->ids_, ids_, count_ * sizeof(uint64_t));
    }
    value = new_value;
  }
  return ret;
}

int ObIndexScanCacheValue::create_value(const ObArray<uint64_t> &ids, char *buf,
                                        int64_t buf_len, ObIndexScanCacheValue *&out_value)
{
  int ret = common::OB_SUCCESS;
  const int64_t needed_size = calc_size(ids.count());
  if (OB_ISNULL(buf) || buf_len < needed_size) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(needed_size));
  } else {
    ObIndexScanCacheValue *value = new (buf) ObIndexScanCacheValue();
    value->count_ = ids.count();
    for (int64_t i = 0; i < ids.count(); ++i) {
      value->ids_[i] = ids.at(i);
    }
    out_value = value;
  }
  return ret;
}

//------------------------------------------------------------------------------
// ObIndexScanCache Implementation
//------------------------------------------------------------------------------

int ObIndexScanCache::init(const char *cache_name, const int64_t priority)
{
  int ret = common::OB_SUCCESS;
  ret = ObKVCache::init(cache_name, priority);
  if (OB_FAIL(ret)) {
    COMMON_LOG(WARN, "failed to init kv cache", K(ret));
  } else if (!pending_map_.created()) {
    ret = pending_map_.create(1024, "PendingIdxScan", "PendingIdxNode");
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to create pending map", K(ret));
    }
  }
  return ret;
}

int ObIndexScanCache::get_cached_ids(
    const ObIndexScanCacheKey &key,
    const ObIndexScanCacheValue *&value,
    common::ObKVCacheHandle &handle)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(get(key, value, handle))) {
    if (common::OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get from index scan cache", K(ret), K(key));
    }
  }
  return ret;
}

int ObIndexScanCache::put_cached_ids(
    const ObIndexScanCacheKey &key,
    const ObIndexScanCacheValue &value)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(put(key, value))) {
    LOG_WARN("failed to put into index scan cache", K(ret), K(key), K(value));
  }
  return ret;
}

int ObIndexScanCache::append_to_pending(const ObIndexScanCacheKey &key, const uint64_t id)
{
  int ret = common::OB_SUCCESS;
  PendingIdList pending_list;

  ret = pending_map_.get_refactored(key, pending_list);
  if (common::OB_HASH_NOT_EXIST == ret) {
    // Create new pending list
    PendingIdList new_list;
    if (OB_FAIL(new_list.ids_.push_back(id))) {
      LOG_WARN("failed to push back id", K(ret), K(id));
    } else if (OB_FAIL(pending_map_.set_refactored(key, new_list))) {
      LOG_WARN("failed to set pending list", K(ret), K(key));
    }
  } else if (OB_SUCCESS == ret) {
    // Append to existing list
    if (OB_FAIL(pending_list.ids_.push_back(id))) {
      LOG_WARN("failed to push back id", K(ret), K(id));
    } else if (OB_FAIL(pending_map_.set_refactored(key, pending_list, 1 /* overwrite */))) {
      LOG_WARN("failed to update pending list", K(ret), K(key));
    }
  } else {
    LOG_WARN("failed to get pending list", K(ret), K(key));
  }
  return ret;
}

const PendingIdList *ObIndexScanCache::get_pending(const ObIndexScanCacheKey &key) const
{
  // Note: This returns nullptr if not found. Use get_refactored for actual lookup.
  // For thread safety, caller should copy the data.
  return nullptr;  // Not implemented for thread safety reasons
}

int ObIndexScanCache::finalize_to_cache(const ObIndexScanCacheKey &key)
{
  int ret = common::OB_SUCCESS;
  PendingIdList pending_list;

  if (OB_FAIL(pending_map_.get_refactored(key, pending_list))) {
    if (common::OB_HASH_NOT_EXIST == ret) {
      ret = common::OB_SUCCESS;  // Nothing to finalize
    } else {
      LOG_WARN("failed to get pending list", K(ret), K(key));
    }
  } else if (pending_list.ids_.count() > 0) {
    const int64_t buf_size = ObIndexScanCacheValue::calc_size(pending_list.ids_.count());
    char *buf = static_cast<char *>(common::ob_malloc(buf_size, "IdxScanCache"));
    if (OB_ISNULL(buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for cache value", K(ret), K(buf_size));
    } else {
      ObIndexScanCacheValue *cache_value = nullptr;
      if (OB_FAIL(ObIndexScanCacheValue::create_value(pending_list.ids_, buf, buf_size, cache_value))) {
        LOG_WARN("failed to create value", K(ret));
      } else if (OB_FAIL(put(key, *cache_value))) {
        LOG_WARN("failed to put cache value", K(ret), K(key));
      }
      common::ob_free(buf);
    }

    // Remove from pending map
    if (OB_SUCC(ret)) {
      if (OB_FAIL(pending_map_.erase_refactored(key))) {
        if (common::OB_HASH_NOT_EXIST == ret) {
          ret = common::OB_SUCCESS;
        } else {
          LOG_WARN("failed to erase pending list", K(ret), K(key));
        }
      }
    }
  }
  return ret;
}

int ObIndexScanCache::clear_pending(const ObIndexScanCacheKey &key)
{
  int ret = common::OB_SUCCESS;
  ret = pending_map_.erase_refactored(key);
  if (common::OB_HASH_NOT_EXIST == ret) {
    ret = common::OB_SUCCESS;
  }
  return ret;
}

int ObIndexScanCache::finalize_all_pending()
{
  int ret = common::OB_SUCCESS;
  ObArray<ObIndexScanCacheKey> keys_to_finalize;

  // Collect all keys first
  for (auto iter = pending_map_.begin(); OB_SUCC(ret) && iter != pending_map_.end(); ++iter) {
    if (OB_FAIL(keys_to_finalize.push_back(iter->first))) {
      LOG_WARN("failed to push back key", K(ret));
    }
  }

  // Finalize each key
  for (int64_t i = 0; OB_SUCC(ret) && i < keys_to_finalize.count(); ++i) {
    if (OB_FAIL(finalize_to_cache(keys_to_finalize.at(i)))) {
      LOG_WARN("failed to finalize pending list", K(ret));
    }
  }
  return ret;
}

int ObIndexScanCache::clear_all_pending()
{
  pending_map_.reuse();
  return common::OB_SUCCESS;
}

} // namespace sql
} // namespace oceanbase
