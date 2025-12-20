/*
 * Copyright (c) 2025 OceanBase.
 */

#include "storage/retrieval/ob_token_posting_list_cache.h"
#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_index_scan_cache.h"
#include "lib/oblog/ob_log.h"
#include "lib/allocator/ob_malloc.h"
#include "share/cache/ob_kvcache_struct.h"
#include "share/cache/ob_kv_storecache.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace sql
{

// ObIndexScanCacheKey implementations

bool ObIndexScanCacheKey::operator==(const ObIKVCacheKey &other) const
{
  const ObIndexScanCacheKey &other_key = reinterpret_cast<const ObIndexScanCacheKey &>(other);
  if (&other == this) return true;
  if (tenant_id_ != other_key.tenant_id_ || index_id_ != other_key.index_id_ ||
      tablet_id_ != other_key.tablet_id_ || range_count_ != other_key.range_count_) {
    return false;
  }
  for (int64_t i = 0; i < range_count_; ++i) {
    if (start_keys_[i] != other_key.start_keys_[i] || end_keys_[i] != other_key.end_keys_[i]) {
      return false;
    }
  }
  return true;
}

uint64_t ObIndexScanCacheKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
  hash_val = common::murmurhash(&index_id_, sizeof(index_id_), hash_val);
  uint64_t tablet_id_val = tablet_id_.id();
  hash_val = common::murmurhash(&tablet_id_val, sizeof(tablet_id_val), hash_val);
  for (int64_t i = 0; i < range_count_; ++i) {
    hash_val = start_keys_[i].hash(hash_val);
    hash_val = end_keys_[i].hash(hash_val);
  }
  return hash_val;
}

int ObIndexScanCacheKey::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_len < size()) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    ObIndexScanCacheKey *new_key = new (buf) ObIndexScanCacheKey();
    new_key->tenant_id_ = tenant_id_;
    new_key->index_id_ = index_id_;
    new_key->tablet_id_ = tablet_id_;
    new_key->range_count_ = range_count_;
    for (int64_t i = 0; i < range_count_; ++i) {
      new_key->start_keys_[i] = start_keys_[i];
      new_key->end_keys_[i] = end_keys_[i];
    }
    key = new_key;
  }
  return ret;
}

// ObIndexScanCacheValue implementations

int ObIndexScanCacheValue::deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
{
  int ret = common::OB_SUCCESS;
  int64_t need_size = size();
  if (OB_ISNULL(buf) || buf_len < need_size) {
    ret = common::OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(buf, static_cast<const void*>(this), need_size);
    value = reinterpret_cast<ObIndexScanCacheValue*>(buf);
  }
  return ret;
}

// PendingRowList implementations

int PendingRowList::append_row(const ObChunkDatumStore::StoredRow *row)
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(row) || row->cnt_ == 0) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row", K(ret), KP(row));
  } else {
    // Extract first column (rowkey) as int64
    const ObDatum *cells = row->cells();
    if (cells[0].is_null()) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey is null", K(ret));
    } else if (cells[0].len_ != sizeof(int64_t)) {
      ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected rowkey format", K(ret), K(cells[0].len_));
    } else {
      int64_t rowkey = cells[0].get_int();
      if (OB_FAIL(rowkeys_.push_back(rowkey))) {
        LOG_WARN("failed to push back rowkey", K(ret), K(rowkey));
      }
    }
  }
  return ret;
}

// ObIndexScanCache implementations

int ObIndexScanCache::init(const char *cache_name, const int64_t priority)
{
  int ret = common::OB_SUCCESS;
  // Call base class ObKVCache::init first
  if (OB_FAIL(ObKVCache::init(cache_name, priority))) {
    LOG_WARN("failed to init ObKVCache base class", K(ret));
  } else if (!pending_map_.created()) {
    if (OB_FAIL(pending_map_.create(1024, "IdxScanPending", "IdxScanPNode"))) {
      LOG_WARN("failed to create pending map", K(ret));
    }
  }
  return ret;
}

int ObIndexScanCache::get_cached_rows(const ObIndexScanCacheKey &key,
                                      const ObIndexScanCacheValue *&value,
                                      common::ObKVCacheHandle &handle)
{
  return this->get(key, value, handle);
}

int ObIndexScanCache::append_row_to_pending(const ObIndexScanCacheKey &key,
                                            const ObChunkDatumStore::StoredRow *row)
{
  int ret = common::OB_SUCCESS;
  PendingRowList *list = nullptr;
  
  if (OB_FAIL(pending_map_.get_refactored(key, list))) {
    if (ret == common::OB_HASH_NOT_EXIST) {
      // Create new list
      void *buf = ob_malloc(sizeof(PendingRowList), "PendingList");
      if (OB_ISNULL(buf)) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
      } else {
        list = new (buf) PendingRowList();
        if (OB_FAIL(pending_map_.set_refactored(key, list))) {
          list->~PendingRowList();
          ob_free(buf);
          LOG_WARN("failed to set pending list to map", K(ret));
        } else {
          ret = list->append_row(row);
        }
      }
    } else {
      LOG_WARN("failed to get pending list", K(ret));
    }
  } else if (OB_ISNULL(list)) {
    ret = common::OB_ERR_UNEXPECTED;
  } else {
    ret = list->append_row(row);
  }
  
  return ret;
}

int ObIndexScanCache::finalize_to_cache(const ObIndexScanCacheKey &key)
{
  int ret = common::OB_SUCCESS;
  PendingRowList *list = nullptr;
  
  if (OB_FAIL(pending_map_.get_refactored(key, list))) {
    // Not in pending, skip
    if (ret == common::OB_HASH_NOT_EXIST) {
      ret = common::OB_SUCCESS;
    }
  } else if (OB_ISNULL(list) || list->rowkeys_.count() == 0) {
    // Empty list, remove from map
    pending_map_.erase_refactored(key);
    if (list != nullptr) {
      list->~PendingRowList();
      ob_free(list);
    }
  } else {
    // Calculate size and allocate cache value
    int64_t rowkey_count = list->rowkeys_.count();
    int64_t value_size = ObIndexScanCacheValue::calc_size(rowkey_count);
    
    char *buf = static_cast<char*>(ob_malloc(value_size, "IdxScanVal"));
    if (OB_ISNULL(buf)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc cache value", K(ret), K(value_size));
    } else {
      ObIndexScanCacheValue *value = new (buf) ObIndexScanCacheValue();
      value->rowkey_count_ = rowkey_count;
      
      // Copy rowkeys to cache value
      for (int64_t i = 0; i < rowkey_count; ++i) {
        value->rowkeys_[i] = list->rowkeys_[i];
      }
      
      // Put to cache
      if (OB_FAIL(this->put(key, *value))) {
        LOG_WARN("failed to put to cache", K(ret));
      } else {
        LOG_INFO("[INDEX_SCAN_CACHE] Finalized to cache", K(rowkey_count));
      }
      
      ob_free(buf);
    }
    
    // Clean up
    list->~PendingRowList();
    ob_free(list);
    pending_map_.erase_refactored(key);
  }
  return ret;
}

int ObIndexScanCache::clear_pending(const ObIndexScanCacheKey &key)
{
  int ret = common::OB_SUCCESS;
  PendingRowList *list = nullptr;
  if (OB_SUCC(pending_map_.get_refactored(key, list)) && list != nullptr) {
    list->~PendingRowList();
    ob_free(list);
  }
  pending_map_.erase_refactored(key);
  return common::OB_SUCCESS;
}

int ObIndexScanCache::finalize_all_pending()
{
  int ret = common::OB_SUCCESS;
  // Copy keys first to avoid modifying map during iteration
  ObArray<ObIndexScanCacheKey> keys;
  for (auto it = pending_map_.begin(); it != pending_map_.end(); ++it) {
    keys.push_back(it->first);
  }
  for (int64_t i = 0; i < keys.count(); ++i) {
    int tmp_ret = finalize_to_cache(keys[i]);
    if (tmp_ret != common::OB_SUCCESS) {
      LOG_WARN("failed to finalize pending", K(tmp_ret), K(i));
    }
  }
  return ret;
}

int ObIndexScanCache::clear_all_pending()
{
  int ret = common::OB_SUCCESS;
  for (auto it = pending_map_.begin(); it != pending_map_.end(); ++it) {
    PendingRowList *list = it->second;
    if (list != nullptr) {
      list->~PendingRowList();
      ob_free(list);
    }
  }
  pending_map_.clear();
  return ret;
}

} // namespace sql
} // namespace oceanbase
