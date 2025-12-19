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

#ifndef OB_INDEX_SCAN_CACHE_H_
#define OB_INDEX_SCAN_CACHE_H_

#include "common/ob_tablet_id.h"
#include "common/object/ob_object.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/cache/ob_kvcache_struct.h"
#include "common/ob_range.h"
#include <cstdint>

namespace oceanbase
{
namespace sql
{

/**
 * Cache key for index scan cache.
 * Key = (tenant_id, index_id, tablet_id, start_keys[], end_keys[])
 * 
 * Each element stores the FIRST COLUMN of each key_range's start_key/end_key.
 * Supports multiple key_ranges (e.g., from OR conditions).
 */
class ObIndexScanCacheKey : public common::ObIKVCacheKey
{
public:
  static constexpr int64_t MAX_RANGE_COUNT = 16;  // 最多支持 16 个 key_range

  ObIndexScanCacheKey()
    : tenant_id_(0),
      index_id_(0),
      tablet_id_(),
      range_count_(0)
  {}

  ObIndexScanCacheKey(
      const uint64_t tenant_id,
      const uint64_t index_id,
      const common::ObTabletID tablet_id)
    : tenant_id_(tenant_id),
      index_id_(index_id),
      tablet_id_(tablet_id),
      range_count_(0)
  {}

  virtual ~ObIndexScanCacheKey() {}

  // Set from key_ranges array (copies first column of each start_key/end_key)
  template<typename RangeArray>
  int set_key_ranges(const RangeArray &key_ranges)
  {
    int ret = common::OB_SUCCESS;
    range_count_ = std::min(static_cast<int64_t>(key_ranges.count()), MAX_RANGE_COUNT);
    
    for (int64_t i = 0; i < range_count_; ++i) {
      const common::ObNewRange &range = key_ranges.at(i);
      
      // 获取 start_key 的第一列
      const common::ObRowkey &start_key = range.get_start_key();
      if (start_key.get_obj_cnt() > 0) {
        start_keys_[i] = start_key.get_obj_ptr()[0];
      } else {
        start_keys_[i].set_min_value();
      }
      
      // 获取 end_key 的第一列
      const common::ObRowkey &end_key = range.get_end_key();
      if (end_key.get_obj_cnt() > 0) {
        end_keys_[i] = end_key.get_obj_ptr()[0];
      } else {
        end_keys_[i].set_max_value();
      }
    }
    return ret;
  }

  // Getters
  uint64_t get_index_id() const { return index_id_; }
  common::ObTabletID get_tablet_id() const { return tablet_id_; }
  int64_t get_range_count() const { return range_count_; }
  const common::ObObj *get_start_keys() const { return start_keys_; }
  const common::ObObj *get_end_keys() const { return end_keys_; }

  virtual bool operator==(const ObIKVCacheKey &other) const override
  {
    const ObIndexScanCacheKey &other_key = reinterpret_cast<const ObIndexScanCacheKey &>(other);
    if (&other == this) {
      return true;
    }
    if (tenant_id_ != other_key.tenant_id_
        || index_id_ != other_key.index_id_
        || tablet_id_ != other_key.tablet_id_
        || range_count_ != other_key.range_count_) {
      return false;
    }
    // Compare each range's start_key and end_key
    for (int64_t i = 0; i < range_count_; ++i) {
      if (start_keys_[i] != other_key.start_keys_[i]) {
        return false;
      }
      if (end_keys_[i] != other_key.end_keys_[i]) {
        return false;
      }
    }
    return true;
  }

  virtual uint64_t hash() const override
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    hash_val = common::murmurhash(&index_id_, sizeof(index_id_), hash_val);
    uint64_t tablet_id_val = tablet_id_.id();
    hash_val = common::murmurhash(&tablet_id_val, sizeof(tablet_id_val), hash_val);
    // Hash each range's start_key and end_key
    for (int64_t i = 0; i < range_count_; ++i) {
      hash_val = start_keys_[i].hash(hash_val);
      hash_val = end_keys_[i].hash(hash_val);
    }
    return hash_val;
  }

  virtual int equal(const ObIKVCacheKey &other, bool &equal) const override
  {
    equal = *this == other;
    return common::OB_SUCCESS;
  }

  virtual int hash(uint64_t &hash_value) const override
  {
    hash_value = hash();
    return common::OB_SUCCESS;
  }

  virtual uint64_t get_tenant_id() const override { return tenant_id_; }

  virtual int64_t size() const override
  {
    return sizeof(ObIndexScanCacheKey);
  }

  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
      ret = common::OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument for index scan cache key deep copy",
                 K(ret), K(buf_len), K(size()));
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

  TO_STRING_KV(K_(tenant_id), K_(index_id), K_(tablet_id), K_(range_count));

private:
  uint64_t tenant_id_;
  uint64_t index_id_;
  common::ObTabletID tablet_id_;
  int64_t range_count_;                           // key_range 数量
  common::ObObj start_keys_[MAX_RANGE_COUNT];     // 每个 range 的 start_key 第一列
  common::ObObj end_keys_[MAX_RANGE_COUNT];       // 每个 range 的 end_key 第一列
};

// Hash functor for ObIndexScanCacheKey
struct IndexScanCacheKeyHash
{
  int operator()(const ObIndexScanCacheKey &key, uint64_t &hash_value) const
  {
    hash_value = key.hash();
    return common::OB_SUCCESS;
  }
};

// Equal functor for ObIndexScanCacheKey
struct IndexScanCacheKeyEqual
{
  bool operator()(const ObIndexScanCacheKey &lhs, const ObIndexScanCacheKey &rhs) const
  {
    return lhs == rhs;
  }
};

/**
 * Cache value for index scan cache.
 * Uses flexible array member for proper memory layout in KVCache.
 * Memory layout: [ObIndexScanCacheValue header][uint64_t ids...]
 */
class ObIndexScanCacheValue : public common::ObIKVCacheValue
{
public:
  ObIndexScanCacheValue() : count_(0) {}

  virtual ~ObIndexScanCacheValue() {}

  virtual int64_t size() const override
  {
    return sizeof(ObIndexScanCacheValue) + count_ * sizeof(uint64_t);
  }

  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;

  const uint64_t &at(int64_t idx) const { return ids_[idx]; }
  uint64_t &at(int64_t idx) { return ids_[idx]; }

  int64_t count() const { return count_; }

  static int64_t calc_size(int64_t id_count)
  {
    return sizeof(ObIndexScanCacheValue) + id_count * sizeof(uint64_t);
  }

  // Create a value in buffer from ObArray
  static int create_value(const ObArray<uint64_t> &ids, char *buf,
                          int64_t buf_len, ObIndexScanCacheValue *&out_value);

  TO_STRING_KV(K_(count));

public:
  int64_t count_;
  uint64_t ids_[0];  // Flexible array member
};

/**
 * Pending id list for dynamic building.
 */
struct PendingIdList
{
  PendingIdList() : ids_() {}
  ObArray<uint64_t> ids_;
};

/**
 * Global cache for index scan results.
 * Two-tier caching:
 * 1. pending_map_: ObHashMap for dynamic appending
 * 2. KVCache: for completed, immutable id lists
 */
class ObIndexScanCache
  : public common::ObKVCache<ObIndexScanCacheKey, ObIndexScanCacheValue>
{
public:
  static ObIndexScanCache &get_instance()
  {
    static ObIndexScanCache cache;
    return cache;
  }

  int init(const char *cache_name, const int64_t priority = 1);

  // Get from KVCache
  int get_cached_ids(const ObIndexScanCacheKey &key,
                     const ObIndexScanCacheValue *&value,
                     common::ObKVCacheHandle &handle);

  // Put to KVCache
  int put_cached_ids(const ObIndexScanCacheKey &key,
                     const ObIndexScanCacheValue &value);

  // Append id to pending list
  int append_to_pending(const ObIndexScanCacheKey &key, const uint64_t id);

  // Get pending list (returns nullptr if not found)
  const PendingIdList *get_pending(const ObIndexScanCacheKey &key) const;

  // Finalize pending list to KVCache
  int finalize_to_cache(const ObIndexScanCacheKey &key);

  // Clear pending list without caching
  int clear_pending(const ObIndexScanCacheKey &key);

  // Finalize all pending lists
  int finalize_all_pending();

  // Clear all pending lists
  int clear_all_pending();

private:
  ObIndexScanCache() {}
  virtual ~ObIndexScanCache() {}

  common::hash::ObHashMap<ObIndexScanCacheKey, PendingIdList,
                          common::hash::NoPthreadDefendMode,
                          IndexScanCacheKeyHash, IndexScanCacheKeyEqual>
      pending_map_;

  DISALLOW_COPY_AND_ASSIGN(ObIndexScanCache);
};

} // namespace sql
} // namespace oceanbase

#endif // OB_INDEX_SCAN_CACHE_H_
