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
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include <cstdint>

namespace oceanbase
{
namespace sql
{

/**
 * Cache key for index scan cache.
 * Key = (tenant_id, index_id, tablet_id, start_keys[], end_keys[])
 */
class ObIndexScanCacheKey : public common::ObIKVCacheKey
{
public:
  static constexpr int64_t MAX_RANGE_COUNT = 16;

  ObIndexScanCacheKey()
    : tenant_id_(0), index_id_(0), tablet_id_(), range_count_(0) {}

  ObIndexScanCacheKey(uint64_t tenant_id, uint64_t index_id, common::ObTabletID tablet_id)
    : tenant_id_(tenant_id), index_id_(index_id), tablet_id_(tablet_id), range_count_(0) {}

  virtual ~ObIndexScanCacheKey() {}

  template<typename RangeArray>
  int set_key_ranges(const RangeArray &key_ranges)
  {
    int ret = common::OB_SUCCESS;
    range_count_ = std::min(static_cast<int64_t>(key_ranges.count()), MAX_RANGE_COUNT);
    for (int64_t i = 0; i < range_count_; ++i) {
      const common::ObNewRange &range = key_ranges.at(i);
      const common::ObRowkey &start_key = range.get_start_key();
      const common::ObRowkey &end_key = range.get_end_key();
      start_keys_[i] = start_key.get_obj_cnt() > 0 ? start_key.get_obj_ptr()[0] : common::ObObj();
      end_keys_[i] = end_key.get_obj_cnt() > 0 ? end_key.get_obj_ptr()[0] : common::ObObj();
    }
    return ret;
  }

  virtual bool operator==(const ObIKVCacheKey &other) const override;
  virtual uint64_t hash() const override;
  virtual int equal(const ObIKVCacheKey &other, bool &equal) const override { equal = *this == other; return common::OB_SUCCESS; }
  virtual int hash(uint64_t &hash_value) const override { hash_value = hash(); return common::OB_SUCCESS; }
  virtual uint64_t get_tenant_id() const override { return tenant_id_; }
  virtual int64_t size() const override { return sizeof(ObIndexScanCacheKey); }
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override;

  TO_STRING_KV(K_(tenant_id), K_(index_id), K_(tablet_id), K_(range_count));

private:
  uint64_t tenant_id_;
  uint64_t index_id_;
  common::ObTabletID tablet_id_;
  int64_t range_count_;
  common::ObObj start_keys_[MAX_RANGE_COUNT];
  common::ObObj end_keys_[MAX_RANGE_COUNT];
};

// Hash functor
struct IndexScanCacheKeyHash {
  int operator()(const ObIndexScanCacheKey &key, uint64_t &hash_value) const {
    hash_value = key.hash();
    return common::OB_SUCCESS;
  }
};

// Equal functor
struct IndexScanCacheKeyEqual {
  bool operator()(const ObIndexScanCacheKey &lhs, const ObIndexScanCacheKey &rhs) const {
    return lhs == rhs;
  }
};

/**
 * Cache value storing serialized row data.
 * Memory layout: [header][row_sizes...][row data buffers...]
 * 
 * Each row is a serialized StoredRow that can be used directly with LastStoredRow.
 */
class ObIndexScanCacheValue : public common::ObIKVCacheValue
{
public:
  ObIndexScanCacheValue() : row_count_(0), total_data_size_(0) {}
  virtual ~ObIndexScanCacheValue() {}

  virtual int64_t size() const override
  {
    return sizeof(ObIndexScanCacheValue) + row_count_ * sizeof(int64_t) + total_data_size_;
  }

  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;

  int64_t row_count() const { return row_count_; }
  
  // Get size of row at index
  int64_t get_row_size(int64_t idx) const {
    if (idx < 0 || idx >= row_count_) return 0;
    const int64_t *sizes = reinterpret_cast<const int64_t*>(data_);
    return sizes[idx];
  }
  
  // Get pointer to row data at index
  const char* get_row_data(int64_t idx) const {
    if (idx < 0 || idx >= row_count_) return nullptr;
    const int64_t *sizes = reinterpret_cast<const int64_t*>(data_);
    const char *row_data = data_ + row_count_ * sizeof(int64_t);
    for (int64_t i = 0; i < idx; ++i) {
      row_data += sizes[i];
    }
    return row_data;
  }

  static int64_t calc_size(int64_t row_count, int64_t total_data_size)
  {
    return sizeof(ObIndexScanCacheValue) + row_count * sizeof(int64_t) + total_data_size;
  }

  TO_STRING_KV(K_(row_count), K_(total_data_size));

public:
  int64_t row_count_;
  int64_t total_data_size_;
  char data_[0];  // Flexible array: [row_sizes[row_count]][row_data...]
};

/**
 * Pending row list for dynamic building.
 */
struct PendingRowList
{
  struct RowInfo {
    char *data;
    int64_t size;
    TO_STRING_KV(KP(data), K(size));
  };
  
  PendingRowList() : rows_(), allocator_("PendingRows") {}
  ~PendingRowList() { clear(); }
  
  void clear() {
    rows_.reset();
    allocator_.reset();
  }
  
  int append_row(const ObChunkDatumStore::StoredRow *row);
  
  ObArray<RowInfo> rows_;
  common::ObArenaAllocator allocator_;
};

/**
 * Global cache for index scan row results.
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
  int get_cached_rows(const ObIndexScanCacheKey &key,
                      const ObIndexScanCacheValue *&value,
                      common::ObKVCacheHandle &handle);

  // Append a row to pending list
  int append_row_to_pending(const ObIndexScanCacheKey &key, 
                            const ObChunkDatumStore::StoredRow *row);

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

  common::hash::ObHashMap<ObIndexScanCacheKey, PendingRowList*,
                          common::hash::NoPthreadDefendMode,
                          IndexScanCacheKeyHash, IndexScanCacheKeyEqual>
      pending_map_;

  DISALLOW_COPY_AND_ASSIGN(ObIndexScanCache);
};

} // namespace sql
} // namespace oceanbase

#endif // OB_INDEX_SCAN_CACHE_H_
