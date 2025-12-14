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

#ifndef OB_TOKEN_POSTING_LIST_CACHE_H
#define OB_TOKEN_POSTING_LIST_CACHE_H

#include "common/ob_tablet_id.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/cache/ob_kvcache_struct.h"
#include <cstdint>

namespace oceanbase {
namespace storage {

/**
 * Cache key for token posting list cache.
 * Key = (tenant_id, index_id, tablet_id, token)
 */
class ObTokenPostingListCacheKey : public common::ObIKVCacheKey {
public:
  ObTokenPostingListCacheKey()
      : tenant_id_(0), index_id_(0), tablet_id_(), token_() {}
  ObTokenPostingListCacheKey(const uint64_t tenant_id, const uint64_t index_id,
                             const common::ObTabletID tablet_id,
                             const common::ObString &token)
      : tenant_id_(tenant_id), index_id_(index_id), tablet_id_(tablet_id),
        token_(token) {}

  virtual ~ObTokenPostingListCacheKey() {}

  virtual bool operator==(const ObIKVCacheKey &other) const override {
    const ObTokenPostingListCacheKey &other_key =
        reinterpret_cast<const ObTokenPostingListCacheKey &>(other);
    return (&other == this) ||
           (other_key.tenant_id_ == tenant_id_ &&
            other_key.index_id_ == index_id_ &&
            other_key.tablet_id_ == tablet_id_ && other_key.token_ == token_);
  }

  virtual uint64_t hash() const override {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    hash_val = common::murmurhash(&index_id_, sizeof(index_id_), hash_val);
    uint64_t tablet_id_val = tablet_id_.id();
    hash_val =
        common::murmurhash(&tablet_id_val, sizeof(tablet_id_val), hash_val);
    hash_val = common::murmurhash(token_.ptr(), token_.length(), hash_val);
    return hash_val;
  }

  virtual int equal(const ObIKVCacheKey &other, bool &equal) const override {
    equal = *this == other;
    return common::OB_SUCCESS;
  }

  virtual int hash(uint64_t &hash_value) const override {
    hash_value = hash();
    return common::OB_SUCCESS;
  }

  virtual uint64_t get_tenant_id() const override { return tenant_id_; }

  virtual int64_t size() const override {
    return sizeof(ObTokenPostingListCacheKey) + token_.length();
  }

  virtual int deep_copy(char *buf, const int64_t buf_len,
                        ObIKVCacheKey *&key) const override {
    int ret = common::OB_SUCCESS;
    const int64_t deep_copy_size = size();
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < deep_copy_size)) {
      ret = common::OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument for token doc cnt cache key deep copy",
                 K(ret), K(buf_len), K(deep_copy_size));
    } else {
      ObTokenPostingListCacheKey *new_key =
          new (buf) ObTokenPostingListCacheKey();
      new_key->tenant_id_ = tenant_id_;
      new_key->index_id_ = index_id_;
      new_key->tablet_id_ = tablet_id_;

      char *token_buf = buf + sizeof(ObTokenPostingListCacheKey);
      MEMCPY(token_buf, token_.ptr(), token_.length());
      new_key->token_.assign_ptr(token_buf, token_.length());

      key = new_key;
    }
    return ret;
  }

  // Getters for accessing key components
  uint64_t get_index_id() const { return index_id_; }
  common::ObTabletID get_tablet_id() const { return tablet_id_; }
  const common::ObString &get_token() const { return token_; }

  TO_STRING_KV(K_(tenant_id), K_(index_id), K_(tablet_id), K_(token));

private:
  uint64_t tenant_id_;
  uint64_t index_id_;
  common::ObTabletID tablet_id_;
  common::ObString token_;
};

/**
 * Wrapper key for pending map that owns the token string memory.
 * Used as HashMap key to avoid dangling pointers.
 */
struct PendingCacheKey {
  PendingCacheKey()
      : tenant_id_(0), index_id_(0), tablet_id_(), token_buf_(), token_len_(0) {
  }

  PendingCacheKey(const ObTokenPostingListCacheKey &key) {
    tenant_id_ = key.get_tenant_id();
    index_id_ = key.get_index_id();
    tablet_id_ = key.get_tablet_id();
    const common::ObString &token = key.get_token();
    token_len_ =
        std::min(token.length(), static_cast<int32_t>(sizeof(token_buf_)));
    if (token_len_ > 0 && token.ptr() != nullptr) {
      MEMCPY(token_buf_, token.ptr(), token_len_);
    }
  }

  bool operator==(const PendingCacheKey &other) const {
    return tenant_id_ == other.tenant_id_ && index_id_ == other.index_id_ &&
           tablet_id_ == other.tablet_id_ && token_len_ == other.token_len_ &&
           (token_len_ == 0 ||
            MEMCMP(token_buf_, other.token_buf_, token_len_) == 0);
  }

  uint64_t hash() const {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    hash_val = common::murmurhash(&index_id_, sizeof(index_id_), hash_val);
    uint64_t tablet_id_val = tablet_id_.id();
    hash_val =
        common::murmurhash(&tablet_id_val, sizeof(tablet_id_val), hash_val);
    hash_val = common::murmurhash(token_buf_, token_len_, hash_val);
    return hash_val;
  }

  // Convert back to ObTokenPostingListCacheKey
  ObTokenPostingListCacheKey to_cache_key() const {
    common::ObString token(token_len_, token_buf_);
    return ObTokenPostingListCacheKey(tenant_id_, index_id_, tablet_id_, token);
  }

  TO_STRING_KV(K_(tenant_id), K_(index_id), K_(tablet_id), K_(token_len));

  uint64_t tenant_id_;
  uint64_t index_id_;
  common::ObTabletID tablet_id_;
  char token_buf_[256]; // Max token length, adjust as needed
  int32_t token_len_;
};

// Hash functor for PendingCacheKey (OceanBase style)
struct PendingCacheKeyHash {
  int operator()(const PendingCacheKey &key, uint64_t &hash_value) const {
    hash_value = key.hash();
    return common::OB_SUCCESS;
  }
};

// Equal functor for PendingCacheKey (OceanBase style)
struct PendingCacheKeyEqual {
  bool operator()(const PendingCacheKey &lhs,
                  const PendingCacheKey &rhs) const {
    return lhs == rhs;
  }
};

struct PostingEntry {
  PostingEntry() : doc_id_(0), token_frequency_(0), doc_len_(0) {}
  PostingEntry(int64_t doc_id, int64_t token_frequency, int64_t doc_len)
      : doc_id_(doc_id), token_frequency_(token_frequency), doc_len_(doc_len) {}
  int64_t doc_id_;
  int64_t token_frequency_;
  int64_t doc_len_;
  TO_STRING_KV(K_(doc_id), K_(token_frequency), K_(doc_len));
};

/**
 * Cache value for token posting list cache.
 * Stores the posting entries for a token.
 */
class ObTokenPostingListValue : public common::ObIKVCacheValue {
public:
  ObTokenPostingListValue() {}

  virtual ~ObTokenPostingListValue() {}

  virtual int64_t size() const override {
    return sizeof(ObTokenPostingListValue) +
           postentry_list_.count() * sizeof(PostingEntry);
  }

  virtual int deep_copy(char *buf, const int64_t buf_len,
                        ObIKVCacheValue *&value) const override {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
      ret = common::OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN,
                 "invalid argument for token doc cnt cache value deep copy",
                 K(ret), K(buf_len), K(size()));
    } else {
      ObTokenPostingListValue *new_value = new (buf) ObTokenPostingListValue();
      if (OB_FAIL(new_value->postentry_list_.assign(postentry_list_))) {
        COMMON_LOG(WARN, "failed to assign postentry list", K(ret));
      } else {
        value = new_value;
      }
    }
    return ret;
  }

  const ObArray<PostingEntry> &postentry_list() const {
    return postentry_list_;
  }

  TO_STRING_KV(K_(postentry_list));

public:
  ObArray<PostingEntry> postentry_list_;
};

/**
 * Pending posting list for dynamic building.
 * Stored in ObHashMap during iteration, moved to KVCache on completion.
 */
struct PendingPostingList {
  PendingPostingList() : entries_() {}
  ObArray<PostingEntry> entries_;
};

/**
 * Global cache for token posting lists.
 * Uses two-tier caching:
 * 1. pending_map_: ObHashMap for dynamic appending during iteration
 * 2. KVCache: for completed, immutable posting lists
 */
class ObTokenPostingListCache
    : public common::ObKVCache<ObTokenPostingListCacheKey,
                               ObTokenPostingListValue> {
public:
  static ObTokenPostingListCache &get_instance() {
    static ObTokenPostingListCache cache;
    return cache;
  }

  // Initialize the cache (call once at startup)
  int init(const char *cache_name, const int64_t priority = 1) {
    int ret = common::OB_SUCCESS;
    ret = ObKVCache::init(cache_name, priority);
    if (OB_FAIL(ret)) {
      COMMON_LOG(WARN, "failed to init kv cache", K(ret));
    } else if (!pending_map_.created()) {
      ret = pending_map_.create(1024, "PendingPostMap", "PendingPostNode");
    }
    return ret;
  }

  // Get from KVCache (completed posting lists only)
  int get_posting_list(const ObTokenPostingListCacheKey &key,
                       const ObTokenPostingListValue *&value,
                       common::ObKVCacheHandle &handle);

  // Put completed posting list to KVCache
  int put_posting_list(const ObTokenPostingListCacheKey &key,
                       const ObTokenPostingListValue &value);

  // Append entry to pending list (dynamic building)
  int append_to_pending(const ObTokenPostingListCacheKey &key,
                        const PostingEntry &entry);

  // Get pending list for reading (returns nullptr if not found)
  const PendingPostingList *
  get_pending(const ObTokenPostingListCacheKey &key) const;

  // Finalize pending list to KVCache and remove from pending map
  int finalize_to_cache(const ObTokenPostingListCacheKey &key);

  // Clear pending list without caching (e.g., on error)
  int clear_pending(const ObTokenPostingListCacheKey &key);

  // Finalize all pending lists to KVCache and clear pending map
  int finalize_all_pending();

  // Clear all pending lists without caching
  int clear_all_pending();

private:
  ObTokenPostingListCache() {}
  virtual ~ObTokenPostingListCache() {}

  // Pending map: PendingCacheKey -> PendingPostingList
  common::hash::ObHashMap<PendingCacheKey, PendingPostingList,
                          common::hash::NoPthreadDefendMode,
                          PendingCacheKeyHash, PendingCacheKeyEqual>
      pending_map_;

  DISALLOW_COPY_AND_ASSIGN(ObTokenPostingListCache);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_TOKEN_POSTING_LIST_CACHE_H
