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

#ifndef OB_TOKEN_DOC_CNT_CACHE_H_
#define OB_TOKEN_DOC_CNT_CACHE_H_

#include "lib/hash_func/murmur_hash.h"
#include "lib/string/ob_string.h"
#include "share/cache/ob_kv_storecache.h"
#include "share/cache/ob_kvcache_struct.h"
#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace storage
{

/**
 * Cache key for token document count cache.
 * Key = (tenant_id, index_id, tablet_id, token)
 */
class ObTokenDocCntCacheKey : public common::ObIKVCacheKey
{
public:
  ObTokenDocCntCacheKey()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      index_id_(common::OB_INVALID_ID),
      tablet_id_(),
      token_()
  {}

  ObTokenDocCntCacheKey(
      const uint64_t tenant_id,
      const uint64_t index_id,
      const common::ObTabletID tablet_id,
      const common::ObString &token)
    : tenant_id_(tenant_id),
      index_id_(index_id),
      tablet_id_(tablet_id),
      token_(token)
  {}

  virtual ~ObTokenDocCntCacheKey() {}

  virtual bool operator==(const ObIKVCacheKey &other) const override
  {
    const ObTokenDocCntCacheKey &other_key = reinterpret_cast<const ObTokenDocCntCacheKey &>(other);
    return (&other == this)
           || (other_key.tenant_id_ == tenant_id_
               && other_key.index_id_ == index_id_
               && other_key.tablet_id_ == tablet_id_
               && other_key.token_ == token_);
  }

  virtual uint64_t hash() const override
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_val);
    hash_val = common::murmurhash(&index_id_, sizeof(index_id_), hash_val);
    uint64_t tablet_id_val = tablet_id_.id();
    hash_val = common::murmurhash(&tablet_id_val, sizeof(tablet_id_val), hash_val);
    hash_val = common::murmurhash(token_.ptr(), token_.length(), hash_val);
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
    return sizeof(ObTokenDocCntCacheKey) + token_.length();
  }

  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const override
  {
    int ret = common::OB_SUCCESS;
    const int64_t deep_copy_size = size();
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < deep_copy_size)) {
      ret = common::OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument for token doc cnt cache key deep copy",
                 K(ret), K(buf_len), K(deep_copy_size));
    } else {
      // Copy the key structure first
      ObTokenDocCntCacheKey *new_key = new (buf) ObTokenDocCntCacheKey();
      new_key->tenant_id_ = tenant_id_;
      new_key->index_id_ = index_id_;
      new_key->tablet_id_ = tablet_id_;

      // Deep copy the token string after the key structure
      char *token_buf = buf + sizeof(ObTokenDocCntCacheKey);
      MEMCPY(token_buf, token_.ptr(), token_.length());
      new_key->token_.assign_ptr(token_buf, token_.length());

      key = new_key;
    }
    return ret;
  }

  TO_STRING_KV(K_(tenant_id), K_(index_id), K_(tablet_id), K_(token));

private:
  uint64_t tenant_id_;
  uint64_t index_id_;
  common::ObTabletID tablet_id_;
  common::ObString token_;
};

/**
 * Cache value for token document count cache.
 * Stores the estimated token document count and max token relevance.
 */
class ObTokenDocCntCacheValue : public common::ObIKVCacheValue
{
public:
  ObTokenDocCntCacheValue()
    : token_doc_cnt_(0),
      max_token_relevance_(0.0)
  {}

  ObTokenDocCntCacheValue(int64_t token_doc_cnt, double max_token_relevance)
    : token_doc_cnt_(token_doc_cnt),
      max_token_relevance_(max_token_relevance)
  {}

  virtual ~ObTokenDocCntCacheValue() {}

  virtual int64_t size() const override
  {
    return sizeof(ObTokenDocCntCacheValue);
  }

  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < size())) {
      ret = common::OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument for token doc cnt cache value deep copy",
                 K(ret), K(buf_len), K(size()));
    } else {
      ObTokenDocCntCacheValue *new_value = new (buf) ObTokenDocCntCacheValue(
          token_doc_cnt_, max_token_relevance_);
      value = new_value;
    }
    return ret;
  }

  int64_t get_token_doc_cnt() const { return token_doc_cnt_; }
  double get_max_token_relevance() const { return max_token_relevance_; }

  TO_STRING_KV(K_(token_doc_cnt), K_(max_token_relevance));

public:
  int64_t token_doc_cnt_;
  double max_token_relevance_;
};

/**
 * Global cache for token document count.
 * Uses LRU eviction policy to manage cache entries.
 */
class ObTokenDocCntCache : public common::ObKVCache<ObTokenDocCntCacheKey, ObTokenDocCntCacheValue>
{
public:
  static ObTokenDocCntCache &get_instance()
  {
    static ObTokenDocCntCache cache;
    return cache;
  }

  int get_token_doc_cnt(
      const ObTokenDocCntCacheKey &key,
      const ObTokenDocCntCacheValue *&value,
      common::ObKVCacheHandle &handle);

  int put_token_doc_cnt(
      const ObTokenDocCntCacheKey &key,
      const ObTokenDocCntCacheValue &value);

private:
  ObTokenDocCntCache() {}
  virtual ~ObTokenDocCntCache() {}
  DISALLOW_COPY_AND_ASSIGN(ObTokenDocCntCache);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_TOKEN_DOC_CNT_CACHE_H_
