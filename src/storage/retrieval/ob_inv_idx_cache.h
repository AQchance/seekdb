
#ifndef OB_INV_IDX_CACHE_H_
#define OB_INV_IDX_CACHE_H_

#define USING_LOG_PREFIX STORAGE

#include "sql/das/ob_das_ir_define.h"
#include "lib/container/ob_array.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include <string>
#include <unordered_map>
#include <memory>

namespace oceanbase {
namespace storage {

// 倒排索引缓存条目
struct ObInvIdxCacheEntry {
  sql::ObDocIdExt doc_id;
  uint64_t doc_length;

  ObInvIdxCacheEntry() : doc_id(), doc_length(0) {}
  ObInvIdxCacheEntry(const sql::ObDocIdExt &id, uint64_t len)
    : doc_id(id), doc_length(len) {}

  // 拷贝构造函数
  ObInvIdxCacheEntry(const ObInvIdxCacheEntry &other)
    : doc_id(other.doc_id), doc_length(other.doc_length) {}

  // 赋值运算符
  ObInvIdxCacheEntry& operator=(const ObInvIdxCacheEntry &other) {
    if (this != &other) {
      doc_id = other.doc_id;
      doc_length = other.doc_length;
    }
    return *this;
  }

  // ObArray 需要 to_string 方法
  TO_STRING_KV(K(doc_id), K(doc_length));
};

// Token 缓存数据
struct ObTokenCacheData {
  common::ObArray<ObInvIdxCacheEntry> entries;  // 使用 ObArray 替代 std::vector
  bool is_complete;       // 标记缓存是否完整（所有倒排索引数据都已读取）
  int64_t next_read_idx;  // 下一次从缓存读取的起始位置

  ObTokenCacheData() : entries(), is_complete(false), next_read_idx(0) {}

  void reset() {
    entries.reset();
    is_complete = false;
    next_read_idx = 0;
  }
};

class ObIvtIdxCache {
public:
  // 获取单例实例
  static ObIvtIdxCache &get_instance() {
    static ObIvtIdxCache instance;
    return instance;
  }

  // 禁止拷贝和赋值
  ObIvtIdxCache(const ObIvtIdxCache&) = delete;
  ObIvtIdxCache& operator=(const ObIvtIdxCache&) = delete;

  void reset() {
    cache_map_.clear();
  }

  // 检查 token 是否在缓存中且缓存完整
  bool is_token_cached_complete(const ObString &token) const {
    std::string key(token.ptr(), token.length());
    auto it = cache_map_.find(key);
    return it != cache_map_.end() && it->second && it->second->is_complete;
  }

  // 获取 token 对应的缓存数据（如果存在且完整）
  ObTokenCacheData* get_token_cache(const ObString &token) {
    std::string key(token.ptr(), token.length());
    auto it = cache_map_.find(key);
    if (it != cache_map_.end() && it->second && it->second->is_complete) {
      return it->second.get();
    }
    return nullptr;
  }

  // 获取或创建 token 对应的缓存数据（用于写入）
  ObTokenCacheData& get_or_create_token_cache(const ObString &token) {
    std::string key(token.ptr(), token.length());
    auto& ptr = cache_map_[key];
    if (!ptr) {
      ptr = std::make_unique<ObTokenCacheData>();
    }
    return *ptr;
  }

  // 追加数据到缓存
  int append_to_cache(const ObString &token,
                      const ObInvIdxCacheEntry &entry) {
    int ret = OB_SUCCESS;
    std::string key(token.ptr(), token.length());
    auto& ptr = cache_map_[key];
    if (!ptr) {
      ptr = std::make_unique<ObTokenCacheData>();
    }
    if (OB_FAIL(ptr->entries.push_back(entry))) {
      LOG_WARN("failed to push back entry to cache", K(ret));
    }
    return ret;
  }

  // 标记 token 缓存为完整
  void mark_cache_complete(const ObString &token) {
    std::string key(token.ptr(), token.length());
    auto it = cache_map_.find(key);
    if (it != cache_map_.end() && it->second) {
      it->second->is_complete = true;
    }
  }

  // 重置 token 的读取位置
  void reset_read_position(const ObString &token) {
    std::string key(token.ptr(), token.length());
    auto it = cache_map_.find(key);
    if (it != cache_map_.end() && it->second) {
      it->second->next_read_idx = 0;
    }
  }

  // 清除未完成的缓存（当需要重新开始读取时）
  void clear_incomplete_cache(const ObString &token) {
    std::string key(token.ptr(), token.length());
    auto it = cache_map_.find(key);
    if (it != cache_map_.end() && it->second && !it->second->is_complete) {
      cache_map_.erase(it);
    }
  }

private:
  // 私有构造函数
  ObIvtIdxCache() {}
  ~ObIvtIdxCache() = default;

  // 使用 unique_ptr 避免 unordered_map 扩容时复制 ObTokenCacheData
  std::unordered_map<std::string, std::unique_ptr<ObTokenCacheData>> cache_map_;
};

// class ObTextIRTokenCacheKey : public common::ObIKVCacheKey {
// public:
//   ObTextIRTokenCacheKey()
//       : /* index_id_(0), */ token_() {}
//   ObTextIRTokenCacheKey(
//       /* const uint64_t index_id, */ const ObString &token)
//       : /* index_id_(index_id), */
//         token_(token) {}
//   ~ObTextIRTokenCacheKey() override = default;
//
//   uint64_t hash() const override {
//     uint64_t hash_val = 0;
//     // hash_val = murmurhash(&index_id_, sizeof(index_id_), hash_val);
//     hash_val = murmurhash(token_.ptr(), token_.length(), hash_val);
//     return hash_val;
//   }
//
//   bool operator==(const ObIKVCacheKey &other) const override {
//     const ObTextIRTokenCacheKey &other_key =
//         reinterpret_cast<const ObTextIRTokenCacheKey &>(other);
//     return // index_id_ == other_key.index_id_ &&
//         token_ == other_key.token_;
//   }
//
//   int equal(const ObIKVCacheKey &other, bool &is_equal) const override {
//     is_equal = *this == other;
//     return OB_SUCCESS;
//   }
//
//   int hash(uint64_t &hash_val) const override {
//     hash_val = hash();
//     return OB_SUCCESS;
//   }
//
//   int64_t size() const override { return sizeof(*this) + token_.length(); }
//
//   int deep_copy(char *buf, const int64_t buf_len,
//                 ObIKVCacheKey *&key) const override {
//     int ret = OB_SUCCESS;
//     if (OB_ISNULL(buf) || buf_len < size()) {
//       ret = OB_INVALID_ARGUMENT;
//       LOG_WARN("invalid argument for text token cache key", K(ret), K(buf_len),
//                K(size()));
//     } else {
//       ObTextIRTokenCacheKey *new_key = new (buf) ObTextIRTokenCacheKey();
//       // new_key->index_id_ = index_id_;
//       char *str_buf = buf + sizeof(ObTextIRTokenCacheKey);
//       MEMCPY(str_buf, token_.ptr(), token_.length());
//       new_key->token_.assign_ptr(str_buf,
//                                  static_cast<int32_t>(token_.length()));
//       key = new_key;
//     }
//     return ret;
//   }
//
// private:
//   // uint64_t index_id_;
//   ObString token_;
// };
//
// class ObTextIRTokenCacheValue : public common::ObIKVCacheValue {
// public:
//   ObTextIRTokenCacheValue() : row_cnt_(0), col_cnt_(0), data_size_(0) {}
//   ObTextIRTokenCacheValue(const int64_t row_cnt, const int64_t col_cnt,
//                           const int64_t data_size)
//       : row_cnt_(row_cnt), col_cnt_(col_cnt), data_size_(data_size) {}
//   ~ObTextIRTokenCacheValue() override = default;
//
//   int64_t size() const override {
//     return sizeof(*this) + sizeof(int64_t) * row_cnt_ + data_size_;
//   }
//
//   const int64_t *offsets() const {
//     return reinterpret_cast<const int64_t *>(this + 1);
//   }
//
//   char *data_base() {
//     return reinterpret_cast<char *>(const_cast<int64_t *>(offsets()) +
//                                     row_cnt_);
//   }
//
//   const sql::ObChunkDatumStore::StoredRow *get_row(const int64_t idx) const {
//     const char *base = reinterpret_cast<const char *>(offsets() + row_cnt_);
//     return reinterpret_cast<const sql::ObChunkDatumStore::StoredRow *>(
//         base + offsets()[idx]);
//   }
//
//   int deep_copy(char *buf, const int64_t buf_len,
//                 ObIKVCacheValue *&value) const override {
//     int ret = OB_SUCCESS;
//     if (OB_ISNULL(buf) || buf_len < size()) {
//       ret = OB_INVALID_ARGUMENT;
//       LOG_WARN("invalid argument for text token cache value", K(ret),
//                K(buf_len), K(size()));
//     } else {
//       ObTextIRTokenCacheValue *new_val =
//           new (buf) ObTextIRTokenCacheValue(row_cnt_, col_cnt_, data_size_);
//       int64_t *dst_offsets =
//           reinterpret_cast<int64_t *>(buf + sizeof(ObTextIRTokenCacheValue));
//       char *dst_base = reinterpret_cast<char *>(dst_offsets + row_cnt_);
//       const int64_t *src_offsets = offsets();
//       const char *src_base =
//           reinterpret_cast<const char *>(src_offsets + row_cnt_);
//       for (int64_t i = 0; i < row_cnt_ && OB_SUCC(ret); ++i) {
//         dst_offsets[i] = src_offsets[i];
//         sql::ObChunkDatumStore::StoredRow *dst_row =
//             new (dst_base + dst_offsets[i]) sql::ObChunkDatumStore::StoredRow();
//         const sql::ObChunkDatumStore::StoredRow *src_row =
//             reinterpret_cast<const sql::ObChunkDatumStore::StoredRow *>(
//                 src_base + src_offsets[i]);
//         if (OB_FAIL(dst_row->assign(src_row))) {
//           LOG_WARN("failed to copy stored row for cache value", K(ret));
//         }
//       }
//       if (OB_SUCC(ret)) {
//         value = new_val;
//       }
//     }
//     return ret;
//   }
//
//   struct TokenPostingCacheEntry {
//     TokenPostingCacheEntry() : token_(), value_(nullptr), next_row_idx_(0) {}
//     ObString token_;
//     ObTextIRTokenCacheValue *value_;
//     int64_t next_row_idx_;
//   };
//
//   int64_t row_cnt_;
//   int64_t col_cnt_;
//   int64_t data_size_;
//   common::ObSEArray<TokenPostingCacheEntry, 4> token_posting_cache_entries_;
// };
//
// class ObTextIRTokenKVCache
//     : public common::ObKVCache<ObTextIRTokenCacheKey, ObTextIRTokenCacheValue> {
// public:
//   static ObTextIRTokenKVCache &get_instance() {
//     static ObTextIRTokenKVCache cache;
//     return cache;
//   }
// };
} // namespace storage
} // namespace oceanbase

#endif // OB_INV_IDX_CACHE_H_
