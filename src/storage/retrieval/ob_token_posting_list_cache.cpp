#define USING_LOG_PREFIX STORAGE
#include "ob_token_posting_list_cache.h"

namespace oceanbase {
namespace storage {

int ObTokenPostingListCache::get_posting_list(
    const ObTokenPostingListCacheKey &key,
    const ObTokenPostingListValue *&value, common::ObKVCacheHandle &handle) {
  int ret = OB_SUCCESS;
  handle.reset();
  if (OB_FAIL(get(key, value, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get posting list from cache", K(ret), K(key));
    }
  }
  return ret;
}

int ObTokenPostingListCache::put_posting_list(
    const ObTokenPostingListCacheKey &key,
    const ObTokenPostingListValue &value) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(put(key, value))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("failed to put posting list to cache", K(ret), K(key), K(value));
    } else {
      // Entry already exists, ignore the error
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObTokenPostingListCache::append_to_pending(
    const ObTokenPostingListCacheKey &key, const PostingEntry &entry) {
  int ret = OB_SUCCESS;

  if (!pending_map_.created()) {
    if (OB_FAIL(
            pending_map_.create(1024, "PendingPostMap", "PendingPostNode"))) {
      LOG_WARN("failed to create pending map", K(ret));
      return ret;
    }
  }

  // Create wrapper key that owns the token string
  PendingCacheKey pending_key(key);

  // Safety check: log if pending_map is getting too large
  if (pending_map_.size() > 10000) {
    LOG_WARN("pending_map is very large, may cause memory issues",
             K(pending_map_.size()), K(pending_key));
  }

  // Try to get existing pending list and append
  PendingPostingList *pending_list = pending_map_.get(pending_key);
  if (OB_NOT_NULL(pending_list)) {
    // Existing list, append entry
    if (OB_FAIL(pending_list->entries_.push_back(entry))) {
      LOG_WARN("failed to append entry to pending list", K(ret), K(entry),
               K(pending_list->entries_.count()));
    }
  } else {
    // Create new pending list
    PendingPostingList new_list;
    if (OB_FAIL(new_list.entries_.push_back(entry))) {
      LOG_WARN("failed to push entry to new list", K(ret), K(entry));
    } else if (OB_FAIL(pending_map_.set_refactored(pending_key, new_list))) {
      LOG_WARN("failed to insert new pending list", K(ret), K(pending_key),
               K(pending_map_.size()));
    }
  }

  return ret;
}

const PendingPostingList *ObTokenPostingListCache::get_pending(
    const ObTokenPostingListCacheKey &key) const {
  PendingCacheKey pending_key(key);
  return pending_map_.get(pending_key);
}

int ObTokenPostingListCache::finalize_to_cache(
    const ObTokenPostingListCacheKey &key) {
  int ret = OB_SUCCESS;

  PendingCacheKey pending_key(key);

  // Get pointer to the pending list in the map (avoid copy)
  PendingPostingList *pending_list_ptr = pending_map_.get(pending_key);
  if (OB_ISNULL(pending_list_ptr)) {
    ret = OB_HASH_NOT_EXIST;
  } else if (pending_list_ptr->entries_.count() > 0) {
    // Use heap allocation instead of alloca to avoid stack overflow
    const int64_t entry_count = pending_list_ptr->entries_.count();
    const int64_t value_size = ObTokenPostingListValue::calc_size(entry_count);

    // Allocate from heap with proper alignment
    char *buf = static_cast<char *>(ob_malloc(value_size, "PostListTmp"));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate temp buffer", K(ret), K(value_size));
    } else {
      ObTokenPostingListValue *cache_value = nullptr;

      if (OB_FAIL(ObTokenPostingListValue::create_temp_value(
              pending_list_ptr->entries_, buf, value_size, cache_value))) {
        LOG_WARN("failed to create temp cache value", K(ret));
      } else if (OB_FAIL(put_posting_list(key, *cache_value))) {
        LOG_WARN("failed to put posting list to cache", K(ret), K(key));
      }

      // Free the temp buffer
      ob_free(buf);
    }

    // Remove from pending map regardless of cache success
    int erase_ret = pending_map_.erase_refactored(pending_key);
    if (OB_SUCCESS != erase_ret && OB_HASH_NOT_EXIST != erase_ret) {
      LOG_WARN("failed to erase pending list", K(erase_ret), K(pending_key));
    }
  }

  return ret;
}

int ObTokenPostingListCache::clear_pending(
    const ObTokenPostingListCacheKey &key) {
  int ret = OB_SUCCESS;
  PendingCacheKey pending_key(key);
  if (OB_FAIL(pending_map_.erase_refactored(pending_key))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to clear pending list", K(ret), K(pending_key));
    } else {
      ret = OB_SUCCESS; // Not an error if not found
    }
  }
  return ret;
}

int ObTokenPostingListCache::finalize_all_pending() {
  int ret = OB_SUCCESS;

  if (!pending_map_.created() || pending_map_.size() == 0) {
    return ret;
  }

  const int64_t total_pending = pending_map_.size();
  LOG_INFO("finalize_all_pending started", K(total_pending));

  // Collect all keys first (we cannot modify map while iterating)
  ObArray<PendingCacheKey> keys_to_finalize;
  for (auto it = pending_map_.begin(); it != pending_map_.end(); ++it) {
    if (OB_FAIL(keys_to_finalize.push_back(it->first))) {
      LOG_WARN("failed to collect pending key", K(ret));
      break;
    }
  }

  LOG_INFO("collected keys to finalize", K(keys_to_finalize.count()));

  // Finalize each pending list
  for (int64_t i = 0; OB_SUCC(ret) && i < keys_to_finalize.count(); ++i) {
    const PendingCacheKey &pending_key = keys_to_finalize.at(i);

    // Get pointer to the pending list in the map (avoid copy)
    PendingPostingList *pending_list_ptr = pending_map_.get(pending_key);
    if (OB_ISNULL(pending_list_ptr)) {
      continue; // Already removed by another thread
    }

    if (pending_list_ptr->entries_.count() > 0) {
      // Convert PendingCacheKey back to ObTokenPostingListCacheKey
      // IMPORTANT: Use the key directly since put_posting_list will deep_copy
      // it
      ObTokenPostingListCacheKey cache_key = pending_key.to_cache_key();

      // Use heap allocation instead of alloca
      const int64_t entry_count = pending_list_ptr->entries_.count();
      const int64_t value_size =
          ObTokenPostingListValue::calc_size(entry_count);

      // Debug log for tracking which key is being processed
      if (i % 100 == 0 || entry_count > 1000) {
        LOG_INFO("finalizing key", K(i), K(keys_to_finalize.count()),
                 K(entry_count), K(value_size), K(pending_key));
      }

      char *buf = static_cast<char *>(ob_malloc(value_size, "PostListTmp"));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("CRITICAL: failed to allocate temp buffer", K(ret),
                  K(value_size), K(i), K(entry_count));
      } else {
        ObTokenPostingListValue *cache_value = nullptr;

        if (OB_FAIL(ObTokenPostingListValue::create_temp_value(
                pending_list_ptr->entries_, buf, value_size, cache_value))) {
          LOG_WARN("failed to create temp cache value", K(ret));
        } else if (OB_FAIL(put_posting_list(cache_key, *cache_value))) {
          LOG_WARN("failed to put posting list to cache", K(ret), K(cache_key));
        }

        ob_free(buf);
      }
    }

    // Remove from pending map regardless of cache success
    int erase_ret = pending_map_.erase_refactored(pending_key);
    if (OB_SUCCESS != erase_ret && OB_HASH_NOT_EXIST != erase_ret) {
      LOG_WARN("failed to erase pending list", K(erase_ret), K(pending_key));
    }
  }

  return ret;
}

int ObTokenPostingListCache::clear_all_pending() {
  int ret = OB_SUCCESS;
  if (pending_map_.created()) {
    ret = pending_map_.clear();
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
