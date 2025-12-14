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

  // Try to get existing pending list and append
  PendingPostingList *pending_list = pending_map_.get(pending_key);
  if (OB_NOT_NULL(pending_list)) {
    // Existing list, append entry
    if (OB_FAIL(pending_list->entries_.push_back(entry))) {
      LOG_WARN("failed to append entry to pending list", K(ret), K(entry));
    }
  } else {
    // Create new pending list
    PendingPostingList new_list;
    if (OB_FAIL(new_list.entries_.push_back(entry))) {
      LOG_WARN("failed to push entry to new list", K(ret), K(entry));
    } else if (OB_FAIL(pending_map_.set_refactored(pending_key, new_list))) {
      LOG_WARN("failed to insert new pending list", K(ret), K(pending_key));
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
  PendingPostingList pending_list;

  if (OB_FAIL(pending_map_.get_refactored(pending_key, pending_list))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get pending list", K(ret), K(pending_key));
    }
  } else if (pending_list.entries_.count() > 0) {
    // Move to KVCache
    ObTokenPostingListValue cache_value;
    if (OB_FAIL(cache_value.postentry_list_.assign(pending_list.entries_))) {
      LOG_WARN("failed to assign entries to cache value", K(ret));
    } else if (OB_FAIL(put_posting_list(key, cache_value))) {
      LOG_WARN("failed to put posting list to cache", K(ret), K(key));
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

  // Collect all keys first (we cannot modify map while iterating)
  ObArray<PendingCacheKey> keys_to_finalize;
  for (auto it = pending_map_.begin(); it != pending_map_.end(); ++it) {
    if (OB_FAIL(keys_to_finalize.push_back(it->first))) {
      LOG_WARN("failed to collect pending key", K(ret));
      break;
    }
  }

  // Finalize each pending list
  for (int64_t i = 0; OB_SUCC(ret) && i < keys_to_finalize.count(); ++i) {
    const PendingCacheKey &pending_key = keys_to_finalize.at(i);
    PendingPostingList pending_list;

    if (OB_FAIL(pending_map_.get_refactored(pending_key, pending_list))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // Already removed by another thread
        continue;
      }
      LOG_WARN("failed to get pending list", K(ret), K(pending_key));
    } else if (pending_list.entries_.count() > 0) {
      // Convert PendingCacheKey back to ObTokenPostingListCacheKey
      ObTokenPostingListCacheKey cache_key = pending_key.to_cache_key();

      // Move to KVCache
      ObTokenPostingListValue cache_value;
      if (OB_FAIL(cache_value.postentry_list_.assign(pending_list.entries_))) {
        LOG_WARN("failed to assign entries to cache value", K(ret));
      } else if (OB_FAIL(put_posting_list(cache_key, cache_value))) {
        LOG_WARN("failed to put posting list to cache", K(ret), K(cache_key));
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
