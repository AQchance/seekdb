#define USING_LOG_PREFIX STORAGE
#include "ob_token_posting_list_cache.h"

namespace oceanbase
{
namespace storage
{
int oceanbase::storage::ObTokenPostingListCache::get_posting_list(
    const ObTokenPostingListCacheKey &key,
    const ObTokenPostingListValue *&value,
    common::ObKVCacheHandle &handle) {
  int ret = OB_SUCCESS;
  handle.reset();
  if (OB_FAIL(get(key, value, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get token doc cnt from cache", K(ret), K(key));
    }
  }
  return ret;
}

int oceanbase::storage::ObTokenPostingListCache::put_posting_list(
    const ObTokenPostingListCacheKey &key,
    const ObTokenPostingListValue &value) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(put(key, value))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("failed to put token doc cnt to cache", K(ret), K(key), K(value));
    } else {
      // Entry already exists, ignore the error
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int oceanbase::storage::ObTokenPostingListCache::insert_posting_entry(
    const ObTokenPostingListCacheKey &key,
    const PostingEntry &posting_entry) {
  int ret = OB_SUCCESS;
  oceanbase::storage::ObTokenPostingListValue *value = nullptr;
  common::ObKVCacheHandle handle;
  if (OB_FAIL(non_const_get(key, value, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get token posting list from cache", K(ret), K(key));
    } else {
      // Entry does not exist, create a new one
      ObTokenPostingListValue new_value;
      if (OB_FAIL(new_value.postentry_list_.push_back(posting_entry))) {
        LOG_WARN("failed to push back posting entry", K(ret), K(posting_entry));
      } else if (OB_FAIL(put_posting_list(key, new_value))) {
        LOG_WARN("failed to put token posting list to cache", K(ret), K(key), K(new_value));
      }
    }
  } else {
    // Entry exists, update it
    if (OB_FAIL(value->postentry_list_.push_back(posting_entry))) {
      LOG_WARN("failed to push back posting entry", K(ret), K(posting_entry));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
