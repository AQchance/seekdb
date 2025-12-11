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

#define USING_LOG_PREFIX STORAGE

#include "storage/retrieval/ob_token_doc_cnt_cache.h"

namespace oceanbase
{
namespace storage
{

int ObTokenDocCntCache::get_token_doc_cnt(
    const ObTokenDocCntCacheKey &key,
    const ObTokenDocCntCacheValue *&value,
    common::ObKVCacheHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  if (OB_FAIL(get(key, value, handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get token doc cnt from cache", K(ret), K(key));
    }
  }
  return ret;
}

int ObTokenDocCntCache::put_token_doc_cnt(
    const ObTokenDocCntCacheKey &key,
    const ObTokenDocCntCacheValue &value)
{
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

} // namespace storage
} // namespace oceanbase
