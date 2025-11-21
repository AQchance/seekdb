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

#include "ob_protected_memtable_mgr_handle.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

using namespace oceanbase::share;
using namespace oceanbase::memtable;

namespace oceanbase
{
namespace storage
{

int ObProtectedMemtableMgrHandle::create_tablet_memtable_mgr_(const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    lib::Worker::CompatMode compat_mode)
{
  int ret = OB_SUCCESS;
  ObTabletMemtableMgr *mgr = NULL;
  ObTabletMemtableMgrPool *pool = MTL(ObTabletMemtableMgrPool*);
  if (memtable_mgr_handle_.is_valid()) {
  } else if (OB_ISNULL(mgr = pool->acquire())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(mgr->ObIMemtableMgr::init(ls_id, tablet_id, compat_mode))) {
    pool->release(mgr);

  } else {
    memtable_mgr_handle_.set_memtable_mgr(mgr, pool);
  }
  return ret;
}

bool ObProtectedMemtableMgrHandle::need_reset_()
{
  bool ret = false;
  SpinRLockGuard guard(memtable_mgr_handle_lock_);
  ret = need_reset_without_lock_();
  return ret;
}

bool ObProtectedMemtableMgrHandle::need_reset_without_lock_()
{
  bool ret = false;
  ObIMemtableMgr *mgr = NULL;
  if (!memtable_mgr_handle_.is_valid()) {
  } else if (FALSE_IT(mgr = memtable_mgr_handle_.get_memtable_mgr())) {
  } else if (!mgr->has_memtable()) {
    ret = true;
  }
  return ret;
}
// only for non-inner tablet
int ObProtectedMemtableMgrHandle::try_reset_memtable_mgr_handle_()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(memtable_mgr_handle_lock_);
  if (need_reset_without_lock_()) {
    memtable_mgr_handle_.reset();
  }
  return ret;
}

int ObProtectedMemtableMgrHandle::reset()
{
  int ret = OB_SUCCESS;
  SpinWLockGuard guard(memtable_mgr_handle_lock_);
  if (memtable_mgr_handle_.is_valid()) {
    ObIMemtableMgr *mgr = memtable_mgr_handle_.get_memtable_mgr();
    if (OB_FAIL(mgr->has_memtable() && mgr->release_memtables())) {

    }
    memtable_mgr_handle_.reset();

  }
  return ret;
}

int ObProtectedMemtableMgrHandle::release_memtables_and_try_reset_memtable_mgr_handle(
    const ObTabletID &tablet_id,
    const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (scn.is_valid() && OB_FAIL(release_memtables(scn))) {

  } else if (!scn.is_valid() && OB_FAIL(release_memtables())) {

  } else if (tablet_id.is_ls_inner_tablet()) {
    // do nothing
  } else if (!need_reset_()) {
  } else if (OB_FAIL(try_reset_memtable_mgr_handle_())) {

  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
