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

#include "storage/tx_storage/ob_empty_shell_task.h"
#include "lib/literals/ob_literals.h"    // ObLSIterator
#include "storage/tx_storage/ob_ls_service.h" // ObLSService
#include "storage/tablet/ob_tablet_iterator.h"
#include "rootserver/ob_tenant_info_loader.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"

namespace oceanbase
{
using namespace share;
namespace storage
{
namespace checkpoint
{

// The time interval for checking deleted tablet trigger is 5s
const int64_t ObEmptyShellTask::GC_EMPTY_TABLET_SHELL_INTERVAL = 5 * 1000 * 1000L;

// The time interval for tablet become empty shell is 24 * 720 * 5s = 1d
const int64_t ObEmptyShellTask::GLOBAL_EMPTY_CHECK_INTERVAL_TIMES = 24 * 720;

void ObEmptyShellTask::runTimerTask()
{

  int ret = OB_SUCCESS;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*);
  bool skip_empty_shell_task = false;
  RLOCAL_STATIC(int64_t, times) = 0;
  times = (times + 1) % GLOBAL_EMPTY_CHECK_INTERVAL_TIMES;

  skip_empty_shell_task = (OB_SUCCESS != (OB_E(EventTable::EN_TABLET_EMPTY_SHELL_TASK_FAILED) OB_SUCCESS));

  if (!SERVER_STORAGE_META_SERVICE.is_started()) {
    // do nothing

  } else if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_UNLIKELY(skip_empty_shell_task)) {
    // do nothing
  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {

  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;

  } else {
    ObLS *ls = NULL;
    int ls_cnt = 0;
    for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      // one ls failed to handle empty shell task should not bother another ls
      if (OB_ITER_END == ret) {
        break;
      } else {
        ObTabletEmptyShellHandler *tablet_empty_shell_handler = NULL;
        if (OB_ISNULL(ls)) {
          ret = OB_ERR_UNEXPECTED;

        } else if (FALSE_IT(tablet_empty_shell_handler = ls->get_tablet_empty_shell_handler())) {
        } else if (tablet_empty_shell_handler->check_stop()) {

        } else if (0 == times || tablet_empty_shell_handler->get_empty_shell_trigger()) {

          tablet_empty_shell_handler->set_empty_shell_trigger(false);
          obsys::ObRLockGuard lock(tablet_empty_shell_handler->wait_lock_);
          bool need_retry = false;
          common::ObTabletIDArray empty_shell_tablet_ids;
          if (OB_FAIL(tablet_empty_shell_handler->get_empty_shell_tablet_ids(empty_shell_tablet_ids, need_retry))) {
            need_retry = true;

          } else if (empty_shell_tablet_ids.empty()) {
            // do nothing
          } else if (OB_FAIL(tablet_empty_shell_handler->update_tablets_to_empty_shell(ls, empty_shell_tablet_ids))) {
            need_retry = true;

          }
          if (need_retry) {

            if (!tablet_empty_shell_handler->get_empty_shell_trigger()) {
              tablet_empty_shell_handler->set_empty_shell_trigger(true);
            }
          }
        }
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (ls_cnt > 0) {

      } else {

      }
    }
  }
}


ObTabletEmptyShellHandler::ObTabletEmptyShellHandler()
  : ls_(NULL),
    is_trigger_(true),
    stopped_(false),
    ddl_empty_shell_checker_(),
    is_inited_(false)
{
}

ObTabletEmptyShellHandler::~ObTabletEmptyShellHandler()
{
  reset();
}

void ObTabletEmptyShellHandler::reset()
{
  ls_ = NULL;
  is_trigger_ = true;
  stopped_ = false;
  ddl_empty_shell_checker_.reset();
  is_inited_ = false;
}

int ObTabletEmptyShellHandler::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;

  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_FAIL(ddl_empty_shell_checker_.init(ls))) {

  } else {
    ls_ = ls;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletEmptyShellHandler::get_empty_shell_tablet_ids(common::ObTabletIDArray &empty_shell_tablet_ids, bool &need_retry)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObLSTabletIterator tablet_iter(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;

  } else if (OB_ISNULL(t3m)) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_FAIL(ls_->get_tablet_svr()->build_tablet_iter(tablet_iter))) {

  } else {
    const uint64_t tenant_id = MTL_ID();
    ObTabletHandle tablet_handle;
    ObTablet *tablet = NULL;
    bool can_become_shell = false;
    bool is_locked = false;
    while (OB_SUCC(ret)) {
      if (check_stop()) {
        ret = OB_EAGAIN;

      } else if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {

        }
      } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;

      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
          ret = OB_ERR_UNEXPECTED;

      } else if (tablet->is_ls_inner_tablet()) {
        // skip ls inner tablet
      } else if (OB_FAIL(tablet->is_locked_by_others<ObTabletCreateDeleteMdsUserData>(is_locked))) {

      } else if (is_locked) {

        need_retry = true;
      } else if (OB_FAIL(check_candidate_tablet_(*tablet, can_become_shell, need_retry))) {

      } else if (!can_become_shell) {

      } else if (OB_FAIL(empty_shell_tablet_ids.push_back(tablet->get_tablet_meta().tablet_id_))) {

      }
    }
  }

  return ret;
}

int ObTabletEmptyShellHandler::update_tablets_to_empty_shell(ObLS *ls, const common::ObIArray<common::ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
    const ObTabletID &tablet_id = tablet_ids.at(i);
    if (OB_FAIL(ls->get_tablet_svr()->update_tablet_to_empty_shell(tablet_id))) {

    } else if (OB_FAIL(ddl_empty_shell_checker_.erase_tablet_record(tablet_id))) {

    } else {
    #ifdef ERRSIM
      const uint64_t tenant_id = MTL_ID();
      SERVER_EVENT_ADD("gc", "turn_into_empty_shell", "tenant_id", tenant_id, "ls_id", ls->get_ls_id(), "tablet_id", tablet_id);
    #endif
    }
  }

  return ret;
}

int ObTabletEmptyShellHandler::check_candidate_tablet_(const ObTablet &tablet, bool &can_become_shell, bool &need_retry)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = MTL_ID();
  const share::ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  can_become_shell = false;
  bool is_written = false;

  if (tablet.is_empty_shell()) {
    // do nothing
  } else if (OB_FAIL(tablet.check_tablet_status_written(is_written))) {

  } else {
    ObTabletCreateDeleteMdsUserData data;
    mds::MdsWriter writer; // will be removed later
    mds::TwoPhaseCommitState trans_stat; // will be removed later
    share::SCN trans_version; // will be removed later

    if (OB_FAIL(tablet.get_latest(data, writer, trans_stat, trans_version))) {
      if (OB_EMPTY_RESULT == ret) {
        ret = OB_SUCCESS;
        STORAGE_LOG(INFO, "tablet status is null, may be create tx is aborted or create user data has not been written",
            K(ret), K(ls_id), K(tablet_id));

        if (!is_written) {
          // mds table has not been written, do nothing
        } else if (!is_user_tenant(tenant_id)) {
          can_become_shell = true;

        } else if (OB_FAIL(check_tablet_from_aborted_tx_(tablet, can_become_shell, need_retry))) {

        }
      } else {

      }
    } else if (mds::TwoPhaseCommitState::ON_COMMIT == trans_stat && data.tablet_status_.is_deleted_for_gc()) {


      if (!is_user_tenant(tenant_id)) {
        can_become_shell = true;

      } else if (OB_FAIL(check_tablet_from_deleted_tx_(tablet, data, can_become_shell, need_retry))) {

      }
    }
  }

  return ret;
}

int ObTabletEmptyShellHandler::check_tablet_from_aborted_tx_(const ObTablet &tablet, bool &can_become_shell, bool &need_retry)
{
  int ret = OB_SUCCESS;
  can_become_shell = false;
  const share::ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  share::SCN rec_scn;
  share::SCN readable_scn(SCN::base_scn());

  if (OB_FAIL(tablet.get_mds_table_rec_scn(rec_scn))) {

  } else if (rec_scn.is_max()) {
    can_become_shell = false;
    need_retry = false;
    STORAGE_LOG(INFO, "mds table rec scn is MAX, redo log has NOT been written, should delete tablet instantly",
        K(ret), K(ls_id), K(tablet_id), K(rec_scn));
  } else if (OB_FAIL(get_readable_scn(readable_scn))) {

  } else if (rec_scn < readable_scn) {
    can_become_shell = true;
    STORAGE_LOG(INFO, "readable scn is bigger than rec scn, tablet should be converted info empty shell",
        K(ret), K(ls_id), K(tablet_id), K(rec_scn), K(readable_scn));
  } else {
    need_retry = true;
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      STORAGE_LOG(INFO, "readable scn is smaller than rec scn, shoudle retry",
          K(ret), K(ls_id), K(tablet_id), K(rec_scn), K(readable_scn));
    }
  }

  return ret;
}

int ObTabletEmptyShellHandler::check_tablet_from_deleted_tx_(
    const ObTablet &tablet,
    const ObTabletCreateDeleteMdsUserData &user_data,
    bool &can_become_shell,
    bool &need_retry)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  can_become_shell = false;
  bool not_depend_on = false;
  ObTabletStatus::Status tablet_status = user_data.get_tablet_status();
  share::SCN readable_scn = SCN::base_scn();
  const uint64_t tenant_id = MTL_ID();

  if (OB_UNLIKELY(!user_data.is_valid())) {
    ret = OB_INVALID_ARGUMENT;

  } else if (!is_user_tenant(tenant_id)) {
    can_become_shell = true;

  } else if (OB_FAIL(get_readable_scn(readable_scn))) {

  } else if (ObTabletStatus::TRANSFER_OUT_DELETED == tablet_status
      && OB_FAIL(check_transfer_out_deleted_tablet_(tablet, user_data, not_depend_on, need_retry))) {

  } else if (!not_depend_on && OB_FAIL(ddl_empty_shell_checker_.check_split_src_deleted_tablet(tablet, user_data, not_depend_on, need_retry))) {

  } else if (ObTabletStatus::DELETED == tablet_status || not_depend_on) {
    if (user_data.delete_commit_scn_.is_valid() && user_data.delete_commit_scn_ <= readable_scn) {
      can_become_shell = true;

    } else {
      need_retry = true;
      if (REACH_THREAD_TIME_INTERVAL(1_s)) {

      }
    }
  }

  return ret;
}

int ObTabletEmptyShellHandler::check_transfer_out_deleted_tablet_(
    const ObTablet &tablet,
    const ObTabletCreateDeleteMdsUserData &user_data,
    bool &can,
    bool &need_retry)
{
  int ret = OB_SUCCESS;
  can = false;
  SCN decided_scn;
  ObMigrationStatus migration_status;
  ObLSService *ls_service = NULL;
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  const ObTabletID tablet_id(tablet.get_tablet_meta().tablet_id_);
  if (!user_data.is_valid() || !user_data.transfer_ls_id_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_FAIL(ls_service->get_ls(user_data.transfer_ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
    if (OB_LS_NOT_EXIST == ret) {
      can = true;

      ret = OB_SUCCESS;
    } else {

    }
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_FAIL(ls->get_migration_status(migration_status))) {

  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_ADD == migration_status
      || ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE == migration_status
      || ObMigrationStatus::OB_MIGRATION_STATUS_REBUILD == migration_status) {
    can = true;
    STORAGE_LOG(INFO, "migration_status is OB_MIGRATION_STATUS_MIGRATE", K(ret),
        "ls_id", ls_->get_ls_id(), K(tablet_id), K(can), K(user_data), K(migration_status));
  } else if (tablet.get_tablet_meta().ha_status_.is_expected_status_deleted()) {
    can = true;

  } else if (OB_FAIL(ls->get_max_decided_scn(decided_scn))) {

  } else if (decided_scn >= user_data.delete_commit_scn_) {
    can = true;
    STORAGE_LOG(INFO, "decided_scn is bigger than transfer finish scn", K(ret),
      "ls_id", ls_->get_ls_id(), K(tablet_id), K(can), K(user_data), K(decided_scn));
  } else {
    need_retry = true;
    if (REACH_THREAD_TIME_INTERVAL(1_s)) {
      STORAGE_LOG(INFO, "decided_scn is smaller than tablet delete commit scn",
        "ls_id", ls_->get_ls_id(), K(tablet_id), K(user_data), K(decided_scn));
    }
  }
  return ret;
}

int ObTabletEmptyShellHandler::get_readable_scn(share::SCN &readable_scn)
{
  int ret = OB_SUCCESS;
  rootserver::ObTenantInfoLoader *info_loader = MTL(rootserver::ObTenantInfoLoader*);

  if (OB_FAIL(info_loader->get_readable_scn(readable_scn))) {

  } else if (OB_UNLIKELY(!readable_scn.is_valid())) {
    ret = OB_ERR_UNEXPECTED;

  }

  return ret;
}

bool ObTabletEmptyShellHandler::get_empty_shell_trigger() const
{
  return ATOMIC_LOAD(&is_trigger_);
}

void ObTabletEmptyShellHandler::set_empty_shell_trigger(bool is_trigger)
{
  ATOMIC_STORE(&is_trigger_, is_trigger);

}

int ObTabletEmptyShellHandler::offline()
{
  int ret = OB_SUCCESS;
  set_stop();
  if (!is_finish()) {
    ret = OB_EAGAIN;

  } else {

  }
  return ret;
}

void ObTabletEmptyShellHandler::online()
{
  set_empty_shell_trigger(true);
  set_start();

}

} // checkpoint
} // storage
} // oceanbase
