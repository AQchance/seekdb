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

#include "storage/tx_storage/ob_checkpoint_service.h"
#include "logservice/ob_log_service.h"

namespace oceanbase
{
using namespace share;
using namespace palf;
namespace storage
{
namespace checkpoint
{

int64_t ObCheckPointService::CHECK_CLOG_USAGE_INTERVAL = 2000 * 1000L;
int64_t ObCheckPointService::CHECKPOINT_INTERVAL = 5000 * 1000L;
int64_t ObCheckPointService::TRAVERSAL_FLUSH_INTERVAL = 5000 * 1000L;

int ObCheckPointService::mtl_init(ObCheckPointService* &m)
{
  return m->init(MTL_ID());
}

int ObCheckPointService::init(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCheckPointService init twice.", K(ret));
  } else if (OB_FAIL(freeze_thread_.init(tenant_id, lib::TGDefIDs::LSFreeze))) {
    LOG_WARN("fail to initialize freeze thread", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObCheckPointService::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(checkpoint_timer_.set_run_wrapper_with_ret(MTL_CTX()))) {

  } else if (OB_FAIL(checkpoint_timer_.init("TxCkpt", ObMemAttr(MTL_ID(), "CheckPointTimer")))) {

  } else if (OB_FAIL(checkpoint_timer_.schedule(checkpoint_task_, CHECKPOINT_INTERVAL, true))) {

  } else if (OB_FAIL(traversal_flush_timer_.set_run_wrapper_with_ret(MTL_CTX()))) {

  } else if (OB_FAIL(traversal_flush_timer_.init("Flush", ObMemAttr(MTL_ID(), "FlushTimer")))) {

  } else if (OB_FAIL(traversal_flush_timer_.schedule(traversal_flush_task_, TRAVERSAL_FLUSH_INTERVAL, true))) {

  } else if (OB_FAIL(check_clog_disk_usage_timer_.set_run_wrapper_with_ret(MTL_CTX()))) {

  } else if (OB_FAIL(check_clog_disk_usage_timer_.init("CKClogDisk", ObMemAttr(MTL_ID(), "DiskUsageTimer")))) {

  } else if (OB_FAIL(check_clog_disk_usage_timer_.schedule(check_clog_disk_usage_task_, CHECK_CLOG_USAGE_INTERVAL, true))) {

  }
  return ret;
}

int ObCheckPointService::stop()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCheckPointService is not initialized", K(ret));
  } else {
    TG_STOP(freeze_thread_.get_tg_id());

  }
  checkpoint_timer_.stop();
  traversal_flush_timer_.stop();
  check_clog_disk_usage_timer_.stop();
  return ret;
}

void ObCheckPointService::wait()
{
  checkpoint_timer_.wait();
  traversal_flush_timer_.wait();
  check_clog_disk_usage_timer_.wait();
  TG_WAIT(freeze_thread_.get_tg_id());
}

int ObCheckPointService::add_ls_freeze_task(
    ObDataCheckpoint *data_checkpoint,
    SCN rec_scn)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(freeze_thread_.add_task(data_checkpoint, rec_scn))) {

  }
  return ret;
}

void ObCheckPointService::destroy()
{
  TG_DESTROY(freeze_thread_.get_tg_id());
  is_inited_ = false;
  checkpoint_timer_.destroy();
  traversal_flush_timer_.destroy();
  check_clog_disk_usage_timer_.destroy();
}

void ObCheckPointService::ObCheckpointTask::runTimerTask()
{

  int ret = OB_SUCCESS;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*);
  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {

  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;

  } else {
    DEBUG_SYNC(BEFORE_CHECKPOINT_TASK);
    ObLS *ls = nullptr;
    int ls_cnt = 0;
    for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      ObLSHandle ls_handle;
      ObCheckpointExecutor *checkpoint_executor = nullptr;
      ObDataCheckpoint *data_checkpoint = nullptr;
      palf::LSN checkpoint_lsn;
      if (OB_FAIL(ls_svr->get_ls(ls->get_ls_id(), ls_handle, ObLSGetMod::APPLY_MOD))) {

      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;

      } else if (OB_ISNULL(data_checkpoint = ls->get_data_checkpoint())) {
        ret = OB_ERR_UNEXPECTED;

      } else if (OB_FAIL(data_checkpoint->check_can_move_to_active_in_newcreate())) {

      } else if (OB_ISNULL(checkpoint_executor = ls->get_checkpoint_executor())) {
        ret = OB_ERR_UNEXPECTED;

      } else if (OB_FAIL(checkpoint_executor->update_clog_checkpoint())) {

      } else {
        checkpoint_lsn = ls->get_clog_base_lsn();
        if (OB_FAIL(ls->get_log_handler()->advance_base_lsn(checkpoint_lsn))) {

        } else {
          FLOG_INFO("[CHECKPOINT] advance palf base lsn successfully",
              K(checkpoint_lsn), K(ls->get_ls_id()));
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

int ObCheckPointService::flush_to_recycle_clog_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*);
  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {

  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;

  } else {
    ObLS *ls = nullptr;
    int64_t ls_cnt = 0;
    int64_t succ_ls_cnt = 0;
    for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      ObCheckpointExecutor *checkpoint_executor = ls->get_checkpoint_executor();
      ObDataCheckpoint *data_checkpoint = ls->get_data_checkpoint();
      if (OB_ISNULL(checkpoint_executor) || OB_ISNULL(data_checkpoint)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "checkpoint_executor or data_checkpoint should not be null",
                    KP(checkpoint_executor), KP(data_checkpoint));
      } else if (data_checkpoint->is_flushing()) {

      } else if (OB_TMP_FAIL(checkpoint_executor->update_clog_checkpoint())) {

      } else if (OB_TMP_FAIL(ls->flush_to_recycle_clog())) {

      } else {
        ++succ_ls_cnt;
      }
    }


    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

void ObCheckPointService::ObTraversalFlushTask::runTimerTask()
{

  int ret = OB_SUCCESS;
  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  ObLSService *ls_svr = MTL(ObLSService*);
  ObCurTraceId::init(GCONF.self_addr_);
  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_FAIL(ls_svr->get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {

  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;

  } else {
    ObLS *ls = nullptr;
    int ls_cnt = 0;
    for (; OB_SUCC(iter->get_next(ls)); ++ls_cnt) {
      ObLSHandle ls_handle;
      ObCheckpointExecutor *checkpoint_executor = nullptr;
      if (OB_FAIL(ls_svr->get_ls(ls->get_ls_id(), ls_handle, ObLSGetMod::APPLY_MOD))) {

      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;

      } else if (OB_ISNULL(checkpoint_executor = ls->get_checkpoint_executor())) {
        ret = OB_ERR_UNEXPECTED;

      } else if (OB_FAIL(checkpoint_executor->traversal_flush())) {

      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      if (ls_cnt > 0) {

      } else {

      }
    }
  }
  ObCurTraceId::reset();
}

void ObCheckPointService::ObCheckClogDiskUsageTask::runTimerTask()
{

  int ret = OB_SUCCESS;
  bool need_flush = false;
  logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
  if (OB_ISNULL(log_service)) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_FAIL(log_service->check_need_do_checkpoint(need_flush))) {

  } else if (need_flush) {
    (void)checkpoint_service_.flush_to_recycle_clog_();
  }
}

} // checkpoint
} // storage
} // oceanbase
