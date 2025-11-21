// owner: yichang.yyf
// owner group: transaction

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
#define USING_LOG_PREFIX TABLELOCK
#define protected public
#define private public

#include "storage/ls/ob_ls.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "mtlenv/tablelock/table_lock_common_env.h"
#include "storage/init_basic_struct.h"
#include "storage/tablelock/ob_lock_memtable.h"

namespace oceanbase
{
namespace storage
{
using namespace checkpoint;
}
namespace transaction
{
namespace tablelock
{
class TestLockMemtableCheckpoint : public ::testing::Test
{
public:
  TestLockMemtableCheckpoint()
    : fake_t3m_(common::OB_SERVER_TENANT_ID),
      ls_id_(ObLSID(100)),
      ls_handle_()
  {

  }
  ~TestLockMemtableCheckpoint() = default;

  static void SetUpTestCase();
  static void TearDownTestCase();
  void SetUp() override
  {
    ObCreateLSArg arg;
    ObLSService *ls_svr = MTL(ObLSService*);

    ASSERT_NE(nullptr, ls_svr);
    ASSERT_EQ(OB_SUCCESS,
              storage::gen_create_ls_arg(OB_SYS_TENANT_ID, ls_id_, arg));
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->create_ls(arg));
    ASSERT_EQ(OB_SUCCESS,
              ls_svr->get_ls(ls_id_, ls_handle_, ObLSGetMod::TABLELOCK_MOD));
    ASSERT_NE(nullptr, ls_ = ls_handle_.get_ls());
    ASSERT_EQ(OB_SUCCESS, ls_->get_lock_table()->get_lock_memtable(table_handle_));
    ASSERT_EQ(OB_SUCCESS, table_handle_.get_lock_memtable(memtable_));

  }
  void TearDown() override
  {

  }

private:
  ObLockMemtable *memtable_;
  ObTableHandleV2 table_handle_;
  ObTenantMetaMemMgr fake_t3m_;
  ObFreezer freezer_;
  ObLSID ls_id_;
  ObLS *ls_;
  ObLSHandle ls_handle_;

  ObArenaAllocator allocator_;
};

void TestLockMemtableCheckpoint::SetUpTestCase()
{

  init_default_lock_test_value();
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
}

void TestLockMemtableCheckpoint::TearDownTestCase()
{

  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestLockMemtableCheckpoint, replay_disorder)
{

  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  int ret = OB_SUCCESS;
  ObCreateLSArg arg;

  share::SCN commit_version;
  share::SCN commit_scn;
  commit_version.set_base();
  commit_scn.set_base();

  // 1.recover unlock op and lock op

  ret = memtable_->recover_obj_lock(DEFAULT_OUT_TRANS_UNLOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = memtable_->recover_obj_lock(DEFAULT_OUT_TRANS_LOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2. update lock status disorder

  commit_version.val_ = 3;
  commit_scn.val_ = 3;
  ret = memtable_->update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);

  commit_version.val_ = 2;
  commit_scn.val_ = 2;
  ret = memtable_->update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 3. check checkpoint
  // The rec_scn should be equal with the smaller commit_scn

  ASSERT_EQ(commit_scn.val_, memtable_->get_rec_scn().val_);

  // 4. flush and get a previous commit log
  // You will find the log about disordered replay in the log file.

  ret = memtable_->recover_obj_lock(DEFAULT_OUT_TRANS_UNLOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, memtable_->flush(share::SCN::max_scn(), 0));
  commit_version.val_ = 1;
  commit_scn.val_ = 1;
  ret = memtable_->update_lock_status(DEFAULT_OUT_TRANS_UNLOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 5. check checkpoint
  // The rec_scn should be equal with the smaller commit_scn
  // during flushing (i.e. it's get from pre_rec_scn)

  ASSERT_EQ(commit_scn.val_, memtable_->get_rec_scn().val_);

  // 6. get a commit log with a commit_scn which
  // is larger than freeze_scn during flushing

  ret = memtable_->recover_obj_lock(DEFAULT_OUT_TRANS_LOCK_OP);
  ASSERT_EQ(OB_SUCCESS, ret);
  commit_version.val_ = 4;
  commit_scn.val_ = 4;
  ret = memtable_->update_lock_status(DEFAULT_OUT_TRANS_LOCK_OP,
                                     commit_version,
                                     commit_scn,
                                     COMMIT_LOCK_OP_STATUS);

  // 7. check checkpoint
  // The rec_scn should still be equal with the smaller
  // commit_scn during flushing (i.e. it's get from pre_rec_scn)

  ASSERT_EQ(1, memtable_->get_rec_scn().val_);

  // 8. flush finish

  ret = memtable_->on_memtable_flushed();
  ASSERT_EQ(OB_SUCCESS, ret);

  // 9. check checkpoint
  // The rec_scn should be equal with the latest commit_scn
  // which got during previous flushing (i.e. it's get from rec_scn)

  ASSERT_EQ(commit_scn.val_, memtable_->get_rec_scn().val_);

  // 10. clean up

  table_handle_.reset();
  ls_handle_.reset();
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ls_id_));
}
}  // namespace tablelock
}  // namespace transaction
}  // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_file_name("test_lock_memtable_checkpoint.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
