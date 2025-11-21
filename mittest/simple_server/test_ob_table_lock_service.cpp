// owner: cxf262476
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

#include <gtest/gtest.h>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "multi_replica/env/ob_multi_replica_util.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::transaction;
using namespace oceanbase::storage;

class ObTableLockServiceTest : public ObSimpleClusterTestBase
{
public:
  // Specify the case run directory prefix test_ob_simple_cluster_
  ObTableLockServiceTest() : ObSimpleClusterTestBase("test_ob_lock_service_") {}
  void get_table_id(const char* tname, uint64_t &table_id);
  void get_lock_owner(const char* where_cond, int64_t &raw_owner_id);
  void get_table_part_ids(const uint64_t table_id, ObIArray<ObObjectID> &part_ids);
  void get_table_tablets(const uint64_t table_id, ObTabletIDArray &tablet_list);
};

void ObTableLockServiceTest::get_table_id(const char* tname, uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  static bool need_init = true;
  if (need_init) {
    need_init = false;
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("sys", "oceanbase"));
  }
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  {
    ObSqlString sql;
    table_id = 0;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select table_id from __all_table where table_name='%s'", tname));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_uint("table_id", table_id));
    }
  }
}

void ObTableLockServiceTest::get_lock_owner(const char* where_cond, int64_t &raw_owner_id)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ObSqlString sql;
  raw_owner_id = -1;
  ASSERT_EQ(OB_SUCCESS,
            sql.assign_fmt(
              "select owner_id from %s.%s where %s", OB_SYS_DATABASE_NAME, OB_ALL_VIRTUAL_OBJ_LOCK_TNAME, where_cond));
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
    sqlclient::ObMySQLResult *result = res.get_result();
    ASSERT_NE(nullptr, result);
    if (OB_SUCC(result->next())) {
      ASSERT_EQ(OB_SUCCESS, result->get_int("owner_id", raw_owner_id));
    } else {
      raw_owner_id = -2;
    }
  }
}

void ObTableLockServiceTest::get_table_part_ids(const uint64_t table_id,
                                                ObIArray<ObObjectID> &part_ids)
{
  int ret = OB_SUCCESS;
  int64_t latest_schema_version = OB_INVALID_VERSION;
  ObRefreshSchemaStatus schema_status;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ObMultiVersionSchemaService *schema_service = nullptr;
  share::ObTenantSwitchGuard tenant_guard;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  part_ids.reset();
  ret = tenant_guard.switch_to(tenant_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  schema_service = MTL(ObTenantSchemaService*)->get_schema_service();
  ret = schema_service->get_schema_version_in_inner_table(sql_proxy,
                                                          schema_status,
                                                          latest_schema_version);
  ret = schema_service->async_refresh_schema(tenant_id,
                                             latest_schema_version);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_service->get_tenant_schema_guard(tenant_id,
                                                schema_guard);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_guard.get_table_schema(tenant_id, table_id, table_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, table_schema);
  ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
  share::schema::ObPartitionSchemaIter partition_iter(*table_schema,
                                       check_partition_mode);
  ObObjectID obj_id;
  while (OB_SUCC(partition_iter.next_object_id(obj_id))) {
    ret = part_ids.push_back(obj_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ASSERT_EQ(OB_ITER_END, ret);
}

void ObTableLockServiceTest::get_table_tablets(const uint64_t table_id,
                                               ObTabletIDArray &tablet_list)
{
  int ret = OB_SUCCESS;
  int64_t latest_schema_version = OB_INVALID_VERSION;
  ObRefreshSchemaStatus schema_status;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ObMultiVersionSchemaService *schema_service = nullptr;
  share::ObTenantSwitchGuard tenant_guard;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  tablet_list.reset();
  ret = tenant_guard.switch_to(tenant_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  schema_service = MTL(ObTenantSchemaService*)->get_schema_service();
  ret = schema_service->get_schema_version_in_inner_table(sql_proxy,
                                                          schema_status,
                                                          latest_schema_version);
  ret = schema_service->async_refresh_schema(tenant_id,
                                             latest_schema_version);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_service->get_tenant_schema_guard(tenant_id,
                                                schema_guard);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_guard.get_table_schema(tenant_id, table_id, table_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, table_schema);
  if (PARTITION_LEVEL_ZERO == table_schema->get_part_level()) {
    ret = tablet_list.push_back(table_schema->get_tablet_id());
    ASSERT_EQ(OB_SUCCESS, ret);
  } else {
    ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
    share::schema::ObPartitionSchemaIter partition_iter(*table_schema,
                                         check_partition_mode);
    ObTabletID tablet_id;
    while (OB_SUCC(partition_iter.next_tablet_id(tablet_id))) {
      ret = tablet_list.push_back(tablet_id);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    ASSERT_EQ(OB_ITER_END, ret);
  }
}

TEST_F(ObTableLockServiceTest, observer_start)
{

}

TEST_F(ObTableLockServiceTest, test_ctx)
{
  const uint64_t table_id = 1;
  int64_t timeout_us = 0;
  int64_t retry_timeout_us = 0;

  // 1. TRY LOCK && NOT DEAD LOCK AVOID
  ObTableLockService::ObTableLockCtx ctx_try_lock;
  ctx_try_lock.task_type_ = ObTableLockTaskType::LOCK_TABLE;
  ctx_try_lock.table_id_ = table_id;
  ctx_try_lock.origin_timeout_us_ = timeout_us;
  ctx_try_lock.timeout_us_ = retry_timeout_us;
  ASSERT_TRUE(ctx_try_lock.is_try_lock());
  ASSERT_FALSE(ctx_try_lock.is_deadlock_avoid_enabled());

  // 2. TIMEOUT && NOT DEAD LOCK AVOID
  timeout_us = 15;
  retry_timeout_us = 15;
  ObTableLockService::ObTableLockCtx ctx_no_try_lock;
  ctx_no_try_lock.task_type_ = ObTableLockTaskType::LOCK_TABLE;
  ctx_no_try_lock.table_id_ = table_id;
  ctx_no_try_lock.origin_timeout_us_ = timeout_us;
  ctx_no_try_lock.timeout_us_ = retry_timeout_us;

  ASSERT_FALSE(ctx_no_try_lock.is_try_lock());
  ASSERT_FALSE(ctx_no_try_lock.is_deadlock_avoid_enabled());

  // 3. TIMEOUT && DEAD LOCK AVOID
  timeout_us = 60 * 1000 * 1000;
  retry_timeout_us = 15;
  ObTableLockService::ObTableLockCtx ctx_deadlock_avoid;
  ctx_deadlock_avoid.task_type_ = ObTableLockTaskType::LOCK_TABLE;
  ctx_deadlock_avoid.table_id_ = table_id;
  ctx_deadlock_avoid.origin_timeout_us_ = timeout_us;
  ctx_deadlock_avoid.timeout_us_ = retry_timeout_us;


  ASSERT_FALSE(ctx_deadlock_avoid.is_try_lock());
  ASSERT_TRUE(ctx_deadlock_avoid.is_deadlock_avoid_enabled());
}

TEST_F(ObTableLockServiceTest, iter_ls)
{
  int ret = OB_SUCCESS;

  share::ObTenantSwitchGuard tenant_guard;
  ObSharedGuard<ObLSIterator> ls_iter;
  if (OB_FAIL(tenant_guard.switch_to(OB_SYS_TENANT_ID))) {
    LOG_WARN("switch tenant failed", KR(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter,
                                                     ObLSGetMod::OBSERVER_MOD))) {
    LOG_WARN("failed to get ls iter", KR(ret));
  } else {
    ObLS *ls = NULL;
    while(OB_SUCC(ret)) {
      if (OB_FAIL(ls_iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("scan next ls failed.", KR(ret));
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get ls", KR(ret));
      } else {
        const share::ObLSID &ls_id = ls->get_ls_id();

      }
    }
  }
}

TEST_F(ObTableLockServiceTest, create_table)
{

  // 1. CREATE ONE PART TABLE
  // 2. CREATE MULTI PART TABLE
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  // 1. ONE PART TABLE
  OB_LOG(INFO, "create_table one part table start");
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    sql.assign_fmt(
                   "create table t_one_part (id int, data int, primary key(id)) "
                  );
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  OB_LOG(INFO, "create_table one part table succ");
  OB_LOG(INFO, "insert data start");
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into t_one_part values(%d, %d)", 1, 1));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }
  OB_LOG(INFO, "check row count");
  {
    int64_t row_cnt = 0;
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) row_cnt from t_one_part"));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    }
    ASSERT_EQ(row_cnt, 1);
  }

  // 2. MULTI PART TABLE
  OB_LOG(INFO, "create_table multi part table start");
  {
    ObSqlString sql;
    sql.assign_fmt(
      "create table t_multi_part (id int, data int, primary key(id)) "
      "partition by range(id) (partition p0 values less than (100), partition p1 values less than (200), partition p2 values less than MAXVALUE)");
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }
  OB_LOG(INFO, "create_table multi part table succ");
  OB_LOG(INFO, "insert data start");
  {
    ObSqlString sql;
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into t_multi_part values(%d, %d)", 1, 1));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into t_multi_part values(%d, %d)", 101, 101));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("insert into t_multi_part values(%d, %d)", 202, 202));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  }

  OB_LOG(INFO, "check row count");
  {
    int64_t row_cnt = 0;
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select count(*) row_cnt from t_multi_part"));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      ASSERT_EQ(OB_SUCCESS, result->next());
      ASSERT_EQ(OB_SUCCESS, result->get_int("row_cnt", row_cnt));
    }
    ASSERT_EQ(row_cnt, 3);
  }
}

TEST_F(ObTableLockServiceTest, lock_table)
{

  int ret = OB_SUCCESS;
  ObTableLockOwnerID out_trans_owner_1(ObTableLockOwnerID::get_owner(0, 1));
  ObTableLockOwnerID out_trans_owner_2(ObTableLockOwnerID::get_owner(0, 2));
  uint64_t table_id = 0;
  ObTableLockMode lock_mode = EXCLUSIVE;
  share::ObTenantSwitchGuard tenant_guard;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));
  // 1. LOCK TABLE
  // 1.1 lock one part table

  get_table_id("t_one_part", table_id);
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 1.2 lock multi part table

  get_table_id("t_multi_part", table_id);
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2. UNLOCK TABLE
  // 2.1 unlock one part table

  get_table_id("t_one_part", table_id);
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.2 unlock multi part table

  get_table_id("t_multi_part", table_id);
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 3. UNLOCK NOT EXIST LOCK
  // 3.1 check unlock with no lock

  get_table_id("t_one_part", table_id);
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_1);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
  // 3.2 check unlock with no lock of the specified owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_2);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
  // 3.3 check unlock with no lock of specified lock mode

  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               SHARE,
                                               out_trans_owner_1);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);

  // 4. LOCK TWICE

  get_table_id("t_one_part", table_id);
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, lock_part)
{

  int ret = OB_SUCCESS;
  // 1. LOCK PARTITION
  // 1. lock multi part table
  ObTxParam tx_param;
  share::ObTenantSwitchGuard tenant_guard;
  ObTxDesc *tx_desc = nullptr;
  ObTransService *txs = nullptr;
  uint64_t table_id = 0;
  ObSEArray<ObObjectID, 1> part_ids;
  ObTableLockMode lock_mode = ROW_EXCLUSIVE;
  ObTableLockOwnerID out_trans_owner_1(ObTableLockOwnerID::get_owner(0, 1));
  ObTableLockOwnerID out_trans_owner_2(ObTableLockOwnerID::get_owner(0, 2));
  ObLockPartitionRequest lock_arg;
  ObUnLockPartitionRequest unlock_arg;

  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 6000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc));

  // 1. LOCK MULTI PART TABLE
  // 1.1 lock multi part table

  part_ids.reset();
  get_table_id("t_multi_part", table_id);
  get_table_part_ids(table_id, part_ids);

  lock_mode = ROW_EXCLUSIVE;
  lock_arg.owner_id_ = out_trans_owner_1;
  lock_arg.lock_mode_ = lock_mode;
  lock_arg.op_type_ = OUT_TRANS_LOCK;
  lock_arg.timeout_us_ = 0;
  lock_arg.table_id_ = table_id;
  lock_arg.part_object_id_ = part_ids[0];
  lock_arg.is_from_sql_ = true;

  ret = MTL(ObTableLockService*)->lock(*tx_desc,
                                                 tx_param,
                                                 lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.2 check lock

  lock_mode = SHARE;
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_2);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 2. COMMIT

  int64_t stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 3. check again

  lock_mode = SHARE;
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_2);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 4. UNLOCK

  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc));
  // unlock part

  part_ids.reset();
  get_table_id("t_multi_part", table_id);
  get_table_part_ids(table_id, part_ids);

  lock_mode = ROW_EXCLUSIVE;
  unlock_arg.owner_id_ = out_trans_owner_1;
  unlock_arg.lock_mode_ = lock_mode;
  unlock_arg.op_type_ = OUT_TRANS_UNLOCK;
  unlock_arg.timeout_us_ = 0;
  unlock_arg.table_id_ = table_id;
  unlock_arg.part_object_id_ = part_ids[0];

  ret = MTL(ObTableLockService*)->unlock(*tx_desc,
                                         tx_param,
                                         unlock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  // commit

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc);
  ASSERT_EQ(OB_SUCCESS, ret);
  // check again

  lock_mode = SHARE;
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_2);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, lock_tablet)
{
  // 1. LOCK TABLET
  // 1.1 lock tablet of one part table
  // 1.2 lock tablet of multi part table
  // 2. UNLOCK TABLET
  // 2.1 unlock tablet of one part table
  // 2.2 unlock tablet of multi part table
  int ret = OB_SUCCESS;
  ObTableLockOwnerID out_trans_owner_1(ObTableLockOwnerID::get_owner(0, 1));
  ObTableLockOwnerID out_trans_owner_2(ObTableLockOwnerID::get_owner(0, 2));
  uint64_t table_id = 0;
  ObTableLockMode lock_mode = EXCLUSIVE;
  share::ObTenantSwitchGuard tenant_guard;
  ObTabletIDArray tablet_list;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));
  // 1. LOCK TABLE
  // 1.1 lock one part table

  get_table_id("t_one_part", table_id);
  get_table_tablets(table_id, tablet_list);
  ret = MTL(ObTableLockService*)->lock_tablet(table_id,
                                              tablet_list[0],
                                              lock_mode,
                                              out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 1.2 lock multi part table

  get_table_id("t_multi_part", table_id);
  get_table_tablets(table_id, tablet_list);
  ret = MTL(ObTableLockService*)->lock_tablet(table_id,
                                              tablet_list[0],
                                              lock_mode,
                                              out_trans_owner_2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2. UNLOCK TABLE
  // 2.1 unlock one part table

  get_table_id("t_one_part", table_id);
  get_table_tablets(table_id, tablet_list);
  ret = MTL(ObTableLockService*)->unlock_tablet(table_id,
                                                tablet_list[0],
                                                lock_mode,
                                                out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.2 unlock multi part table

  get_table_id("t_multi_part", table_id);
  get_table_tablets(table_id, tablet_list);
  ret = MTL(ObTableLockService*)->unlock_tablet(table_id,
                                                tablet_list[0],
                                                lock_mode,
                                                out_trans_owner_2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 3. UNLOCK NOT EXIST LOCK
  // 3.1 check unlock with no lock

  get_table_id("t_one_part", table_id);
  get_table_tablets(table_id, tablet_list);
  ret = MTL(ObTableLockService*)->unlock_tablet(table_id,
                                                tablet_list[0],
                                                lock_mode,
                                                out_trans_owner_2);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
  // 3.2 check unlock with no lock of the specified owner

  ret = MTL(ObTableLockService*)->lock_tablet(table_id,
                                              tablet_list[0],
                                              lock_mode,
                                              out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTableLockService*)->unlock_tablet(table_id,
                                                tablet_list[0],
                                                lock_mode,
                                                out_trans_owner_2);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);
  // 3.3 check unlock with no lock of specified lock mode

  ret = MTL(ObTableLockService*)->unlock_tablet(table_id,
                                                tablet_list[0],
                                                SHARE,
                                                out_trans_owner_1);
  ASSERT_EQ(OB_OBJ_LOCK_NOT_EXIST, ret);

  // 4. LOCK TWICE

  get_table_id("t_one_part", table_id);
  get_table_tablets(table_id, tablet_list);
  ret = MTL(ObTableLockService*)->lock_tablet(table_id,
                                              tablet_list[0],
                                              lock_mode,
                                              out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTableLockService*)->unlock_tablet(table_id,
                                                tablet_list[0],
                                                lock_mode,
                                                out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, in_trans_lock_table)
{

  int ret = OB_SUCCESS;
  // 1. LOCK TABLE
  // 1.1 lock one part table
  // 1.2 lock multi part table
  ObTxParam tx_param;
  share::ObTenantSwitchGuard tenant_guard;
  ObTxDesc *tx_desc = nullptr;
  ObTransService *txs = nullptr;
  uint64_t table_id = 0;
  ObTableLockMode lock_mode = ROW_EXCLUSIVE;
  ObTableLockOwnerID in_trans_owner(ObTableLockOwnerID::default_owner());
  ObTableLockOwnerID out_trans_owner(ObTableLockOwnerID::get_owner(0, 1));
  ObLockTableRequest lock_arg;

  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 6000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc));

  // 1.1 lock one part table

  get_table_id("t_one_part", table_id);
  lock_mode = ROW_EXCLUSIVE;
  lock_arg.table_id_ = table_id;
  lock_arg.owner_id_ = in_trans_owner;
  lock_arg.lock_mode_ = lock_mode;
  lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
  lock_arg.timeout_us_ = 0;
  lock_arg.is_from_sql_ = true;
  ret = MTL(ObTableLockService*)->lock(*tx_desc,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.2 check lock
  lock_mode = SHARE;

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner);
  ASSERT_EQ(OB_EAGAIN, ret);
  // 2. LOCK MULTI PART TABLE
  // 2.1 lock multi part table
  // lock upgrade

  get_table_id("t_multi_part", table_id);
  lock_mode = ROW_EXCLUSIVE;
  lock_arg.table_id_ = table_id;
  lock_arg.owner_id_ = in_trans_owner;
  lock_arg.lock_mode_ = lock_mode;
  lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
  lock_arg.timeout_us_ = 0;
  lock_arg.is_from_sql_ = true;
  ret = MTL(ObTableLockService*)->lock(*tx_desc,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2.2 check lock

  lock_mode = SHARE;
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner);
  ASSERT_EQ(OB_EAGAIN, ret);
  // 3. CLEAN

  const int64_t stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, lock_out_trans_after_in_trans)
{

  int ret = OB_SUCCESS;
  // 1. LOCK TABLE
  // 1.1 lock one part table
  ObTxParam tx_param;
  share::ObTenantSwitchGuard tenant_guard;
  ObTxDesc *tx_desc = nullptr;
  ObTransService *txs = nullptr;
  uint64_t table_id = 0;
  ObTableLockMode lock_mode = ROW_EXCLUSIVE;
  ObTableLockOwnerID out_trans_owner_1(ObTableLockOwnerID::get_owner(0, 1));
  ObTableLockOwnerID out_trans_owner_2(ObTableLockOwnerID::get_owner(0, 2));
  ObTableLockOwnerID in_trans_owner(ObTableLockOwnerID::default_owner());
  ObLockTableRequest lock_arg;

  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 6000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc));

  // 1. ONLY IN_TRANS TEST
  // 1.1 lock in_trans

  get_table_id("t_one_part", table_id);
  lock_mode = ROW_EXCLUSIVE;
  lock_arg.table_id_ = table_id;
  lock_arg.owner_id_ = in_trans_owner;
  lock_arg.lock_mode_ = lock_mode;
  lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
  lock_arg.timeout_us_ = 0;
  lock_arg.is_from_sql_ = true;
  ret = MTL(ObTableLockService*)->lock(*tx_desc,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 1.2 check lock
  lock_mode = SHARE;

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_1);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 1.3. commit lock

  int64_t stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 1.4 recheck lock after commit
  lock_mode = SHARE;

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 1.5 unlock check lock
  lock_mode = SHARE;

  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2. BOTH OUT_TRANS AND IN_TRANS
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc));
  // 2.1 lock in_trans lock

  get_table_id("t_one_part", table_id);
  lock_mode = ROW_EXCLUSIVE;
  lock_arg.table_id_ = table_id;
  lock_arg.owner_id_ = in_trans_owner;
  lock_arg.lock_mode_ = lock_mode;
  lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
  lock_arg.timeout_us_ = 0;
  lock_arg.is_from_sql_ = true;
  ret = MTL(ObTableLockService*)->lock(*tx_desc,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2.2 lock out_trans lock

  lock_mode = ROW_EXCLUSIVE;
  lock_arg.table_id_ = table_id;
  lock_arg.owner_id_ = out_trans_owner_1;
  lock_arg.lock_mode_ = lock_mode;
  lock_arg.op_type_ = OUT_TRANS_LOCK;
  lock_arg.timeout_us_ = 0;
  lock_arg.is_from_sql_ = true;
  ret = MTL(ObTableLockService*)->lock(*tx_desc,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2.3 check lock
  lock_mode = SHARE;

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_2);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 2.4 commit lock

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2.5 recheck lock after commit
  lock_mode = SHARE;

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_2);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 2.6 unlock out_trans lock
  lock_mode = ROW_EXCLUSIVE;

  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2.7 recheck after unlock
  lock_mode = SHARE;

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             lock_mode,
                                             out_trans_owner_2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2.8 unlock check lock
  lock_mode = SHARE;

  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               lock_mode,
                                               out_trans_owner_2);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, in_trans_lock_obj)
{

  int ret = OB_SUCCESS;

  ObTxParam tx_param;
  share::ObTenantSwitchGuard tenant_guard;
  ObTxDesc *tx_desc1 = nullptr;
  ObTransService *txs = nullptr;
  uint64_t obj_id1 = 1010;
  ObTableLockMode lock_mode1 = SHARE;
  ObTableLockOwnerID OWNER_ONE;
  OWNER_ONE.convert_from_value(static_cast<ObLockOwnerType>(0), 1);
  ObLockObjsRequest lock_arg;
  ObLockID lock_id;

  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 6000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc1));


  lock_arg.lock_mode_ = lock_mode1;
  lock_arg.op_type_ = IN_TRANS_COMMON_LOCK;
  lock_arg.timeout_us_ = 0;
  lock_id.set(ObLockOBJType::OBJ_TYPE_COMMON_OBJ, obj_id1);
  lock_arg.objs_.push_back(lock_id);
  lock_arg.is_from_sql_ = true;

  ret = MTL(ObTableLockService*)->lock(*tx_desc1,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);


  ObTxDesc *tx_desc2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc2));
  ret = MTL(ObTableLockService*)->lock(*tx_desc2,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);


  ObTxDesc *tx_desc3 = nullptr;
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc3));
  ObTableLockMode lock_mode2 = EXCLUSIVE;
  lock_arg.lock_mode_ = lock_mode2;
  ret = MTL(ObTableLockService*)->lock(*tx_desc3,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_ERR_EXCLUSIVE_LOCK_CONFLICT, ret);


  const int64_t stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc1, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->commit_tx(*tx_desc2, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->commit_tx(*tx_desc3, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc3);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, replace_lock_table_from_x_to_rx)
{

  int ret = OB_SUCCESS;
  // 1. LOCK TABLE
  // 1.1 lock one part table
  ObTxParam tx_param;
  share::ObTenantSwitchGuard tenant_guard;
  ObTxDesc *tx_desc1 = nullptr;
  ObTxDesc *tx_desc2= nullptr;
  ObTxDesc *tx_desc3= nullptr;
  ObTransService *txs = nullptr;
  uint64_t table_id = 0;
  ObTabletIDArray tablet_list;
  ObTableLockMode ori_lock_mode = EXCLUSIVE;
  ObTableLockMode check_lock_mode = EXCLUSIVE;
  ObTableLockMode new_lock_mode = ROW_EXCLUSIVE;
  ObTableLockOwnerID owner_one(ObTableLockOwnerID::get_owner(0, 1));
  ObTableLockOwnerID owner_two(ObTableLockOwnerID::get_owner(0, 2));
  ObLockTableRequest lock_arg;
  ObUnLockTableRequest unlock_arg;
  int64_t stmt_timeout_ts = -1;
  char where_cond[512] = {0};
  int64_t raw_owner_id = -1;

  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 6000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc1));

  // 1. lock table

  get_table_id("t_one_part", table_id);
  get_table_tablets(table_id, tablet_list);
  lock_arg.table_id_ = table_id;
  lock_arg.owner_id_ = owner_one;
  lock_arg.lock_mode_ = ori_lock_mode;
  lock_arg.op_type_ = OUT_TRANS_LOCK;
  lock_arg.timeout_us_ = 0;

  unlock_arg.table_id_ = table_id;
  unlock_arg.owner_id_ = owner_one;
  unlock_arg.lock_mode_ = ori_lock_mode;
  unlock_arg.op_type_ = OUT_TRANS_UNLOCK;
  unlock_arg.timeout_us_ = 0;

  ret = MTL(ObTableLockService*)->lock(*tx_desc1,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2. check lock

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_two);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 3. commit lock

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc1, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 4. replace lock
  // check before commit replace
  ASSERT_EQ(OB_SUCCESS, databuff_printf(where_cond, 512, "obj_id = %lu LIMIT 1", table_id));
  get_lock_owner(where_cond, raw_owner_id);
  ASSERT_EQ(1, raw_owner_id);

  ASSERT_EQ(OB_SUCCESS, databuff_printf(where_cond, 512, "obj_id = %lu LIMIT 1", tablet_list[0].id()));
  get_lock_owner(where_cond, raw_owner_id);
  ASSERT_EQ(1, raw_owner_id);


  ObReplaceLockRequest replace_lock_arg;
  replace_lock_arg.unlock_req_ = &unlock_arg;
  replace_lock_arg.new_lock_mode_ = new_lock_mode;
  replace_lock_arg.new_lock_owner_ = owner_two;
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc2));

  ret = MTL(ObTableLockService*)->replace_lock(*tx_desc2,
                                               tx_param,
                                               replace_lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 5. commit rplace lock

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc2, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // check after commit replace
  ASSERT_EQ(OB_SUCCESS, databuff_printf(where_cond, 512, "obj_id = %lu LIMIT 1", table_id));
  get_lock_owner(where_cond, raw_owner_id);
  ASSERT_EQ(2, raw_owner_id);

  // there's no lock on tablet, so owner is -2
  ASSERT_EQ(OB_SUCCESS, databuff_printf(where_cond, 512, "obj_id = %lu LIMIT 1", tablet_list[0].id()));
  get_lock_owner(where_cond, raw_owner_id);
  ASSERT_EQ(-2, raw_owner_id);

  // 6. try to lock by origin owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_one);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 7. check new owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             new_lock_mode,
                                             owner_two);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 8. unlock

  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc3));
  unlock_arg.owner_id_ = owner_two;
  unlock_arg.lock_mode_ = new_lock_mode;
  ret = MTL(ObTableLockService *)->unlock(*tx_desc3, tx_param, unlock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc3, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc3);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 9. try to lock by origin owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_one);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 10. check new owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_two);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 11. unlock by origin owner

  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               check_lock_mode,
                                               owner_one);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, replace_lock_table_from_rx_to_x)
{

  int ret = OB_SUCCESS;
  // 1. LOCK TABLE
  // 1.1 lock one part table
  ObTxParam tx_param;
  share::ObTenantSwitchGuard tenant_guard;
  ObTxDesc *tx_desc1 = nullptr;
  ObTxDesc *tx_desc2= nullptr;
  ObTxDesc *tx_desc3= nullptr;
  ObTransService *txs = nullptr;
  uint64_t table_id = 0;
  ObTabletIDArray tablet_list;
  ObTableLockMode ori_lock_mode = ROW_EXCLUSIVE;
  ObTableLockMode check_lock_mode = EXCLUSIVE;
  ObTableLockMode new_lock_mode = EXCLUSIVE;
  ObTableLockOwnerID owner_one(ObTableLockOwnerID::get_owner(0, 1));
  ObTableLockOwnerID owner_two(ObTableLockOwnerID::get_owner(0, 2));
  ObLockTableRequest lock_arg;
  ObUnLockTableRequest unlock_arg;
  int64_t stmt_timeout_ts = -1;
  char where_cond[512] = {0};
  int64_t raw_owner_id = -1;

  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 6000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc1));

  // 1. lock table

  get_table_id("t_one_part", table_id);
  get_table_tablets(table_id, tablet_list);
  lock_arg.table_id_ = table_id;
  lock_arg.owner_id_ = owner_one;
  lock_arg.lock_mode_ = ori_lock_mode;
  lock_arg.op_type_ = OUT_TRANS_LOCK;
  lock_arg.timeout_us_ = 0;

  unlock_arg.table_id_ = table_id;
  unlock_arg.owner_id_ = owner_one;
  unlock_arg.lock_mode_ = ori_lock_mode;
  unlock_arg.op_type_ = OUT_TRANS_UNLOCK;
  unlock_arg.timeout_us_ = 0;

  ret = MTL(ObTableLockService*)->lock(*tx_desc1,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2. check lock

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_two);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 3. commit lock

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc1, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 4. replace lock
  // check before commit replace
  ASSERT_EQ(OB_SUCCESS, databuff_printf(where_cond, 512, "obj_id = %lu LIMIT 1", table_id));
  get_lock_owner(where_cond, raw_owner_id);
  ASSERT_EQ(1, raw_owner_id);

  // there's no lock on tablet, so owner is -2
  ASSERT_EQ(OB_SUCCESS, databuff_printf(where_cond, 512, "obj_id = %lu LIMIT 1", tablet_list[0].id()));
  get_lock_owner(where_cond, raw_owner_id);
  ASSERT_EQ(-2, raw_owner_id);


  ObReplaceLockRequest replace_lock_arg;
  replace_lock_arg.unlock_req_ = &unlock_arg;
  replace_lock_arg.new_lock_mode_ = new_lock_mode;
  replace_lock_arg.new_lock_owner_ = owner_two;
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc2));

  ret = MTL(ObTableLockService*)->replace_lock(*tx_desc2,
                                               tx_param,
                                               replace_lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 5. commit rplace lock

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc2, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // check after commit replace
  ASSERT_EQ(OB_SUCCESS, databuff_printf(where_cond, 512, "obj_id = %lu LIMIT 1", table_id));
  get_lock_owner(where_cond, raw_owner_id);
  ASSERT_EQ(2, raw_owner_id);

  ASSERT_EQ(OB_SUCCESS, databuff_printf(where_cond, 512, "obj_id = %lu LIMIT 1", tablet_list[0].id()));
  get_lock_owner(where_cond, raw_owner_id);
  ASSERT_EQ(2, raw_owner_id);

  // 6. try to lock by origin owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_one);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 7. unlock

  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc3));
  unlock_arg.owner_id_ = owner_two;
  unlock_arg.lock_mode_ = new_lock_mode;
  ret = MTL(ObTableLockService *)->unlock(*tx_desc3, tx_param, unlock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc3, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc3);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 8. try to lock by origin owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_one);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 9. check new owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_two);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 10. unlock by origin owner

  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               check_lock_mode,
                                               owner_one);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, replace_lock_table_from_x_to_s)
{

  int ret = OB_SUCCESS;
  // 1. LOCK TABLE
  // 1.1 lock one part table
  ObTxParam tx_param;
  share::ObTenantSwitchGuard tenant_guard;
  ObTxDesc *tx_desc1 = nullptr;
  ObTxDesc *tx_desc2= nullptr;
  ObTxDesc *tx_desc3= nullptr;
  ObTransService *txs = nullptr;
  uint64_t table_id = 0;
  ObTabletIDArray tablet_list;
  ObTableLockMode ori_lock_mode = EXCLUSIVE;
  ObTableLockMode check_lock_mode = EXCLUSIVE;
  ObTableLockMode new_lock_mode = SHARE;
  ObTableLockOwnerID owner_one(ObTableLockOwnerID::get_owner(0, 1));
  ObTableLockOwnerID owner_two(ObTableLockOwnerID::get_owner(0, 2));
  ObLockTableRequest lock_arg;
  ObUnLockTableRequest unlock_arg;
  int64_t stmt_timeout_ts = -1;
  char where_cond[512] = {0};
  int64_t raw_owner_id = -1;

  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 6000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc1));

  // 1. lock table

  get_table_id("t_one_part", table_id);
  get_table_tablets(table_id, tablet_list);
  lock_arg.table_id_ = table_id;
  lock_arg.owner_id_ = owner_one;
  lock_arg.lock_mode_ = ori_lock_mode;
  lock_arg.op_type_ = OUT_TRANS_LOCK;
  lock_arg.timeout_us_ = 0;

  unlock_arg.table_id_ = table_id;
  unlock_arg.owner_id_ = owner_one;
  unlock_arg.lock_mode_ = ori_lock_mode;
  unlock_arg.op_type_ = OUT_TRANS_UNLOCK;
  unlock_arg.timeout_us_ = 0;

  ret = MTL(ObTableLockService*)->lock(*tx_desc1,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2. check lock

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_two);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 3. commit lock

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc1, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 4. replace lock
  // check before commit replace
  ASSERT_EQ(OB_SUCCESS, databuff_printf(where_cond, 512, "obj_id = %lu LIMIT 1", table_id));
  get_lock_owner(where_cond, raw_owner_id);
  ASSERT_EQ(1, raw_owner_id);

  ASSERT_EQ(OB_SUCCESS, databuff_printf(where_cond, 512, "obj_id = %lu LIMIT 1", tablet_list[0].id()));
  get_lock_owner(where_cond, raw_owner_id);
  ASSERT_EQ(1, raw_owner_id);


  ObReplaceLockRequest replace_lock_arg;
  replace_lock_arg.unlock_req_ = &unlock_arg;
  replace_lock_arg.new_lock_mode_ = new_lock_mode;
  replace_lock_arg.new_lock_owner_ = owner_two;
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc2));

  ret = MTL(ObTableLockService*)->replace_lock(*tx_desc2,
                                               tx_param,
                                               replace_lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 5. commit rplace lock

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc2, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // check after commit replace
  ASSERT_EQ(OB_SUCCESS, databuff_printf(where_cond, 512, "obj_id = %lu LIMIT 1", table_id));
  get_lock_owner(where_cond, raw_owner_id);
  ASSERT_EQ(2, raw_owner_id);

  ASSERT_EQ(OB_SUCCESS, databuff_printf(where_cond, 512, "obj_id = %lu LIMIT 1", tablet_list[0].id()));
  get_lock_owner(where_cond, raw_owner_id);
  ASSERT_EQ(2, raw_owner_id);

  // 6. try to lock by origin owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_one);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 7. check new owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             new_lock_mode,
                                             owner_two);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 8. unlock

  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc3));
  unlock_arg.owner_id_ = owner_two;
  unlock_arg.lock_mode_ = new_lock_mode;
  ret = MTL(ObTableLockService *)->unlock(*tx_desc3, tx_param, unlock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc3, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc3);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 9. try to lock by origin owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_one);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 10. check new owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_two);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 11. unlock by origin owner

  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               check_lock_mode,
                                               owner_one);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, replace_lock_part)
{

  int ret = OB_SUCCESS;
  // 1.1 lock one part table
  ObTxParam tx_param;
  share::ObTenantSwitchGuard tenant_guard;
  ObTxDesc *tx_desc1 = nullptr;
  ObTxDesc *tx_desc3 = nullptr;
  ObTxDesc *tx_desc2 = nullptr;
  ObTxDesc *tx_desc4 = nullptr;
  ObTransService *txs = nullptr;
  uint64_t table_id = 0;
  ObSEArray<ObObjectID, 1> part_ids;
  ObTableLockMode ori_lock_mode = EXCLUSIVE;
  ObTableLockMode check_lock_mode = EXCLUSIVE;
  ObTableLockMode new_lock_mode = ROW_SHARE;
  ObTableLockOwnerID owner_one(ObTableLockOwnerID::get_owner(0, 1));
  ObTableLockOwnerID owner_two(ObTableLockOwnerID::get_owner(0, 2));
  ObLockPartitionRequest lock_arg;
  ObUnLockPartitionRequest unlock_arg;
  int64_t stmt_timeout_ts = -1;

  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 6000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc1));

  // 1. lock partition

  part_ids.reset();
  get_table_id("t_multi_part", table_id);
  get_table_part_ids(table_id, part_ids);

  lock_arg.owner_id_ = owner_one;
  lock_arg.lock_mode_ = ori_lock_mode;
  lock_arg.op_type_ = OUT_TRANS_LOCK;
  lock_arg.timeout_us_ = 0;
  lock_arg.table_id_ = table_id;
  lock_arg.part_object_id_ = part_ids[0];

  unlock_arg.owner_id_ = owner_one;
  unlock_arg.lock_mode_ = ori_lock_mode;
  unlock_arg.op_type_ = OUT_TRANS_UNLOCK;
  unlock_arg.timeout_us_ = 0;
  unlock_arg.table_id_ = table_id;
  unlock_arg.part_object_id_ = part_ids[0];

  ret = MTL(ObTableLockService*)->lock(*tx_desc1,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2. check lock

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_two);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 3. commit lock

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc1, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 4. replace lock

  ObReplaceLockRequest replace_lock_arg;
  replace_lock_arg.unlock_req_ = &unlock_arg;
  replace_lock_arg.new_lock_mode_ = new_lock_mode;
  replace_lock_arg.new_lock_owner_ = owner_two;
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc2));

  ret = MTL(ObTableLockService*)->replace_lock(*tx_desc2,
                                               tx_param,
                                               replace_lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 5. check lock

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             ori_lock_mode,
                                             owner_one);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 6. commit rplace lock

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc2, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 7. check lock again

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             ori_lock_mode,
                                             owner_one);
  ASSERT_EQ(OB_EAGAIN, ret);
  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             new_lock_mode,
                                             owner_two);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               new_lock_mode,
                                               owner_two);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc3));
  lock_arg.lock_mode_ = new_lock_mode;
  lock_arg.owner_id_ = owner_two;
  ret = MTL(ObTableLockService *)->lock(*tx_desc3, tx_param, lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->commit_tx(*tx_desc3, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc3);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 8. unlock and commit

  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc4));
  unlock_arg.lock_mode_ = new_lock_mode;
  unlock_arg.owner_id_ = owner_two;
  ret = MTL(ObTableLockService *)->unlock(*tx_desc4, tx_param, unlock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc4, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc4);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 9. check lock

  ret = MTL(ObTableLockService *)->lock_table(table_id, check_lock_mode, owner_one);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = MTL(ObTableLockService *)->unlock_table(table_id, check_lock_mode, owner_one);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, replace_lock_obj)
{

  int ret = OB_SUCCESS;
  ObTxParam tx_param;
  share::ObTenantSwitchGuard tenant_guard;
  ObTxDesc *tx_desc1 = nullptr;
  ObTxDesc *tx_desc3 = nullptr;
  ObTxDesc *tx_desc2 = nullptr;
  ObTxDesc *tx_desc4 = nullptr;
  ObTransService *txs = nullptr;
  uint64_t table_id;
  ObTableLockMode ori_lock_mode = EXCLUSIVE;
  ObTableLockMode check_lock_mode = EXCLUSIVE;
  ObTableLockMode new_lock_mode = ROW_SHARE;
  ObTableLockOwnerID owner_one(ObTableLockOwnerID::get_owner(0, 1));
  ObTableLockOwnerID owner_two(ObTableLockOwnerID::get_owner(0, 2));
  ObLockObjsRequest lock_arg;
  ObUnLockObjsRequest unlock_arg;
  int64_t stmt_timeout_ts = -1;
  int64_t obj_id1 = 1001;
  int64_t obj_id2 = 1002;
  int64_t obj_id3 = 1003;

  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 6000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc1));

  // 1. lock obj

  get_table_id("t_one_part", table_id);
  lock_arg.owner_id_ = owner_one;
  lock_arg.lock_mode_ = ori_lock_mode;
  lock_arg.op_type_ = OUT_TRANS_LOCK;
  lock_arg.timeout_us_ = 0;
  ObLockID lock_id1;
  ObLockID lock_id2;
  ObLockID lock_id3;
  lock_id1.set(ObLockOBJType::OBJ_TYPE_COMMON_OBJ, obj_id1);
  lock_arg.objs_.push_back(lock_id1);
  lock_id2.set(ObLockOBJType::OBJ_TYPE_COMMON_OBJ, obj_id2);
  lock_arg.objs_.push_back(lock_id2);
  lock_id3.set(ObLockOBJType::OBJ_TYPE_COMMON_OBJ, obj_id3);
  lock_arg.objs_.push_back(lock_id3);

  unlock_arg.owner_id_ = owner_one;
  unlock_arg.lock_mode_ = ori_lock_mode;
  unlock_arg.op_type_ = OUT_TRANS_UNLOCK;
  unlock_arg.timeout_us_ = 0;
  unlock_arg.objs_.push_back(lock_id1);
  unlock_arg.objs_.push_back(lock_id2);
  unlock_arg.objs_.push_back(lock_id3);

  ret = MTL(ObTableLockService*)->lock(*tx_desc1,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2. commit lock

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc1, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 3. replace lock

  ObReplaceLockRequest replace_lock_arg;
  replace_lock_arg.unlock_req_ = &unlock_arg;
  replace_lock_arg.new_lock_mode_ = new_lock_mode;
  replace_lock_arg.new_lock_owner_ = owner_two;
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc2));

  ret = MTL(ObTableLockService*)->replace_lock(*tx_desc2,
                                               tx_param,
                                               replace_lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 4. check lock

  lock_arg.lock_mode_ = new_lock_mode;
  lock_arg.owner_id_ = owner_two;
  ret = MTL(ObTableLockService*)->lock(*tx_desc2,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 5. commit rplace lock

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc2, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 6. check lock again

  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc3));
  ret = MTL(ObTableLockService *)->lock(*tx_desc3, tx_param, lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->commit_tx(*tx_desc3, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc3);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 7. unlock and commit

  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc4));
  unlock_arg.op_type_ = OUT_TRANS_UNLOCK;
  unlock_arg.lock_mode_ = new_lock_mode;
  unlock_arg.owner_id_ = owner_two;
  ret = MTL(ObTableLockService *)->unlock(*tx_desc4, tx_param, unlock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc4, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc4);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, replace_lock_and_unlock_concurrency)
{

  int ret = OB_SUCCESS;
  ObTxParam tx_param;
  share::ObTenantSwitchGuard tenant_guard;
  ObTxDesc *tx_desc1 = nullptr;
  ObTxDesc *tx_desc2= nullptr;
  ObTxDesc *tx_desc3= nullptr;
  ObTransService *txs = nullptr;
  uint64_t table_id = 0;
  ObTableLockMode ori_lock_mode = EXCLUSIVE;
  ObTableLockMode check_lock_mode = EXCLUSIVE;
  ObTableLockMode new_lock_mode = SHARE;
  ObTableLockOwnerID owner_one(ObTableLockOwnerID::get_owner(0, 1));
  ObTableLockOwnerID owner_two(ObTableLockOwnerID::get_owner(0, 2));
  ObLockTableRequest lock_arg;
  ObUnLockTableRequest unlock_arg;
  int64_t stmt_timeout_ts = -1;

  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 6000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc1));

  // 1. lock in_trans

  get_table_id("t_one_part", table_id);
  lock_arg.table_id_ = table_id;
  lock_arg.owner_id_ = owner_one;
  lock_arg.lock_mode_ = ori_lock_mode;
  lock_arg.op_type_ = OUT_TRANS_LOCK;
  lock_arg.timeout_us_ = 0;

  unlock_arg.table_id_ = table_id;
  unlock_arg.owner_id_ = owner_one;
  unlock_arg.lock_mode_ = ori_lock_mode;
  unlock_arg.op_type_ = OUT_TRANS_UNLOCK;
  unlock_arg.timeout_us_ = 0;

  ret = MTL(ObTableLockService*)->lock(*tx_desc1,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2. check lock

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_two);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 3. commit lock

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc1, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 3. unlock and not commit
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc2));
  ret = MTL(ObTableLockService*)->unlock(*tx_desc2,
                                         tx_param,
                                         unlock_arg);

  // 4. replace lock

  ObReplaceLockRequest replace_lock_arg;
  replace_lock_arg.unlock_req_ = &unlock_arg;
  replace_lock_arg.new_lock_mode_ = new_lock_mode;
  replace_lock_arg.new_lock_owner_ = owner_two;
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc3));

  ret = MTL(ObTableLockService*)->replace_lock(*tx_desc3,
                                               tx_param,
                                               replace_lock_arg);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 5. rollback unlock

  ret = txs->rollback_tx(*tx_desc2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 6. replace again

  ret = MTL(ObTableLockService*)->replace_lock(*tx_desc3,
                                               tx_param,
                                               replace_lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 7. commit rplace lock

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc3, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc3);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 8. try to lock by origin owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_one);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 9. check new owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             new_lock_mode,
                                             owner_two);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 10. unlock

  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc3));
  unlock_arg.owner_id_ = owner_two;
  unlock_arg.lock_mode_ = new_lock_mode;
  ret = MTL(ObTableLockService *)->unlock(*tx_desc3, tx_param, unlock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc3, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc3);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 11. try to lock by origin owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_one);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 12. check new owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_two);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 13. unlock by origin owner

  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               check_lock_mode,
                                               owner_one);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, replace_all_locks)
{

  int ret = OB_SUCCESS;
  ObTxParam tx_param;
  share::ObTenantSwitchGuard tenant_guard;
  ObTxDesc *tx_desc1 = nullptr;
  ObTxDesc *tx_desc2= nullptr;
  ObTxDesc *tx_desc3= nullptr;
  ObTxDesc *tx_desc4= nullptr;
  ObTransService *txs = nullptr;
  uint64_t table_id = 0;
  ObTableLockMode check_lock_mode = EXCLUSIVE;
  ObTableLockMode new_lock_mode = SHARE;
  ObTableLockOwnerID owner_one(ObTableLockOwnerID::get_owner(0, 1));
  ObTableLockOwnerID owner_two(ObTableLockOwnerID::get_owner(0, 2));
  ObTableLockOwnerID owner_three(ObTableLockOwnerID::get_owner(0, 3));
  ObTableLockMode lock_mode_one = ROW_SHARE;
  ObTableLockMode lock_mode_two = ROW_EXCLUSIVE;
  ObLockTableRequest lock_arg;
  ObLockTableRequest new_lock_arg;
  ObUnLockTableRequest unlock_arg1;
  ObUnLockTableRequest unlock_arg2;
  int64_t stmt_timeout_ts = -1;

  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 6000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc1));

  // 1. lock out_trans

  get_table_id("t_one_part", table_id);
  lock_arg.table_id_ = table_id;
  lock_arg.owner_id_ = owner_one;
  lock_arg.lock_mode_ = lock_mode_one;
  lock_arg.op_type_ = OUT_TRANS_LOCK;
  lock_arg.timeout_us_ = 0;

  unlock_arg1.table_id_ = table_id;
  unlock_arg1.owner_id_ = owner_one;
  unlock_arg1.lock_mode_ = lock_mode_one;
  unlock_arg1.op_type_ = OUT_TRANS_UNLOCK;
  unlock_arg1.timeout_us_ = 0;

  ret = MTL(ObTableLockService*)->lock(*tx_desc1,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
  // 2. check lock

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_two);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 3. commit lock

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc1, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 4. lock table and commit with another owenr and mode

  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc2));
  lock_arg.owner_id_ = owner_two;
  lock_arg.lock_mode_ = lock_mode_two;

  unlock_arg2.table_id_ = table_id;
  unlock_arg2.owner_id_ = owner_two;
  unlock_arg2.lock_mode_ = lock_mode_two;
  unlock_arg2.op_type_ = OUT_TRANS_UNLOCK;
  unlock_arg2.timeout_us_ = 0;

  ret = MTL(ObTableLockService*)->lock(*tx_desc2,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = txs->commit_tx(*tx_desc2, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 5. replace lock

  ObArenaAllocator allocator;
  ObReplaceAllLocksRequest replace_lock_arg(allocator);
  new_lock_arg.table_id_ = table_id;
  new_lock_arg.owner_id_ = owner_three;
  new_lock_arg.lock_mode_ = EXCLUSIVE;
  new_lock_arg.op_type_ = OUT_TRANS_LOCK;
  new_lock_arg.timeout_us_ = 0;

  replace_lock_arg.lock_req_ = &new_lock_arg;
  replace_lock_arg.unlock_req_list_.push_back(&unlock_arg1);
  replace_lock_arg.unlock_req_list_.push_back(&unlock_arg2);
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc3));

  ret = MTL(ObTableLockService*)->replace_lock(*tx_desc3,
                                               tx_param,
                                               replace_lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);
   // 6. commit rplace lock

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc3, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc3);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 7. try to lock by origin owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             ROW_SHARE,
                                             owner_one);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 8. check new owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             EXCLUSIVE,
                                             owner_three);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 9. unlock

  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc4));
  unlock_arg2.owner_id_ = owner_three;
  unlock_arg2.lock_mode_ = EXCLUSIVE;
  ret = MTL(ObTableLockService *)->unlock(*tx_desc4, tx_param, unlock_arg2);
  ASSERT_EQ(OB_SUCCESS, ret);

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc4, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc4);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 10. try to lock by origin owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_one);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 11. check new owner

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_three);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 12. unlock by origin owner

  ret = MTL(ObTableLockService*)->unlock_table(table_id,
                                               check_lock_mode,
                                               owner_one);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, replace_all_locks_with_dml_locks)
{

  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ACQUIRE_CONN_FROM_SQL_PROXY(conn1, sql_proxy);
  ACQUIRE_CONN_FROM_SQL_PROXY(conn2, sql_proxy);

  ObTxParam tx_param;
  share::ObTenantSwitchGuard tenant_guard;
  ObTxDesc *tx_desc1 = nullptr;
  ObTxDesc *tx_desc2= nullptr;
  ObTxDesc *tx_desc3= nullptr;
  ObTxDesc *tx_desc4= nullptr;
  ObTransService *txs = nullptr;
  uint64_t table_id = 0;
  ObTableLockMode check_lock_mode = EXCLUSIVE;
  ObTableLockMode new_lock_mode = SHARE;
  ObTableLockOwnerID owner_one(ObTableLockOwnerID::get_owner(0, 1));
  ObTableLockOwnerID owner_two(ObTableLockOwnerID::get_owner(0, 2));
  ObTableLockMode lock_mode_one = ROW_SHARE;
  ObTableLockMode lock_mode_two = ROW_EXCLUSIVE;
  ObLockTableRequest lock_arg;
  ObLockTableRequest new_lock_arg;
  ObUnLockTableRequest unlock_arg;
  ObUnLockTableRequest unlock_arg2;
  ObArenaAllocator allocator;
  ObReplaceAllLocksRequest replace_lock_arg(allocator);
  int64_t stmt_timeout_ts = -1;

  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 6000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);

  // 1. insert rows (get 2 RX IN_TRANS locks)


  OB_LOG(INFO, "insert 2 rows without commit");
  {
    WRITE_SQL_BY_CONN(conn1, "begin;");
    WRITE_SQL_BY_CONN(conn1, "INSERT INTO t_one_part VALUES (2, 2);");
    WRITE_SQL_BY_CONN(conn2, "begin;");
    WRITE_SQL_BY_CONN(conn2, "INSERT INTO t_one_part VALUES (3, 3);");
  }

  // 2. lock table with RX lock
  get_table_id("t_one_part", table_id);
  lock_arg.table_id_ = table_id;
  lock_arg.owner_id_ = owner_one;
  lock_arg.lock_mode_ = lock_mode_one;
  lock_arg.op_type_ = OUT_TRANS_LOCK;
  lock_arg.timeout_us_ = 0;

  unlock_arg.table_id_ = table_id;
  unlock_arg.owner_id_ = owner_one;
  unlock_arg.lock_mode_ = lock_mode_one;
  unlock_arg.op_type_ = OUT_TRANS_UNLOCK;
  unlock_arg.timeout_us_ = 0;

  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc1));
  ret = MTL(ObTableLockService*)->lock(*tx_desc1,
                                       tx_param,
                                       lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc1, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 3. check lock

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             check_lock_mode,
                                             owner_two);
  ASSERT_EQ(OB_EAGAIN, ret);

  // 4. replace lock

  new_lock_arg.table_id_ = table_id;
  new_lock_arg.owner_id_ = owner_two;
  new_lock_arg.lock_mode_ = EXCLUSIVE;
  new_lock_arg.op_type_ = OUT_TRANS_LOCK;
  new_lock_arg.timeout_us_ = 0;

  replace_lock_arg.lock_req_ = &new_lock_arg;
  replace_lock_arg.unlock_req_list_.push_back(&unlock_arg);

  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc2));
  ret = MTL(ObTableLockService*)->replace_lock(*tx_desc2,
                                               tx_param,
                                               replace_lock_arg);
  ASSERT_EQ(OB_TRANS_NEED_ROLLBACK, ret);

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->rollback_tx(*tx_desc2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 5. commit insert (release 2 RX IN_TRANS locks)

  {
    WRITE_SQL_BY_CONN(conn1, "commit;");
    WRITE_SQL_BY_CONN(conn2, "commit;");
  }

  // 6. replace again

  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc3));
  ret = MTL(ObTableLockService*)->replace_lock(*tx_desc3,
                                               tx_param,
                                               replace_lock_arg);
  ASSERT_EQ(OB_SUCCESS, ret);

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc3, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc3);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 7. check replace successfully

  ret = MTL(ObTableLockService*)->lock_table(table_id,
                                             EXCLUSIVE,
                                             owner_two);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 8. unlock

  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc4));
  unlock_arg2.table_id_ = table_id;
  unlock_arg2.owner_id_ = owner_two;
  unlock_arg2.lock_mode_ = EXCLUSIVE;
  unlock_arg2.op_type_ = OUT_TRANS_UNLOCK;
  unlock_arg2.timeout_us_ = 0;
  ret = MTL(ObTableLockService *)->unlock(*tx_desc4, tx_param, unlock_arg2);
  ASSERT_EQ(OB_SUCCESS, ret);

  stmt_timeout_ts = ObTimeUtility::current_time() + 1000 * 1000;
  ret = txs->commit_tx(*tx_desc4, stmt_timeout_ts);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = txs->release_tx(*tx_desc4);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTableLockServiceTest, retry_out_trans_with_4038)
{


  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  ACQUIRE_CONN_FROM_SQL_PROXY(conn1, sql_proxy);
  int ret = OB_SUCCESS;
  ObTxParam tx_param;
  share::ObTenantSwitchGuard tenant_guard;
  ObTxDesc *tx_desc1 = nullptr;
  ObTransService *txs = nullptr;
  ObLockID lock_id1;
  ObLockID lock_id2;
  int64_t obj_id1 = 1001;
  int64_t obj_id2 = 1002;

  lock_id1.set(ObLockOBJType::OBJ_TYPE_COMMON_OBJ, obj_id1);
  lock_id2.set(ObLockOBJType::OBJ_TYPE_COMMON_OBJ, obj_id2);

  ObTableLockOwnerID owner(ObTableLockOwnerID::get_owner(0, 1));
  ObTableLockMode lock_mode = EXCLUSIVE;

  ObLockObjsRequest lock_arg1;
  ObLockObjsRequest lock_arg2;
  int64_t stmt_timeout_ts = -1;

  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = 6000 * 1000L;
  tx_param.lock_timeout_us_ = -1;
  tx_param.cluster_id_ = GCONF.cluster_id;

  ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_NE(nullptr, MTL(ObTableLockService*));

  txs = MTL(ObTransService*);
  ASSERT_NE(nullptr, txs);
  ASSERT_EQ(OB_SUCCESS, txs->acquire_tx(tx_desc1));

  // 1. lock out_trans


  lock_arg1.objs_.push_back(lock_id1);
  lock_arg1.owner_id_ = owner;
  lock_arg1.lock_mode_ = lock_mode;
  lock_arg1.op_type_ = OUT_TRANS_LOCK;
  lock_arg1.timeout_us_ = 0;

  ret = MTL(ObTableLockService *)->lock(*tx_desc1, tx_param, lock_arg1);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 2. eanble errsim

  WRITE_SQL_BY_CONN(conn1, "alter system set_tp tp_name = 'EN_OB_NOT_MASTER_IN_TABLELOCK', error_code = 4038, frequency = 1;");

  // 3. lock out_trans with error -4038

  lock_arg2.objs_.push_back(lock_id2);
  lock_arg2.owner_id_ = owner;
  lock_arg2.lock_mode_ = lock_mode;
  lock_arg2.op_type_ = OUT_TRANS_LOCK;
  lock_arg2.timeout_us_ = 0;

  ret = MTL(ObTableLockService *)->lock(*tx_desc1, tx_param, lock_arg2);
  ASSERT_EQ(OB_TRANS_TIMEOUT, ret);

  // 4. try to commit trans

  ret = txs->commit_tx(*tx_desc1, stmt_timeout_ts);
  ASSERT_EQ(OB_TRANS_ROLLBACKED, ret);

  ret = txs->release_tx(*tx_desc1);
  ASSERT_EQ(OB_SUCCESS, ret);
}
} // end unittest
} // end oceanbase


int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(OB_LOG_LEVEL_INFO);
  OB_LOGGER.set_mod_log_levels("STORAGE.TABLELOCK:DEBUG");
  OB_LOGGER.set_enable_async_log(false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
