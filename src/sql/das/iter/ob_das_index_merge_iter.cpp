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

#include "lib/ob_errno.h"
#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_index_merge_iter.h"
#include "src/sql/das/ob_das_attach_define.h"
#include <algorithm>
#include <vector>
#include "storage/retrieval/ob_token_posting_list_cache.h"
#include "sql/das/ob_index_scan_cache.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASIndexMergeIter::IndexMergeRowStore::init(common::ObIAllocator &allocator,
                                                  const common::ObIArray<ObExpr*> *exprs,
                                                  ObEvalCtx *eval_ctx,
                                                  int64_t max_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exprs) || OB_ISNULL(eval_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr for init index merge row store", K(ret));
  } else if (OB_ISNULL(store_rows_ =
      static_cast<LastDASStoreRow*>(allocator.alloc(max_size * sizeof(LastDASStoreRow))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(max_size), K(ret));
  } else {
    exprs_ = exprs;
    eval_ctx_ = eval_ctx;
    max_size_ = max_size;
    for (int64_t i = 0; i < max_size_; i++) {
      new (store_rows_ + i) LastDASStoreRow(allocator);
      store_rows_[i].reuse_ = true;
    }
  }

  return ret;
}

int ObDASIndexMergeIter::IndexMergeRowStore::save(bool is_vectorized, int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(size > max_size_) || OB_ISNULL(store_rows_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error for save store rows", K(size), K_(max_size), K_(store_rows), K(ret));
  } else {
    if (is_vectorized) {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
      batch_info_guard.set_batch_size(size);
      for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
        batch_info_guard.set_batch_idx(i);
        if (OB_FAIL(store_rows_[i].save_store_row(*exprs_, *eval_ctx_))) {
          LOG_WARN("index merge iter failed to store rows", K(ret));
        }
      }
    } else if (OB_FAIL(store_rows_[0].save_store_row(*exprs_, *eval_ctx_))) {
      LOG_WARN("index merge iter failed to store rows", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    cur_idx_ = 0;
    saved_size_ = size;
  }

  return ret;
}

int ObDASIndexMergeIter::IndexMergeRowStore::to_expr()
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
  batch_info_guard.set_batch_size(1);
  batch_info_guard.set_batch_idx(0);
  // use deep copy to avoid storage layer sanity check
  if (OB_FAIL(store_rows_[cur_idx_].store_row_->to_expr<true>(*exprs_, *eval_ctx_))) {
    LOG_WARN("index merge iter failed to convert store row to expr", K(ret));
  } else {
    cur_idx_++;
  }
  return ret;
}

const ObDatum *ObDASIndexMergeIter::IndexMergeRowStore::cur_datums()
{
  const ObDatum *datums = nullptr;
  if (OB_LIKELY(cur_idx_ < saved_size_) && OB_NOT_NULL(store_rows_) && OB_NOT_NULL(store_rows_[cur_idx_].store_row_)) {
    datums = store_rows_[cur_idx_].store_row_->cells();
  }
  return datums;
}

void ObDASIndexMergeIter::IndexMergeRowStore::reuse()
{
  cur_idx_ = OB_INVALID_INDEX;
  saved_size_ = 0;
  iter_end_ = false;
  // Reset cache state (handle managed externally)
  cached_value_ = nullptr;
  cache_offset_ = 0;
  cache_total_count_ = 0;
  use_cached_data_ = false;
}

void ObDASIndexMergeIter::IndexMergeRowStore::reset()
{
  if (OB_NOT_NULL(store_rows_)) {
    for (int64_t i = 0; i < max_size_; i++) {
      store_rows_[i].~LastDASStoreRow();
    }
    store_rows_ = nullptr;
  }
  exprs_ = nullptr;
  eval_ctx_ = nullptr;
  max_size_ = 1;
  saved_size_ = 0;
  cur_idx_ = OB_INVALID_INDEX;
  iter_end_ = false;
  // Reset cache state (handle managed externally)
  cached_value_ = nullptr;
  cache_offset_ = 0;
  cache_total_count_ = 0;
  use_cached_data_ = false;
}

int ObDASIndexMergeIter::MergeResultBuffer::init(int64_t max_size,
                                                 ObEvalCtx *eval_ctx,
                                                 const common::ObIArray<ObExpr*> *exprs,
                                                 common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(max_size <= 0) || OB_ISNULL(eval_ctx) || OB_ISNULL(exprs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(max_size), K(eval_ctx), K(exprs), K(ret));
  } else if (OB_FAIL(result_store_.init(UINT64_MAX, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID, "DASIndexMerge"))) {
    LOG_WARN("failed to init result store", K(ret));
  } else {
    result_store_.set_allocator(alloc);
    max_size_ = max_size;
    eval_ctx_ = eval_ctx;
    exprs_ = exprs;
  }
  return ret;
}

int ObDASIndexMergeIter::MergeResultBuffer::add_row()
{
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
  batch_info_guard.set_batch_size(1);
  batch_info_guard.set_batch_idx(0);
  return result_store_.add_row(*exprs_, eval_ctx_);
}

int ObDASIndexMergeIter::MergeResultBuffer::to_expr(int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t read_size = 0;
  if (OB_FAIL(result_store_.begin(result_store_iter_))) {
    LOG_WARN("failed to begin iterate result store", K(ret));
  } else if (OB_UNLIKELY(!(result_store_iter_.is_valid() && result_store_iter_.has_next()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected no available rows", K(ret));
  } else if (OB_FAIL(result_store_iter_.get_next_batch<true>(*exprs_, *eval_ctx_, max_size_, read_size))) {
    LOG_WARN("failed to get next batch from result store", K(ret));
  } else if (OB_UNLIKELY(size != read_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected read size not equal to actually size", K(ret));
  }
  return ret;
}

void ObDASIndexMergeIter::MergeResultBuffer::reuse() { reset(); }

void ObDASIndexMergeIter::MergeResultBuffer::reset()
{
  result_store_iter_.reset();
  result_store_.reset();
}

int ObDASIndexMergeIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (ObDASIterType::DAS_ITER_INDEX_MERGE != param.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner init das iter with bad param type", K(param));
  } else {
    ObDASIndexMergeIterParam &index_merge_param = static_cast<ObDASIndexMergeIterParam&>(param);
    merge_type_ = index_merge_param.merge_type_;
    rowkey_exprs_ = index_merge_param.rowkey_exprs_;
    get_next_row_ = (merge_type_ == INDEX_MERGE_UNION) ?
        &ObDASIndexMergeIter::union_get_next_row : &ObDASIndexMergeIter::intersect_get_next_row;
    get_next_rows_ = (merge_type_ == INDEX_MERGE_UNION) ?
        &ObDASIndexMergeIter::union_get_next_rows : &ObDASIndexMergeIter::intersect_get_next_rows;
    merge_ctdef_ = index_merge_param.ctdef_;
    merge_rtdef_ = index_merge_param.rtdef_;
    tx_desc_ = index_merge_param.tx_desc_;
    snapshot_ = index_merge_param.snapshot_;
    is_reverse_ = index_merge_param.is_reverse_;
    
    LOG_INFO("[INDEX_MERGE_EXEC] ObDASIndexMergeIter::inner_init CALLED - INDEX MERGE EXECUTION STARTED",
             K(merge_type_), 
             "is_intersect", (merge_type_ == INDEX_MERGE_INTERSECT),
             "is_topk_target", (merge_ctdef_ != nullptr ? merge_ctdef_->is_topk_target_ : false),
             "limit_expr", (merge_ctdef_ != nullptr ? merge_ctdef_->limit_expr_ : nullptr),
             "offset_expr", (merge_ctdef_ != nullptr ? merge_ctdef_->offset_expr_ : nullptr),
             "child_count", index_merge_param.child_iters_->count(),
             "rowkey_exprs_count", rowkey_exprs_ != nullptr ? rowkey_exprs_->count() : 0);
    
    lib::ContextParam context_param;
    context_param.set_mem_attr(MTL_ID(), "DASIndexMerge", ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
    // Support both UNION and INTERSECT merge types
    if (OB_UNLIKELY(merge_type_ != INDEX_MERGE_UNION && merge_type_ != INDEX_MERGE_INTERSECT)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid merge type", K(merge_type_));
    } else if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_ctx_, context_param))) {
      LOG_WARN("failed to create index merge memctx", K(ret));
    } else {
      common::ObArenaAllocator &alloc = mem_ctx_->get_arena_allocator();
      child_iters_.set_allocator(&alloc);
      child_stores_.set_allocator(&alloc);
      child_scan_rtdefs_.set_allocator(&alloc);
      child_scan_params_.set_allocator(&alloc);
      child_tablet_ids_.set_allocator(&alloc);
      child_match_against_exprs_.set_allocator(&alloc);
      int64_t child_cnt = index_merge_param.child_iters_->count();
      if (OB_FAIL(child_iters_.assign(*index_merge_param.child_iters_))) {
        LOG_WARN("failed to assign child iters", K(ret));
      } else if (OB_FAIL(child_scan_rtdefs_.assign(*index_merge_param.child_scan_rtdefs_))) {
        LOG_WARN("failed to assign child scan rtdefs", K(ret));
      } else if (OB_FAIL(child_stores_.prepare_allocate(child_cnt))) {
        LOG_WARN("failed to prepare allocate child stores", K(ret));
      } else if (OB_FAIL(child_scan_params_.prepare_allocate(child_cnt))) {
        LOG_WARN("failed to prepare allocate child scan params", K(ret));
      } else if (OB_FAIL(child_tablet_ids_.prepare_allocate(child_cnt))) {
        LOG_WARN("failed to prepare allocate child tablet ids", K(ret));
      } else if (OB_FAIL(child_match_against_exprs_.prepare_allocate(child_cnt))) {
        LOG_WARN("failed to prepare allocate child match against exprs", K(ret));
      } else {
        ObArray<ObExpr*> match_against_exprs;
        for (int64_t i = 0; OB_SUCC(ret) && i < child_cnt; i++) {
          ObDASIter *child = child_iters_.at(i);
          IndexMergeRowStore &row_store = child_stores_.at(i);
          ObDASScanRtDef *child_scan_rtdef = child_scan_rtdefs_.at(i);
          ObTableScanParam *&child_scan_param = child_scan_params_.at(i);
          ExprFixedArray &child_match_against_expr = child_match_against_exprs_.at(i);
          if (OB_ISNULL(child) || OB_ISNULL(child->get_output())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid child iter", K(i), K(child), K(ret));
          } else if (OB_FAIL(row_store.init(alloc, child->get_output(), eval_ctx_, max_size_))) {
            LOG_WARN("failed to init row store", K(ret));
          } else if (OB_FAIL(extract_match_against_exprs(*child->get_output(), match_against_exprs))) {
            LOG_WARN("failed to extract match against exprs", K(ret));
          } else if (FALSE_IT(child_match_against_expr.set_allocator(&alloc))) {
          } else if (OB_FAIL(child_match_against_expr.assign(match_against_exprs))) {
            LOG_WARN("failed to assign match against exprs", K(ret));
          } else if (child_scan_rtdef != nullptr) {
            // need to prepare scan param for normal scan node
            if (OB_ISNULL(child_scan_param = OB_NEWx(ObTableScanParam, &alloc))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate child scan param", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(result_buffer_.init(max_size_, eval_ctx_, output_, mem_ctx_->get_malloc_allocator()))) {
            LOG_WARN("failed to init merge result buffer", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDASIndexMergeIter::init_scan_param(const share::ObLSID &ls_id,
                                         const common::ObTabletID &tablet_id,
                                         const sql::ObDASScanCtDef *ctdef,
                                         sql::ObDASScanRtDef *rtdef,
                                         ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctdef) || OB_ISNULL(rtdef)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(ctdef), KPC(rtdef), K(ls_id), K(tablet_id));
  } else {
    uint64_t tenant_id = MTL_ID();
    scan_param.tenant_id_ = tenant_id;
    scan_param.key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamKR"));
    scan_param.ss_key_ranges_.set_attr(ObMemAttr(tenant_id, "ScanParamSSKR"));
    scan_param.tx_lock_timeout_ = rtdef->tx_lock_timeout_;
    scan_param.index_id_ = ctdef->ref_table_id_;
    scan_param.is_get_ = ctdef->is_get_;
    scan_param.is_for_foreign_check_ = false;
    scan_param.timeout_ = rtdef->timeout_ts_;
    scan_param.scan_flag_ = rtdef->scan_flag_;
    scan_param.reserved_cell_count_ = ctdef->access_column_ids_.count();
    scan_param.allocator_ = &rtdef->stmt_allocator_;
    scan_param.scan_allocator_ = &rtdef->scan_allocator_;
    scan_param.sql_mode_ = rtdef->sql_mode_;
    scan_param.frozen_version_ = rtdef->frozen_version_;
    scan_param.force_refresh_lc_ = rtdef->force_refresh_lc_;
    scan_param.output_exprs_ = &(ctdef->pd_expr_spec_.access_exprs_);
    scan_param.calc_exprs_ = &(ctdef->pd_expr_spec_.calc_exprs_);
    scan_param.aggregate_exprs_ = &(ctdef->pd_expr_spec_.pd_storage_aggregate_output_);
    scan_param.table_param_ = &(ctdef->table_param_);
    scan_param.op_ = rtdef->p_pd_expr_op_;
    scan_param.row2exprs_projector_ = rtdef->p_row2exprs_projector_;
    scan_param.schema_version_ = ctdef->schema_version_;
    scan_param.tenant_schema_version_ = rtdef->tenant_schema_version_;
    scan_param.limit_param_ = rtdef->limit_param_;
    scan_param.need_scn_ = rtdef->need_scn_;
    scan_param.pd_storage_flag_ = ctdef->pd_expr_spec_.pd_storage_flag_.pd_flag_;
    scan_param.fb_snapshot_ = rtdef->fb_snapshot_;
    scan_param.fb_read_tx_uncommitted_ = rtdef->fb_read_tx_uncommitted_;
    scan_param.ls_id_ = ls_id;
    scan_param.tablet_id_ = tablet_id;
    if (!ctdef->pd_expr_spec_.pushdown_filters_.empty()) {
      scan_param.op_filters_ = &ctdef->pd_expr_spec_.pushdown_filters_;
    }
    scan_param.pd_storage_filters_ = rtdef->p_pd_expr_op_->pd_storage_filters_;
    if (OB_NOT_NULL(tx_desc_)) {
      scan_param.tx_id_ = tx_desc_->get_tx_id();
    } else {
      scan_param.tx_id_.reset();
    }

    if (OB_NOT_NULL(snapshot_)) {
      if (OB_FAIL(scan_param.snapshot_.assign(*snapshot_))) {
        LOG_WARN("assign snapshot fail", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null snapshot", K(ret), KPC_(snapshot));
    }

    if (FAILEDx(scan_param.column_ids_.assign(ctdef->access_column_ids_))) {
      LOG_WARN("failed to init column ids", K(ret));
    } else if (OB_FAIL(prepare_scan_ranges(scan_param, rtdef))) {
      LOG_WARN("failed to prepare scan ranges", K(ret));
    }
  }
  return ret;
}

int ObDASIndexMergeIter::prepare_scan_ranges(ObTableScanParam &scan_param, const ObDASScanRtDef *rtdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rtdef)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr scan rtdef", K(ret));
  } else if (OB_FAIL(scan_param.key_ranges_.assign(rtdef->key_ranges_))) {
    LOG_WARN("failed to assign key ranges", K(ret));
  } else if (OB_FAIL(scan_param.ss_key_ranges_.assign(rtdef->ss_key_ranges_))) {
    LOG_WARN("failed to assign ss key ranges", K(ret));
  } else if (OB_FAIL(scan_param.mbr_filters_.assign(rtdef->mbr_filters_))) {
    LOG_WARN("failed to assign mbr filters", K(ret));
  }

  LOG_TRACE("index merge iter prepare scan ranges", K(scan_param), KPC(rtdef), K(ret));
  return ret;
}

void ObDASIndexMergeIter::reset_datum_ptr(const common::ObIArray<ObExpr*> *exprs, int64_t size)
{
  if (OB_NOT_NULL(exprs) && size > 0) {
    for (int64_t i = 0; i < exprs->count(); i++) {
      ObExpr *expr = exprs->at(i);
      if (OB_NOT_NULL(expr)) {
        expr->locate_datums_for_update(*eval_ctx_, size);
        ObEvalInfo &info = expr->get_eval_info(*eval_ctx_);
        info.point_to_frame_ = true;
      }
    }
  }
}

int ObDASIndexMergeIter::extract_match_against_exprs(const common::ObIArray<ObExpr*> &exprs, 
                                                     common::ObIArray<ObExpr*> &match_against_exprs)
{
  int ret = OB_SUCCESS;
  match_against_exprs.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObExpr *expr = exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr expr", K(ret));
    } else if (T_FUN_MATCH_AGAINST == expr->type_) {
      if (OB_FAIL(match_against_exprs.push_back(expr))) {
        LOG_WARN("failed to push back match against expr", K(ret));
      }
    }
  }
  return ret;
}

int ObDASIndexMergeIter::fill_default_values_for_union(const common::ObIArray<ObExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObExpr *expr = exprs.at(i);
    if (OB_ISNULL(expr) || OB_UNLIKELY(T_FUN_MATCH_AGAINST != expr->type_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid expr to fill default value", KP(expr), K(ret));
    } else {
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
      batch_info_guard.set_batch_size(1);
      batch_info_guard.set_batch_idx(0);
      ObDatum &datum = expr->locate_datum_for_write(*eval_ctx_);
      datum.set_double(0.0);
      expr->set_evaluated_flag(*eval_ctx_);
      expr->set_evaluated_projected(*eval_ctx_);
    }
  }
  return ret;
}

int ObDASIndexMergeIter::save_row_to_result_buffer()
{
  return result_buffer_.add_row();
}

int ObDASIndexMergeIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_scan_rtdefs_.count(); ++i) {
    ObDASScanRtDef *scan_rtdef = child_scan_rtdefs_.at(i);
    ObDASIter *iter = child_iters_.at(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr iter", K(ret));
    } else if (scan_rtdef != nullptr) {
      const ObDASScanCtDef *scan_ctdef = static_cast<const ObDASScanCtDef*>(scan_rtdef->ctdef_);
      ObTableScanParam *scan_param = child_scan_params_.at(i);
      if (OB_ISNULL(scan_ctdef) || OB_ISNULL(scan_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr child scan info", K(scan_ctdef), K(scan_param), K(ret));
      } else if (OB_FAIL(init_scan_param(ls_id_, child_tablet_ids_.at(i), scan_ctdef, scan_rtdef, *scan_param))) {
        LOG_WARN("failed to init child scan param", K(ret));
      } else if (OB_FAIL(iter->do_table_scan())) {
        LOG_WARN("child iter failed to do table scan", K(ret));
      }
    } else if (OB_FAIL(iter->do_table_scan())) {
      LOG_WARN("child iter failed to do table scan", K(ret));
    }
  }
  return ret;
}

int ObDASIndexMergeIter::rescan()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_scan_rtdefs_.count(); ++i) {
    ObDASScanRtDef *scan_rtdef = child_scan_rtdefs_.at(i);
    ObDASIter *iter = child_iters_.at(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr iter", K(ret));
    } else if (scan_rtdef != nullptr) {
      const ObDASScanCtDef *scan_ctdef = static_cast<const ObDASScanCtDef*>(scan_rtdef->ctdef_);
      ObTableScanParam *scan_param = child_scan_params_.at(i);
      if (OB_ISNULL(scan_ctdef) || OB_ISNULL(scan_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr child scan info", K(scan_ctdef), K(scan_param), K(ret));
      } else {
        scan_param->tablet_id_ = child_tablet_ids_.at(scan_ctdef->index_merge_idx_);
        scan_param->ls_id_ = ls_id_;
        if (OB_FAIL(prepare_scan_ranges(*scan_param, scan_rtdef))) {
          LOG_WARN("failed to prepare scan ranges", K(ret));
        } else if (OB_FAIL(iter->rescan())) {
          LOG_WARN("child iter failed to rescan", K(ret));
        }
      }
    } else if (OB_FAIL(iter->rescan())) {
      LOG_WARN("child iter failed to rescan", K(ret));
    }
  }
  return ret;
}

void ObDASIndexMergeIter::clear_evaluated_flag()
{
  for (int64_t i = 0; i < child_iters_.count(); ++i) {
    if (OB_NOT_NULL(child_iters_.at(i))) {
      child_iters_.at(i)->clear_evaluated_flag();
    }
  }
}

int ObDASIndexMergeIter::set_ls_tablet_ids(const ObLSID &ls_id, const ObDASRelatedTabletID &related_tablet_ids)
{
  int ret = OB_SUCCESS;
  ls_id_ = ls_id;
  const ObIArray<ObTabletID> &index_merge_tablet_ids = related_tablet_ids.index_merge_tablet_ids_;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_scan_rtdefs_.count(); ++i) {
    ObDASScanRtDef *scan_rtdef = child_scan_rtdefs_.at(i);
    if (scan_rtdef != nullptr) {
      const ObDASScanCtDef *scan_ctdef = static_cast<const ObDASScanCtDef*>(scan_rtdef->ctdef_);
      if (OB_ISNULL(scan_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr scan ctdef", K(ret));
      } else {
        child_tablet_ids_.at(i) = index_merge_tablet_ids.at(scan_ctdef->index_merge_idx_);
      }
    }
  }
  return ret;
}

int ObDASIndexMergeIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_scan_rtdefs_.count(); ++i) {
    ObDASScanRtDef *scan_rtdef = child_scan_rtdefs_.at(i);
    ObDASIter *iter = child_iters_.at(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr iter", K(ret));
    } else if (scan_rtdef != nullptr) {
      const ObDASScanCtDef *scan_ctdef = static_cast<const ObDASScanCtDef*>(scan_rtdef->ctdef_);
      ObTableScanParam *scan_param = child_scan_params_.at(i);
      if (OB_ISNULL(scan_ctdef) || OB_ISNULL(scan_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr child scan info", K(scan_ctdef), K(scan_param), K(ret));
      } else {
        const ObTabletID &new_tablet_id = child_tablet_ids_.at(scan_ctdef->index_merge_idx_);
        const ObTabletID &old_tablet_id = scan_param->tablet_id_;
        scan_param->need_switch_param_ = scan_param->need_switch_param_ ||
            (old_tablet_id.is_valid() && old_tablet_id != new_tablet_id);
        scan_param->key_ranges_.reuse();
        scan_param->ss_key_ranges_.reuse();
        scan_param->mbr_filters_.reuse();
        if (OB_FAIL(iter->reuse())) {
          LOG_WARN("child iter failed to reuse", K(ret));
        }
      }
    } else if (OB_FAIL(iter->reuse())) {
      LOG_WARN("child iter failed to reuse", K(ret));
    }
  }
  for (int64_t i = 0; i < child_stores_.count(); ++i) {
    child_stores_.at(i).reuse();
  }
  result_buffer_.reuse();
  return ret;
}

int ObDASIndexMergeIter::inner_release()
{

  int ret = OB_SUCCESS;
  child_iters_.reset();
  child_scan_rtdefs_.reset();
  child_tablet_ids_.reset();
  for (int64_t i = 0; i < child_stores_.count(); ++i) {
    child_stores_.at(i).reset();
  }
  child_stores_.reset();
  for (int64_t i = 0; i < child_scan_params_.count(); ++i) {
    ObTableScanParam *scan_param = child_scan_params_.at(i);
    if (scan_param != nullptr) {
      scan_param->destroy_schema_guard();
      scan_param->snapshot_.reset();
      scan_param->destroy();
    }
  }
  for (int64_t i = 0; i < child_match_against_exprs_.count(); ++i) {
    child_match_against_exprs_.at(i).reset();
  }
  child_match_against_exprs_.reset();
  result_buffer_.reset();
  if (OB_NOT_NULL(mem_ctx_)) {
    mem_ctx_->reset_remain_one_page();
    DESTROY_CONTEXT(mem_ctx_);
    mem_ctx_ = nullptr;
  }
  return ret;
}

int ObDASIndexMergeIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (OB_FAIL((this->*get_next_row_)())) {
    if (ret != OB_ITER_END) {
      LOG_WARN("index merge iter failed to get next row", K(ret));
    }
  }
  return ret;
}

int ObDASIndexMergeIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  int64_t before_count = count;
  if (OB_FAIL((this->*get_next_rows_)(count, capacity))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("index merge iter failed to get next rows", K(ret));
    }
  }
  // 打印本次返回的行数
  // LOG_INFO("[INDEX_MERGE_DEBUG] inner_get_next_rows DONE",
  //          K(before_count), K(count), K(capacity), K(ret),
  //          "rows_returned", count - before_count);
  // LOG_TRACE("[DAS ITER] index merge iter get next rows", K(count), K(capacity), K(ret));
  const ObBitVector *skip = nullptr;
  // PRINT_VECTORIZED_ROWS(SQL, DEBUG, *eval_ctx_, *output_, count, skip);

  return ret;
}

int ObDASIndexMergeIter::compare(int64_t cur_idx, int64_t &output_idx, int &cmp_ret)
{
  int ret = OB_SUCCESS;
  cmp_ret = 0;
  if (cur_idx == OB_INVALID_INDEX || !child_stores_.at(cur_idx).have_data()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", K(cur_idx), K(ret));
  } else if (output_idx == OB_INVALID_INDEX) {
    output_idx = cur_idx;
  } else {
    const ObDatum *cur_datums = child_stores_.at(cur_idx).cur_datums();
    const ObDatum *output_datums = child_stores_.at(output_idx).cur_datums();
    if (OB_ISNULL(cur_datums) || OB_ISNULL(output_datums)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(cur_datums), K(output_datums));
    }
    ObObj cur_obj;
    ObObj output_obj;
    for (int64_t i = 0; (cmp_ret == 0) && OB_SUCC(ret) && i < rowkey_exprs_->count(); i++) {
      const ObExpr *expr = rowkey_exprs_->at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (OB_FAIL(cur_datums[i].to_obj(cur_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("failed to convert left datum to obj", K(i), KPC(expr), K(ret));
      } else if (OB_FAIL(output_datums[i].to_obj(output_obj, expr->obj_meta_, expr->obj_datum_map_))) {
        LOG_WARN("failed to convert right datum to obj", K(i), KPC(expr), K(ret));
      } else if (OB_FAIL(cur_obj.check_collation_free_and_compare(output_obj, cmp_ret))) {
          LOG_WARN("failed to compare cur obj with output obj", K(ret));
      } else if (cmp_ret != 0) {
        if (cmp_ret < 0) {
          output_idx = OB_UNLIKELY(is_reverse_) ? output_idx : cur_idx;
        } else {
          output_idx = OB_UNLIKELY(is_reverse_) ? cur_idx : output_idx;
        }
      }
    }
  }
  return ret;
}

int ObDASIndexMergeIter::intersect_get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  static int64_t call_count = 0;
  call_count++;
  if (call_count <= 10 || call_count % 1000 == 0) {
    // LOG_INFO("[INDEX_MERGE_EXEC] intersect_get_next_row CALLED",
    //          K(call_count), K(child_stores_.count()));
    // 打印每个子迭代器的类型
    for (int64_t i = 0; i < child_iters_.count(); ++i) {
      if (OB_NOT_NULL(child_iters_.at(i))) {
        // LOG_INFO("[INDEX_MERGE_EXEC] child iter type",
        //          K(i), "type", child_iters_.at(i)->get_type());
      }
    }
  }
  while (OB_SUCC(ret) && !got_row) {
      /* try to fill each child store */
    int64_t output_idx = OB_INVALID_INDEX;
    int cmp_ret = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stores_.count(); i++) {
      IndexMergeRowStore &child_store = child_stores_.at(i);
      if (!child_store.have_data()) {
        if (!child_store.iter_end_) {
          ObDASIter *child_iter = child_iters_.at(i);
          if (OB_ISNULL(child_iter)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr", K(i));
          } else if (OB_FAIL(child_iter->get_next_row())) {
            if (OB_ITER_END == ret) {
              child_store.iter_end_ = true;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to get next row from child iter", K(ret));
            }
          } else if (OB_FAIL(child_store.save(false, 1))) {
            LOG_WARN("failed to save child row", K(ret));
          } else {
            // DEBUG: 打印子迭代器返回的行的 rowkey
            const ObDatum *child_datums = child_store.cur_datums();
            if (child_datums != nullptr && rowkey_exprs_ != nullptr && rowkey_exprs_->count() > 0) {
              int64_t rowkey_val = child_datums[0].get_int();
              // LOG_INFO("[INDEX_MERGE_DEBUG] Child iter returned row",
              //          K(i), K(rowkey_val), "datum", child_datums[0]);
            }
            if (OB_FAIL(compare(i, output_idx, cmp_ret))) {
              LOG_WARN("index merge failed to compare row", K(i), K(output_idx), K(ret));
            }
          }
        }
      } else if (OB_FAIL(compare(i, output_idx, cmp_ret))) {
        LOG_WARN("index merge failed to compare row", K(i), K(output_idx), K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (output_idx == OB_INVALID_INDEX) {
      /* no available row, stop the entire process */
      ret = OB_ITER_END;
    } else {
      /* find all available rows */
      bool all_matched = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stores_.count(); i++) {
        if (output_idx == i) {
          /* skip */
        } else if (child_stores_.at(i).iter_end_ || !child_stores_.at(i).have_data()) {
          ret = OB_ITER_END;
        } else if (OB_FAIL(compare(i, output_idx, cmp_ret))) {
            LOG_WARN("index merge failed to compare row", K(i), K(output_idx), K(ret));
        } else if (cmp_ret == 0) {
          // DEBUG: 打印匹配的行
          const ObDatum *match_datums = child_stores_.at(i).cur_datums();
          const ObDatum *output_datums = child_stores_.at(output_idx).cur_datums();
          if (match_datums != nullptr && output_datums != nullptr) {
            // LOG_INFO("[INDEX_MERGE_DEBUG] Compare matched",
            //          K(i), K(output_idx), K(cmp_ret),
            //          "child_i_rowkey", match_datums[0].get_int(),
            //          "output_rowkey", output_datums[0].get_int());
          }
          // FIXME: only one store will project data in intersect get next row(s).
          child_stores_.at(i).cur_idx_++;
        } else {
          // DEBUG: 打印不匹配的行
          const ObDatum *nomatch_datums = child_stores_.at(i).cur_datums();
          const ObDatum *output_datums = child_stores_.at(output_idx).cur_datums();
          if (nomatch_datums != nullptr && output_datums != nullptr) {
            // LOG_INFO("[INDEX_MERGE_DEBUG] Compare NOT matched",
            //          K(i), K(output_idx), K(cmp_ret),
            //          "child_i_rowkey", nomatch_datums[0].get_int(),
            //          "output_rowkey", output_datums[0].get_int());
          }
          all_matched = false;
        }
      }
      if (OB_SUCC(ret)) {
        if (all_matched) {
          /* found available row in each child store, output */
          // 打印当前输出行的 rowkey 信息
          const ObDatum *out_datums = child_stores_.at(output_idx).cur_datums();
          static int64_t row_output_count = 0;
          if (out_datums != nullptr && rowkey_exprs_ != nullptr && rowkey_exprs_->count() > 0) {
            row_output_count++;
            // 将 rowkey 转换成可读的整数值
            int64_t rowkey_int = 0;
            if (out_datums[0].len_ > 0) {
              rowkey_int = out_datums[0].get_int();
            }
            // LOG_INFO("[INDEX_MERGE_DEBUG] OUTPUT ROW (row version)",
            //          K(row_output_count),
            //          K(output_idx),
            //          K(rowkey_int),
            //          "child0_cur_idx", child_stores_.at(0).cur_idx_,
            //          "child0_saved", child_stores_.at(0).saved_size_,
            //          "child1_cur_idx", child_stores_.count() > 1 ? child_stores_.at(1).cur_idx_ : -1,
            //          "child1_saved", child_stores_.count() > 1 ? child_stores_.at(1).saved_size_ : -1,
            //          "rowkey_datum0", out_datums[0]);
          }
          if (OB_FAIL(child_stores_.at(output_idx).to_expr())) {
            LOG_WARN("index merge failed to convert row to expr", K(ret));
          } else {
            got_row = true;
            // 注意：to_expr() 内部已经做了 cur_idx_++，这里不要再加！
          }
        } else {
          child_stores_.at(output_idx).cur_idx_++;
        }
      }
    }
  }

  return ret;
}

int ObDASIndexMergeIter::intersect_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  bool got_row = false;
  static int64_t total_output_count = 0;
  result_buffer_.reuse();
  // 用于存储每一行的 StoredRow 和 relevance score
  struct RowWithScore {
    ObChunkDatumStore::StoredRow *stored_row;  // 指向存储的行
    double relevance_score;
    
    RowWithScore() : stored_row(nullptr), relevance_score(0.0) {}
  };

  // 用来存储所有符合条件的行
  std::vector<RowWithScore> all_rows;
  ObArenaAllocator temp_allocator("TempRowStore");  // 临时分配器


  // LOG_INFO("[INDEX_MERGE_DEBUG] intersect_get_next_rows ENTER", K(count), K(capacity));
  while (OB_SUCC(ret) && count < capacity) {
      /* try to fill each child store */
    int64_t output_idx = OB_INVALID_INDEX;
    int cmp_ret = 0;
    int64_t child_rows_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stores_.count(); i++) {
      IndexMergeRowStore &child_store = child_stores_.at(i);
      if (!child_store.have_data()) {
        if (!child_store.iter_end_) {
          ObDASIter *child_iter = child_iters_.at(i);
          if (OB_ISNULL(child_iter)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr", K(i));
          } else {
            // Check if this is a non-FTS iterator that can use caching
            ObDASIterType iter_type = child_iter->get_type();
            bool is_fts_iter = (iter_type == DAS_ITER_TEXT_RETRIEVAL 
                                || iter_type == DAS_ITER_TEXT_RETRIEVAL_MERGE);
            ObTableScanParam *scan_param = child_scan_params_.at(i);
            bool cache_hit = false;
            const ObIndexScanCacheValue *cached_value = nullptr;
            common::ObKVCacheHandle cache_handle;
            ObIndexScanCacheKey cache_key;
            
            // Try cache for non-FTS iterators with valid scan_param
            if (!is_fts_iter && scan_param != nullptr) {
              // Build cache key
              cache_key = ObIndexScanCacheKey(
                  scan_param->tenant_id_,
                  scan_param->index_id_,
                  scan_param->tablet_id_);
              if (scan_param->key_ranges_.count() > 0) {
                cache_key.set_key_ranges(scan_param->key_ranges_);
              }
              
              // Try to get from cache (or continue from previous position)
              common::ObKVCacheHandle temp_handle;  // Temporary handle for this batch
              int cache_ret = ObIndexScanCache::get_instance().get_cached_rows(
                  cache_key, cached_value, temp_handle);
              
              if (OB_SUCCESS == cache_ret && cached_value != nullptr && cached_value->rowkey_count() > 0) {
                // Determine batch to read
                int64_t cache_offset = child_store.use_cached_data_ ? child_store.cache_offset_ : 0;
                int64_t total_cached = cached_value->rowkey_count();
                int64_t remaining = total_cached - cache_offset;
                
                if (remaining > 0) {
                  cache_hit = true;
                  child_rows_cnt = std::min(remaining, child_store.max_size_);
                  
                  // Mark that we're using cached data
                  if (!child_store.use_cached_data_) {
                    child_store.use_cached_data_ = true;
                    child_store.cache_offset_ = 0;
                    child_store.cache_total_count_ = total_cached;
                    LOG_INFO("[INDEX_SCAN_CACHE] Cache HIT (first batch)", K(i),
                             "batch_size", child_rows_cnt, "total", total_cached);
                  } else {
                    LOG_INFO("[INDEX_SCAN_CACHE] Cache HIT (continued)", K(i),
                             "batch_size", child_rows_cnt, "offset", cache_offset, "total", total_cached);
                  }
                }
                else if(remaining == 0 && child_store.use_cached_data_){
                  ret = OB_ITER_END;
                  LOG_INFO("[INDEX_SCAN_CACHE] Cache HIT (end)", K(i),
                           "batch_size", child_rows_cnt, "offset", cache_offset, "total", total_cached);
                }
              }
              
              
              // Populate child_store from current batch of cached rowkeys
              if (OB_SUCC(ret) &&cache_hit && cached_value != nullptr) {
                if (child_store.store_rows_ != nullptr && child_rows_cnt > 0) {
                  const int64_t *cached_rowkeys = cached_value->get_rowkeys();
                  int64_t total_cached = cached_value->rowkey_count();
                  int64_t actual_filled = 0;  // Track actual number of rows filled
                  
                  for (int64_t row_idx = 0; row_idx < child_rows_cnt && OB_SUCC(ret); ++row_idx) {
                    int64_t cache_row_idx = child_store.cache_offset_ + row_idx;
                    if (cache_row_idx < total_cached) {
                      int64_t rowkey = cached_rowkeys[cache_row_idx];
                      
                      // Build minimal StoredRow with single int64 column (rowkey)
                      // Layout: [StoredRow header][ObDatum][int64 data]
                      int64_t row_size = sizeof(ObChunkDatumStore::StoredRow) + sizeof(ObDatum) + sizeof(int64_t);
                      char *row_buf = static_cast<char*>(mem_ctx_->get_arena_allocator().alloc(row_size));
                      
                      if (OB_ISNULL(row_buf)) {
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                        LOG_WARN("failed to alloc memory for cached row", K(ret), K(row_size));
                      } else {
                        // Build StoredRow
                        ObChunkDatumStore::StoredRow *stored_row = 
                            reinterpret_cast<ObChunkDatumStore::StoredRow*>(row_buf);
                        stored_row->cnt_ = 1;  // Only rowkey column
                        stored_row->row_size_ = row_size;
                        
                        // Setup ObDatum
                        ObDatum *cell = stored_row->cells();
                        int64_t *data_ptr = reinterpret_cast<int64_t*>(row_buf + sizeof(ObChunkDatumStore::StoredRow) + sizeof(ObDatum));
                        *data_ptr = rowkey;  // Write rowkey value
                        
                        cell->ptr_ = reinterpret_cast<const char*>(data_ptr);
                        cell->len_ = sizeof(int64_t);
                        cell->pack_ = 0;
                        cell->null_ = 0;
                        
                        child_store.store_rows_[row_idx].store_row_ = stored_row;
                        actual_filled++;  // Increment actual filled count
                      }
                    } else {
                      // Reached end of cached data, stop filling
                      break;
                    }
                  }
                  
                  if (OB_SUCC(ret)) {
                    child_store.cur_idx_ = 0;
                    child_store.saved_size_ = actual_filled;  // Use actual filled count, not child_rows_cnt!
                    // Update cache offset
                    child_store.cache_offset_ += actual_filled;
                    
                    // Log cache read progress (don't set iter_end here - data hasn't been consumed yet!)
                    LOG_INFO("[INDEX_SCAN_CACHE] Batch filled from cache", K(i), K(actual_filled),
                             "offset", child_store.cache_offset_, "total", child_store.cache_total_count_);
                  }
                } else {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("store_rows_ is null or no data to read", 
                           K(child_store.store_rows_), K(child_rows_cnt));
                }
              }
            }
            
            // If cache miss, call the iterator normally
            if (OB_SUCC(ret) && !cache_hit) {
              ret = child_iter->get_next_rows(child_rows_cnt, capacity);
              if (OB_ITER_END == ret && child_rows_cnt > 0) {
                ret = OB_SUCCESS;
              }
              if (OB_SUCC(ret)) {
                if (OB_FAIL(child_store.save(true, child_rows_cnt))) {
                  LOG_WARN("failed to save child rows", K(child_rows_cnt), K(ret));
                } else {
                  // Cache the rows for non-FTS iterators (rowkey-only format)
                  if (!is_fts_iter && scan_param != nullptr && child_store.store_rows_ != nullptr) {
                    for (int64_t row_idx = 0; row_idx < child_rows_cnt; ++row_idx) {
                      if (child_store.store_rows_[row_idx].store_row_ != nullptr) {
                        int cache_ret = ObIndexScanCache::get_instance().append_row_to_pending(
                            cache_key, child_store.store_rows_[row_idx].store_row_);
                        if (cache_ret != OB_SUCCESS) {
                          LOG_WARN("failed to cache row", K(cache_ret), K(row_idx));
                        }
                      }
                    }
                  }
                  // Debug: check for consecutive duplicate rowkeys in child batch
                  if (rowkey_exprs_ != nullptr && rowkey_exprs_->count() > 0 && i == 0) {
                    // Only check FTS child (child 0) for duplicates
                    int64_t prev_int = -1;
                    for (int64_t row_idx = child_store.cur_idx_; row_idx < child_store.saved_size_; row_idx++) {
                      if (child_store.store_rows_ != nullptr && 
                          child_store.store_rows_[row_idx].store_row_ != nullptr) {
                        const ObDatum *datums = child_store.store_rows_[row_idx].store_row_->cells();
                        if (datums != nullptr) {
                          int64_t cur_int = datums[0].get_int();
                          if (row_idx > child_store.cur_idx_ && cur_int == prev_int) {
                            // Duplicate detected
                          }
                          prev_int = cur_int;
                        }
                      }
                    }
                  }
                  // Compare call is INSIDE save success block (like original code)
                  if (OB_FAIL(compare(i, output_idx, cmp_ret))) {
                    LOG_WARN("index merge failed to compare row", K(i), K(output_idx), K(ret));
                  } else if (child_iter->get_type() == DAS_ITER_SORT) {
                    reset_datum_ptr(child_iter->get_output(), child_rows_cnt);
                  }
                }
              } else if (OB_ITER_END == ret) {
                child_store.iter_end_ = true;
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("failed to get next rows from child iter", K(ret));
              }
            } else {
              // Cache hit path - also need to call compare
              if (OB_SUCC(ret)) {
                if (OB_FAIL(compare(i, output_idx, cmp_ret))) {
                  LOG_WARN("index merge failed to compare row (cache hit)", K(i), K(output_idx), K(ret));
                }
              }
            }
          }
        }
      } else if (OB_FAIL(compare(i, output_idx, cmp_ret))) {
        LOG_WARN("index merge failed to compare row", K(i), K(output_idx), K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (output_idx == OB_INVALID_INDEX) {
      /* no available row, stop the entire process */
      ret = OB_ITER_END;
    } else {
      /* find all available rows */
      bool all_matched = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stores_.count(); i++) {
        if (output_idx == i) {
          /* skip */
        } else if (child_stores_.at(i).iter_end_ || !child_stores_.at(i).have_data()) {
          ret = OB_ITER_END;
        } else if (OB_FAIL(compare(i, output_idx, cmp_ret))) {
            LOG_WARN("index merge failed to compare row", K(i), K(output_idx), K(ret));
        } else if (cmp_ret == 0) {
          child_stores_.at(i).cur_idx_++;
        } else {
          all_matched = false;
        }
      }
      if (OB_SUCC(ret)) {
        if (all_matched) {
          // 获取 relevance score
          double relevance_score = 0.0;
          const ObDatum *fts_datums = child_stores_.at(0).cur_datums();
          if (fts_datums != nullptr && child_stores_.at(0).exprs_ != nullptr 
              && child_stores_.at(0).exprs_->count() > 1) {
            relevance_score = fts_datums[1].get_double();
          }

        
          // 获取当前的 StoredRow
          const ObChunkDatumStore::StoredRow *src_row = child_stores_.at(output_idx).store_rows_[child_stores_.at(output_idx).cur_idx_].store_row_;
          
          if (OB_ISNULL(src_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null stored row", K(ret));
          } else {
            // 使用 StoredRow::build 创建一个新的 StoredRow 副本
            // 计算需要的内存大小
            int64_t row_size = src_row->row_size_;
            char *buf = static_cast<char*>(temp_allocator.alloc(row_size));
            if (OB_ISNULL(buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to allocate memory for stored row", K(row_size), K(ret));
            } else {
              // 创建新的 StoredRow 并使用 assign 复制数据
              ObChunkDatumStore::StoredRow *copied_row = 
                  reinterpret_cast<ObChunkDatumStore::StoredRow*>(buf);
              
              if (OB_FAIL(copied_row->assign(src_row))) {
                LOG_WARN("failed to assign stored row", K(ret));
              } else {
                RowWithScore row_score;
                row_score.stored_row = copied_row;
                row_score.relevance_score = relevance_score;
                all_rows.push_back(row_score);
                
                // 移动到下一行
                child_stores_.at(output_idx).cur_idx_++;
                
                // LOG_INFO("[INDEX_MERGE_DEBUG] COLLECTED ROW",
                //         "total", all_rows.size(),
                //         "score", relevance_score);
              }
            }
          }

          /* found available row in each child store, output */
          // 打印当前输出行的 rowkey 信息
          const ObDatum *out_datums = child_stores_.at(output_idx).cur_datums();
          if (out_datums != nullptr && rowkey_exprs_ != nullptr && rowkey_exprs_->count() > 0) {
            total_output_count++;
            // LOG_INFO("[INDEX_MERGE_DEBUG] OUTPUT ROW (rows version)",
            //          K(total_output_count),
            //          K(output_idx),
            //          "child0_cur_idx", child_stores_.at(0).cur_idx_,
            //          "child0_saved", child_stores_.at(0).saved_size_,
            //          "child1_cur_idx", child_stores_.count() > 1 ? child_stores_.at(1).cur_idx_ : -1,
            //          "child1_saved", child_stores_.count() > 1 ? child_stores_.at(1).saved_size_ : -1,
            //          "rowkey_datum0", out_datums[0]);
          }
          // if (OB_FAIL(child_stores_.at(output_idx).to_expr())) {
          //   LOG_WARN("index merge failed to convert row to expr", K(ret));
          // } else if (OB_FAIL(save_row_to_result_buffer())) {
          //   LOG_WARN("failed to save row to result buffer", K(ret));
          // } else {
          //   count += 1;
          // }
        } else {
          child_stores_.at(output_idx).cur_idx_++;
        }
      }
    }
  }
  // finalize all pendings and clear all pendings
  oceanbase::storage::ObTokenPostingListCache::get_instance().finalize_all_pending();
  oceanbase::storage::ObTokenPostingListCache::get_instance().clear_all_pending();
  
  // Finalize index scan cache
  ObIndexScanCache::get_instance().finalize_all_pending();
  ObIndexScanCache::get_instance().clear_all_pending();

  // 结束后检查 ret
  if(OB_ITER_END == ret){
    ret = OB_SUCCESS;
  }  

  int64_t total_matched = all_rows.size();
  // LOG_INFO("[INDEX_MERGE_DEBUG] Collection phase done", K(total_matched), K(ret));

  if (OB_SUCC(ret) && total_matched > 0) {
    // 先排序
    std::sort(all_rows.begin(), all_rows.end(),
              [](const RowWithScore &a, const RowWithScore &b) {
                return a.relevance_score > b.relevance_score;  // 降序排序
              });
    int64_t rows_to_return = total_matched;  // default: return all
    int64_t offset = 0;
    // Evaluate limit expression if present
    if (OB_NOT_NULL(merge_ctdef_) && OB_NOT_NULL(merge_ctdef_->limit_expr_)) {
      ObDatum *limit_datum = nullptr;
      if (OB_FAIL(merge_ctdef_->limit_expr_->eval(*eval_ctx_, limit_datum))) {
        LOG_WARN("failed to eval limit expr", K(ret), KPC(merge_ctdef_->limit_expr_));
      } else if (OB_ISNULL(limit_datum)) {
        LOG_WARN("limit datum is null after eval", K(ret));
      } else if (limit_datum->is_null()) {
        LOG_INFO("limit datum is sql null", K(ret));
      } else {
        rows_to_return = limit_datum->get_int();
        LOG_INFO("Evaluated limit expr", "limit", rows_to_return);
      }
    } else {
      LOG_INFO("No limit expr found in ctdef", "ctdef", merge_ctdef_, 
               "limit_expr", (merge_ctdef_ ? merge_ctdef_->limit_expr_ : nullptr));
    }
    // Evaluate offset expression if present
    if (OB_SUCC(ret) && OB_NOT_NULL(merge_ctdef_) && OB_NOT_NULL(merge_ctdef_->offset_expr_)) {
      ObDatum *offset_datum = nullptr;
      if (OB_FAIL(merge_ctdef_->offset_expr_->eval(*eval_ctx_, offset_datum))) {
        LOG_WARN("failed to eval offset expr", K(ret), KPC(merge_ctdef_->offset_expr_));
      } else if (OB_ISNULL(offset_datum)) {
        LOG_WARN("offset datum is null after eval", K(ret));
      } else if (offset_datum->is_null()) {
        LOG_INFO("offset datum is sql null", K(ret));
      } else {
        offset = offset_datum->get_int();
        LOG_INFO("Evaluated offset expr", "offset", offset);
      }
    }
    // Clamp rows_to_return to available data after offset
    if (OB_SUCC(ret)) {
      if (offset >= total_matched) {
        rows_to_return = 0;
      } else if (offset + rows_to_return > total_matched) {
        rows_to_return = total_matched - offset;
      }
    }
    count = rows_to_return;
    LOG_INFO("[INDEX_MERGE_DEBUG] TopK sort with limit", 
             K(total_matched), K(rows_to_return), K(offset), K(capacity));
    for (int64_t i = 0; OB_SUCC(ret) && i < rows_to_return; i++) {
      const RowWithScore &row_score = all_rows[offset + i];
      if (OB_ISNULL(row_score.stored_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null stored row", K(i), K(ret));
      } else {
        // 将 StoredRow 投影到表达式
        ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
        batch_info_guard.set_batch_size(1);
        batch_info_guard.set_batch_idx(0);
        
        if (OB_FAIL(row_score.stored_row->to_expr<true>(*output_, *eval_ctx_))) {
          LOG_WARN("failed to convert stored row to expr", K(i), K(ret));
        } else if (OB_FAIL(save_row_to_result_buffer())) {
          LOG_WARN("failed to save row to result buffer", K(i), K(ret));
        } else {
          // LOG_INFO("[INDEX_MERGE_DEBUG] Added sorted row", 
          //          K(i), "score", row_score.relevance_score);
        }
      }
    }
  }

  // 最后将 result_buffer_ 转换回表达式
  if (OB_SUCC(ret) && count > 0) {
    if (OB_FAIL(result_buffer_.to_expr(count))) {
      LOG_WARN("failed to convert result buffer to exprs", K(ret));
    }
  }
  
  // LOG_INFO("[INDEX_MERGE_DEBUG] intersect_get_next_rows EXIT", 
  //          K(count), K(total_matched), K(ret));
  
  ret = OB_ITER_END;
  return ret;
}

int ObDASIndexMergeIter::union_get_next_row()
{
  int ret = OB_SUCCESS;
  /* try to fill each child store */
  int64_t output_idx = OB_INVALID_INDEX;
  int cmp_ret = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stores_.count(); i++) {
    IndexMergeRowStore &child_store = child_stores_.at(i);
    if (!child_store.have_data()) {
      if (!child_store.iter_end_) {
        ObDASIter *child_iter = child_iters_.at(i);
        if (OB_ISNULL(child_iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr", K(i));
        } else if (OB_FAIL(child_iter->get_next_row())) {
          if (OB_ITER_END == ret) {
            child_store.iter_end_ = true;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get next row from child iter", K(ret));
          }
        } else if (OB_FAIL(child_store.save(false, 1))) {
          LOG_WARN("failed to save child row", K(ret));
        } else if (OB_FAIL(compare(i, output_idx, cmp_ret))) {
          LOG_WARN("index merge failed to compare row", K(i), K(output_idx), K(ret));
        }
      }
    } else if (OB_FAIL(compare(i, output_idx, cmp_ret))) {
      LOG_WARN("index merge failed to compare row", K(i), K(output_idx), K(ret));
    }
  }

  // TODO: use min-heap optimization
  if (OB_FAIL(ret)) {
  } else if (output_idx == OB_INVALID_INDEX) {
    /* no available row, stop the entire process */
    ret = OB_ITER_END;
  } else {
    /* find all available rows */
    bool need_fill_default_value = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stores_.count(); i++) {
      need_fill_default_value = false;
      if (output_idx == i) {
        /* skip */
      } else if (child_stores_.at(i).have_data()) {
        if (OB_FAIL(compare(i, output_idx, cmp_ret))) {
          LOG_WARN("index merge failed to compare row", K(i), K(output_idx), K(ret));
        } else if (cmp_ret == 0) {
          if (OB_FAIL(child_stores_.at(i).to_expr())) {
            LOG_WARN("failed to convert store row to expr", K(ret));
          }
        } else {
          // this child does not have a corresponding rowkey
          need_fill_default_value = true;
        }
      } else {
        // this child does not have available rows
        need_fill_default_value = true;
      }
      if (OB_SUCC(ret) && need_fill_default_value) {
        if (OB_FAIL(fill_default_values_for_union(child_match_against_exprs_.at(i)))) {
          LOG_WARN("failed to fill default values for union", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(child_stores_.at(output_idx).to_expr())) {
        LOG_WARN("failed to convert store row to expr", K(ret));
      }
    }
  }
  return ret;
}

int ObDASIndexMergeIter::union_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  count = 0;
  result_buffer_.reuse();
  while (OB_SUCC(ret) && count < capacity) {
    /* try to fill each child store */
    int64_t output_idx = OB_INVALID_INDEX;
    int cmp_ret = 0;
    int64_t child_rows_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stores_.count(); i++) {
      IndexMergeRowStore &child_store = child_stores_.at(i);
      child_rows_cnt = 0;
      if (!child_store.have_data()) {
        if (!child_store.iter_end_) {
          ObDASIter *child_iter = child_iters_.at(i);
          if (OB_ISNULL(child_iter)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected nullptr", K(i));
          } else {
            ret = child_iter->get_next_rows(child_rows_cnt, capacity);
            if (OB_ITER_END == ret && child_rows_cnt > 0) {
              ret = OB_SUCCESS;
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(child_store.save(true, child_rows_cnt))) {
                LOG_WARN("failed to save child rows", K(child_rows_cnt), K(ret));
              } else if (OB_FAIL(compare(i, output_idx, cmp_ret))) {
                LOG_WARN("index merge failed to compare row", K(i), K(output_idx), K(ret));
              } else if (child_iter->get_type() == DAS_ITER_SORT) {
                reset_datum_ptr(child_iter->get_output(), child_rows_cnt);
              }
            } else if (OB_ITER_END == ret) {
              child_store.iter_end_ = true;
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("failed to get next rows from child iter", K(ret));
            }
          }
        }
      } else if (OB_FAIL(compare(i, output_idx, cmp_ret))) {
        LOG_WARN("index merge failed to compare row", K(i), K(output_idx), K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (output_idx == OB_INVALID_INDEX) {
      /* no available row, stop the entire process */
      ret = OB_ITER_END;
    } else {
      /* find all available rows */
      bool need_fill_default_value = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < child_stores_.count(); i++) {
        need_fill_default_value = false;
        if (output_idx == i) {
          /* skip */
        } else if (child_stores_.at(i).have_data()) {
          if (OB_FAIL(compare(i, output_idx, cmp_ret))) {
            LOG_WARN("index merge failed to compare row", K(i), K(output_idx), K(ret));
          } else if (cmp_ret == 0) {
            if (OB_FAIL(child_stores_.at(i).to_expr())) {
              LOG_WARN("failed to convert store row to expr", K(ret));  
            }
          } else {
            // this child does not have a corresponding rowkey
            need_fill_default_value = true;
          }
        } else {
          // this child does not have available rows
          need_fill_default_value = true;
        }
        if (OB_SUCC(ret) && need_fill_default_value) {
          if (OB_FAIL(fill_default_values_for_union(child_match_against_exprs_.at(i)))) {
            LOG_WARN("failed to fill default values for union", K(ret), K(i));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(child_stores_.at(output_idx).to_expr())) {
          LOG_WARN("failed to convert store row to expr", K(ret));
        } else {
          // now we get a available result row, save it to result buffer
          if (OB_FAIL(save_row_to_result_buffer())) {
            LOG_WARN("failed to save row to result buffer", K(ret));
          } else {
            count += 1;
          }
        }
      }
    }
  }

  if (OB_ITER_END == ret && count > 0) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret) && count > 0) {
    if (OB_FAIL(result_buffer_.to_expr(count))) {
      LOG_WARN("failed to convert result buffer to exprs", K(ret));
    }
  }
  return ret;
}


}  // namespace sql
}  // namespace oceanbase
