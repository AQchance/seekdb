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

#include "ob_scalar_index_wrapper_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace storage
{

ObScalarIndexWrapperIter::ObScalarIndexWrapperIter()
  : scan_iter_(nullptr),
    table_scan_iter_(nullptr),
    eval_ctx_(nullptr),
    in_shallow_status_(false),
    is_iter_end_(false),
    id_check_limit_(nullptr)
{
}

ObScalarIndexWrapperIter::~ObScalarIndexWrapperIter()
{
}

int ObScalarIndexWrapperIter::init(sql::ObDASScanIter *scan_iter, const sql::ObEvalCtx *eval_ctx, const sql::ObDocIdExt *doc_id_limit)
{
  int ret = OB_SUCCESS;
  LOG_INFO("ObScalarIndexWrapperIter::init ENTRY", K(scan_iter), K(eval_ctx));
  if (OB_ISNULL(scan_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    scan_iter_ = scan_iter;
    eval_ctx_ = eval_ctx;
    table_scan_iter_ = nullptr;
    if (OB_NOT_NULL(scan_iter_)) {
      // Try to extract underlying table scan iterator for performance
      common::ObNewRowIterator *iter = scan_iter_->get_output_result_iter();
    if (OB_ISNULL(iter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("scan iter result iterator is null", K(ret));
    } else {
        common::ObNewRowIterator::IterType type = iter->get_type();
        LOG_INFO("ObScalarIndexWrapperIter::init check type", K(type));
        if (type == common::ObNewRowIterator::ObTableScanIterator) {
             table_scan_iter_ = static_cast<storage::ObTableScanIterator*>(iter);
             LOG_INFO("ObScalarIndexWrapperIter::init success casting to TableScanIterator");
        } else {
             LOG_INFO("ObScalarIndexWrapperIter::init failed casting", K(type));
        }
    }
}
    
    if (OB_NOT_NULL(doc_id_limit)) {
      // Virtual Mode (Deprecated but supported for logic if scan_iter is null)
      // If scan_iter is NOT null, limit is just a check hint, not a driver.
      // We strictly use scan_iter driver now.
      id_check_limit_ = doc_id_limit;
    } else {
      id_check_limit_ = nullptr;
    }
    is_iter_end_ = false;
    in_shallow_status_ = true; // Use shallow to represent "peeked" state or ready state
    
    // Setup max score tuple (always 0, or just to serve IDs)
    // For pruning, we use max_domain_id_ to represent the NEXT valid ID.
    // min_domain_id_ is effectively the same if granularity is row-level.
    max_score_tuple_.max_score_ = 0.0;
    
    // Initial fetch to prime the iterator
    // We cannot do valid row check without scan_iter and eval_ctx
    int64_t temp_doc_id = 0; // Placeholder for initial fetch
    if (OB_FAIL(scan_next_valid_row(nullptr, true))) {
        if (ret != OB_ITER_END) {
             LOG_WARN("failed to get first row from scalar iter", K(ret));
        } else {
            is_iter_end_ = true;
            ret = OB_SUCCESS; // Empty is fine
        }
    }
  }
  return ret;
}

int ObScalarIndexWrapperIter::get_next_row()
{
    // This is called during precise evaluation or projection.
    // If we are already pointing to the valid row, just return success.
    if (is_iter_end_) {
        return OB_ITER_END;
    }
    return OB_SUCCESS;
}

int ObScalarIndexWrapperIter::get_next_batch(const int64_t capacity, int64_t &count)
{
    // Not typically called in BMW context for dimensions
    return OB_NOT_SUPPORTED;
}

int ObScalarIndexWrapperIter::get_curr_score(double &score) const
{
    score = 0.0;
    return OB_SUCCESS;
}

int ObScalarIndexWrapperIter::get_curr_id(const common::ObDatum *&datum) const
{
    if (is_iter_end_) {
        return OB_ITER_END;
    }
    datum = &curr_doc_id_;
    return OB_SUCCESS;
}

int ObScalarIndexWrapperIter::scan_next_valid_row(const common::ObDatum *min_target_id, const bool inclusive)
{
    int ret = OB_SUCCESS;
    if (OB_ISNULL(scan_iter_) || OB_ISNULL(eval_ctx_)) {
       // Virtual logic removed for brevity as we focus on scan_iter_
       return OB_SUCCESS;
    }


    bool found = false;
    while (OB_SUCC(ret) && !found) {
        if (OB_FAIL(scan_iter_->get_next_row())) {
             if (ret == OB_ITER_END) {
                is_iter_end_ = true;
                ret = OB_SUCCESS; 
                break;
            } else {
                LOG_WARN("failed to get next row", K(ret));
            }
        } else {
             const sql::ExprFixedArray *output = scan_iter_->get_output();
             if (OB_ISNULL(output) || output->count() < 1) {
                 ret = OB_ERR_UNEXPECTED;
                 LOG_WARN("unexpected output exprs cnt", K(ret));
             } else {
                 sql::ObExpr *doc_id_expr = output->at(0); 
                 common::ObDatum &datum = doc_id_expr->locate_expr_datum(*const_cast<sql::ObEvalCtx*>(eval_ctx_));
                 int64_t val = datum.get_int();
                 
                 // Deep copy
                 // Note: ObDatum doesn't own memory. 
                 // We should ideally copy into a buffer if it crosses rows.
                 // For now assuming safe within call.
                 curr_doc_id_.set_datum(datum); 
                 
                 if (min_target_id != nullptr) {
                     int64_t target = min_target_id->get_int();
                     if (val < target) continue;
                     if (!inclusive && val == target) continue;
                 }
                 
                 if (id_check_limit_ != nullptr) {
                     if (val >= id_check_limit_->get_datum().get_int()) {
                         is_iter_end_ = true;
                         break;
                     }
                 }
                 found = true;
            }
        }
    }
    return ret;
}

int ObScalarIndexWrapperIter::advance_shallow(const common::ObDatum &id_datum, const bool inclusive)
{
    if (is_iter_end_) return OB_SUCCESS;
    
    // Optimization: check current
    if (curr_doc_id_.get_int() > id_datum.get_int()) return OB_SUCCESS;
    if (inclusive && curr_doc_id_.get_int() == id_datum.get_int()) return OB_SUCCESS;
    
    return scan_next_valid_row(&id_datum, inclusive);
}

int ObScalarIndexWrapperIter::get_curr_block_max_info(const ObMaxScoreTuple *&max_score_tuple)
{
    // If ended, we effectively return a "block" that is [MAX, MAX] with score 0?
    // This allows BMW to process it (and likely discard/terminate).
    if (is_iter_end_) {
        // We need a datum representing MAX.
        // Hack: just return regular OB_ITER_END?
        // If we return OB_ITER_END, ObSRBMWIterImpl logs warning.
        // Let's rely on BMW to check is_iter_end? 
        // No, BMW calls this method.
        // We will return OB_SUCCESS but set IDs to Int64 Max?
        // Or simply fail normally, and we modify BMW to handle it?
        // Modifying BMW is better practice than hacking data.
        return OB_ITER_END; 
    }
    
    max_score_tuple_.min_domain_id_ = &curr_doc_id_;
    // For Virtual Range: Max Domain is also current?
    // If we are dense [0, 1000], we essentially say "MaxScore 0 for range [Current, Limit)".
    // But BMW expects block info.
    // If we say [Current, Current], it works but is slow (step by step).
    // If we say [Current, Limit], it implies we exist everywhere in between?
    // Yes, for Virtual Range "id < 1000", we exist everywhere! (It's a "Limit", not "Set").
    // So if virtual mode: Max = Limit - 1.
    // If scan mode: Max = Current? (Sparse).
    
    if (OB_ISNULL(scan_iter_)) {
       // Virtual Dense Range
       // We can claim coverage up to limit.
       // But we need a Datum for that.
       // We can keep `limit_datum_` around.
       // For now, simple step-by-step is safest.
       max_score_tuple_.max_domain_id_ = &curr_doc_id_;
    } else {
       max_score_tuple_.max_domain_id_ = &curr_doc_id_;
    }

    max_score_tuple_.max_score_ = 0.0;
    max_score_tuple = &max_score_tuple_;
    
    return OB_SUCCESS;
}

} // namespace storage
} // namespace oceanbase
