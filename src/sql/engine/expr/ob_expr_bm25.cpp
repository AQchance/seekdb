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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_bm25.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "share/vector_type/ob_vector_bm25.h"

namespace oceanbase
{
namespace sql
{
ObExprBM25::ObExprBM25(ObIAllocator &alloc)
  : ObFuncExprOperator(alloc, T_FUN_SYS_BM25, N_BM25, 4, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

int ObExprBM25::calc_result_typeN(
    ObExprResType &result_type,
    ObExprResType *types,
    int64_t param_num,
    common::ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(type_ctx);
  if (OB_UNLIKELY(param_num != 5)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("BM25 expr should have 4 parameters", K(ret), K(param_num));
  } else {
    types[TOKEN_DOC_CNT_PARAM_IDX].set_calc_type(ObIntType);
    types[TOTAL_DOC_CNT_PARAM_IDX].set_calc_type(ObIntType);
    types[DOC_TOKEN_CNT_PARAM_IDX].set_calc_type(ObIntType);
    types[AVG_DOC_CNT_PARAM_IDX].set_calc_type(ObDoubleType);
    types[RELATED_TOKEN_CNT_PARAM_IDX].set_calc_type(ObUInt64Type);
    result_type.set_double();
  }
  return ret;
}

int ObExprBM25::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;
  UNUSED(expr_cg_ctx);
  CK(5 == raw_expr.get_param_count());
  rt_expr.eval_func_ = eval_bm25_relevance_expr;
  rt_expr.eval_batch_func_ = eval_batch_bm25_relevance_expr;
  return ret;
}

int ObExprBM25::eval_bm25_relevance_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum)
{
  int ret = OB_SUCCESS;
  ObDatum *token_doc_cnt_datum = nullptr;
  ObDatum *total_doc_cnt_datum = nullptr;
  ObDatum *doc_token_cnt_datum = nullptr;
  ObDatum *avg_doc_token_cnt_datum = nullptr;
  ObDatum *related_token_cnt_datum = nullptr;
  if (OB_FAIL(expr.eval_param_value(
      ctx,
      token_doc_cnt_datum,
      total_doc_cnt_datum,
      doc_token_cnt_datum,
      avg_doc_token_cnt_datum,
      related_token_cnt_datum))) {
    LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (OB_UNLIKELY(token_doc_cnt_datum->is_null() || total_doc_cnt_datum->is_null()
      || doc_token_cnt_datum->is_null() || avg_doc_token_cnt_datum->is_null() || related_token_cnt_datum->is_null())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null datum", K(ret), KPC(token_doc_cnt_datum), KPC(total_doc_cnt_datum),
        KPC(doc_token_cnt_datum), KPC(avg_doc_token_cnt_datum), KPC(related_token_cnt_datum));
  } else {
    const int64_t token_doc_cnt = token_doc_cnt_datum->get_int();
    const int64_t total_doc_cnt = total_doc_cnt_datum->get_int();
    const int64_t related_token_cnt = related_token_cnt_datum->get_uint();
    const int64_t doc_token_cnt = doc_token_cnt_datum->get_int();
    const double avg_doc_token_cnt = avg_doc_token_cnt_datum->get_double();
    const double norm_len = doc_token_cnt / avg_doc_token_cnt;
    const double token_weight = query_token_weight(token_doc_cnt, total_doc_cnt);
    const double doc_weight = doc_token_weight(related_token_cnt, norm_len);
    const double relevance = token_weight * doc_weight;
    res_datum.set_double(relevance);
    LOG_DEBUG("show bm25 parameters for current document",
        K(token_doc_cnt), K(total_doc_cnt), K(related_token_cnt), K(doc_token_cnt), K(avg_doc_token_cnt),
        K(norm_len), K(token_weight), K(doc_weight), K(relevance));
  }
  return ret;
}

int ObExprBM25::eval_batch_bm25_relevance_expr(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObDatumVector token_doc_cnt_datum;
  ObDatumVector total_doc_cnt_datum;
  ObDatumVector doc_token_cnt_datum;
  ObDatumVector avg_doc_token_cnt_datum;
  ObDatumVector related_token_cnt_datum;
  if (OB_FAIL(expr.eval_batch_param_value(
    ctx,
    skip,
    size,
    token_doc_cnt_datum,
    total_doc_cnt_datum,
    doc_token_cnt_datum,
    avg_doc_token_cnt_datum,
    related_token_cnt_datum))) {
      LOG_WARN("evaluate parameter value failed", K(ret));
  } else if (OB_UNLIKELY(token_doc_cnt_datum.at(0)->is_null() || total_doc_cnt_datum.at(0)->is_null()
      || avg_doc_token_cnt_datum.at(0)->is_null())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null datum", K(ret), KPC(token_doc_cnt_datum.at(0)), KPC(total_doc_cnt_datum.at(0)),
         KPC(avg_doc_token_cnt_datum.at(0)));
  } else {
      const int64_t token_doc_cnt = token_doc_cnt_datum.at(0)->get_int();
      const int64_t total_doc_cnt = total_doc_cnt_datum.at(0)->get_int();
      const double token_weight = query_token_weight(token_doc_cnt, total_doc_cnt);
      const double avg_doc_token_cnt = avg_doc_token_cnt_datum.at(0)->get_double();
      ObDatum *res_datum = expr.locate_batch_datums(ctx);
      ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

      // Check if we can use SIMD fast path:
      // 1. No rows to skip (all rows need processing)
      // 2. No rows already evaluated
      // 3. Data is contiguous (ObDatumVector stores contiguous datums)
      bool can_use_simd = true;
      int64_t contiguous_start = -1;
      int64_t contiguous_count = 0;

      // Find contiguous segments without skips or already-evaluated rows
      for (int64_t i = 0; i < size && can_use_simd; ++i) {
        if (skip.contain(i) || eval_flags.at(i)) {
          can_use_simd = false;
        } else if (doc_token_cnt_datum.at(i)->is_null() || related_token_cnt_datum.at(i)->is_null()) {
          can_use_simd = false;
        }
      }

      if (can_use_simd && size > 0) {
        // SIMD fast path: extract data to contiguous arrays and compute in batch
        // Use stack allocation for small batches, heap for large ones
        constexpr int64_t STACK_BATCH_SIZE = 256;
        int64_t doc_token_cnts_stack[STACK_BATCH_SIZE];
        uint64_t related_token_cnts_stack[STACK_BATCH_SIZE];
        double results_stack[STACK_BATCH_SIZE];

        int64_t *doc_token_cnts = doc_token_cnts_stack;
        uint64_t *related_token_cnts = related_token_cnts_stack;
        double *results = results_stack;

        common::ObIAllocator *alloc = nullptr;
        if (size > STACK_BATCH_SIZE) {
          alloc = &ctx.exec_ctx_.get_allocator();
          doc_token_cnts = static_cast<int64_t*>(alloc->alloc(size * sizeof(int64_t)));
          related_token_cnts = static_cast<uint64_t*>(alloc->alloc(size * sizeof(uint64_t)));
          results = static_cast<double*>(alloc->alloc(size * sizeof(double)));
          if (OB_ISNULL(doc_token_cnts) || OB_ISNULL(related_token_cnts) || OB_ISNULL(results)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory for SIMD batch", K(ret), K(size));
          }
        }

        if (OB_SUCC(ret)) {
          // Extract data from datum vectors to contiguous arrays
          for (int64_t i = 0; i < size; ++i) {
            doc_token_cnts[i] = doc_token_cnt_datum.at(i)->get_int();
            related_token_cnts[i] = related_token_cnt_datum.at(i)->get_uint();
          }

          // Call SIMD kernel
          ret = common::bm25_batch_compute(
              doc_token_cnts, related_token_cnts,
              avg_doc_token_cnt, token_weight,
              results, size, true /* use_simd */);

          if (OB_SUCC(ret)) {
            // Write results back to datum array
            for (int64_t i = 0; i < size; ++i) {
              res_datum[i].set_double(results[i]);
              eval_flags.set(i);
            }
            LOG_DEBUG("BM25 SIMD batch completed", K(size), K(token_doc_cnt),
                      K(total_doc_cnt), K(avg_doc_token_cnt), K(token_weight));
          }
        }
      } else {
        // Scalar fallback path: process row by row
        for(int64_t i = 0; OB_SUCC(ret) && i < size; ++i)
        {
          if (OB_UNLIKELY(doc_token_cnt_datum.at(i)->is_null() || related_token_cnt_datum.at(i)->is_null())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null datum", K(ret), KPC(doc_token_cnt_datum.at(i)), KPC(related_token_cnt_datum.at(i)));
          } else if (!skip.contain(i) && !eval_flags.at(i)) {
            const int64_t related_token_cnt = related_token_cnt_datum.at(i)->get_uint();
            const int64_t doc_token_cnt = doc_token_cnt_datum.at(i)->get_int();
            const double norm_len = doc_token_cnt / avg_doc_token_cnt;
            const double doc_weight = doc_token_weight(related_token_cnt, norm_len);
            const double relevance = token_weight * doc_weight;
            res_datum[i].set_double(relevance);
            eval_flags.set(i);
            LOG_DEBUG("show bm25 parameters for current document",
                K(token_doc_cnt), K(total_doc_cnt), K(related_token_cnt), K(doc_token_cnt), K(avg_doc_token_cnt),
                K(norm_len), K(token_weight), K(doc_weight), K(relevance));
          }
        }
      }
  }
  return ret;
}

double ObExprBM25::doc_token_weight(const int64_t token_freq, const double norm_len)
{
  const double tf = static_cast<double>(token_freq);
  return tf / (tf + p_k1 * (1.0 - p_b + p_b * norm_len));
}

double ObExprBM25::query_token_weight(const int64_t doc_freq, const int64_t doc_cnt)
{
  const double df = static_cast<double>(doc_freq);
  const double len = static_cast<double>(doc_cnt);
  // Since we might use approximate count statistic for total doc cnt, possibilities there are
  //   document frequencies larger than total doc cnt
  const double diff = (len - df) > 0 ? (len - df) : 0;
  const double idf = std::log((diff + 0.5) / (df + 0.5));
  return MAX(p_epsilon, idf) * (1.0 + p_k1);
}


} // namespace sql
} // namespace oceanbase
