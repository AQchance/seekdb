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

#ifndef OB_SCALAR_INDEX_WRAPPER_ITER_H_
#define OB_SCALAR_INDEX_WRAPPER_ITER_H_

#include "storage/retrieval/ob_i_sparse_retrieval_iter.h"
#include "sql/das/iter/ob_das_scan_iter.h"
#include "storage/retrieval/ob_block_max_iter.h"

namespace oceanbase
{
namespace storage
{

class ObScalarIndexWrapperIter : public ObISRDimBlockMaxIter
{
public:
  ObScalarIndexWrapperIter();
  virtual ~ObScalarIndexWrapperIter();

  int init(sql::ObDASScanIter *scan_iter, const sql::ObEvalCtx *eval_ctx, const sql::ObDocIdExt *id_check_limit = nullptr);
  
  // Overrides from ObISparseRetrievalDimIter
  virtual int get_next_row() override;
  virtual int get_next_batch(const int64_t capacity, int64_t &count) override;
  virtual bool is_mandatory() const override { return true; }

  // Overrides from ObISRDaaTDimIter
  virtual int get_curr_score(double &score) const override;
  virtual int get_curr_id(const common::ObDatum *&datum) const override;
  
  // Overrides from ObISRDimBlockMaxIter
  virtual int advance_shallow(const common::ObDatum &id_datum, const bool inclusive) override;
  virtual int get_curr_block_max_info(const ObMaxScoreTuple *&max_score_tuple) override;
  virtual bool in_shallow_status() const override { return in_shallow_status_; }

  TO_STRING_KV(K(in_shallow_status_), K(curr_doc_id_), K(is_iter_end_));

private:
  int scan_next_valid_row(const common::ObDatum *min_target_id = nullptr, bool inclusive = true);
  
private:
  sql::ObDASScanIter *scan_iter_;
  storage::ObTableScanIterator *table_scan_iter_;
  const sql::ObEvalCtx *eval_ctx_;
  bool in_shallow_status_;
  bool is_iter_end_;
  common::ObDatum curr_doc_id_;
  ObMaxScoreTuple max_score_tuple_;
  common::ObArenaAllocator allocator_;
  const sql::ObDocIdExt *id_check_limit_; // Optional upper limit for searching
};

} // namespace storage
} // namespace oceanbase

#endif // OB_SCALAR_INDEX_WRAPPER_ITER_H_
