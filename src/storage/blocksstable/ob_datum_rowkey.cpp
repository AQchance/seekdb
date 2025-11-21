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

#include "ob_datum_rowkey.h"
#include "share/schema/ob_table_param.h"
#include "storage/blocksstable/ob_datum_rowkey_vector.h"

namespace oceanbase
{
namespace blocksstable
{

static ObStorageDatum make_ext_datum(const int64_t ext_value) { ObStorageDatum datum; datum.set_ext_value(ext_value); return datum; }

ObStorageDatum ObDatumRowkey::MIN_DATUM = make_ext_datum(ObObj::MIN_OBJECT_VALUE);
ObStorageDatum ObDatumRowkey::MAX_DATUM = make_ext_datum(ObObj::MAX_OBJECT_VALUE);
ObDatumRowkey ObDatumRowkey::MIN_ROWKEY(&ObDatumRowkey::MIN_DATUM, 1);
ObDatumRowkey ObDatumRowkey::MAX_ROWKEY(&ObDatumRowkey::MAX_DATUM, 1);

ObDatumRowkey::ObDatumRowkey(ObStorageDatum *datums, const int64_t datum_cnt)
  : datum_cnt_(datum_cnt),
    group_idx_(0),
    hash_(0),
    datums_(datums),
    store_rowkey_()
{}

ObDatumRowkey::ObDatumRowkey(ObStorageDatumBuffer &datum_buffer)
  : datum_cnt_(datum_buffer.get_capacity()),
    group_idx_(0),
    hash_(0),
    datums_(datum_buffer.get_datums()),
    store_rowkey_()
{
}

int ObDatumRowkey::murmurhash(const uint64_t seed, const ObStorageDatumUtils &datum_utils, uint64_t &hash) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || !datum_utils.is_valid() || datum_utils.get_rowkey_count() < datum_cnt_)) {
    ret = OB_INVALID_ARGUMENT;

  } else {
    hash = seed;
    if (is_ext_rowkey()) {
      if (OB_FAIL(datum_utils.get_ext_hash_funcs().hash_func_(datums_[0], hash, hash))) {

      }
    } else {
      for (int64_t i = 0; i < datum_cnt_ && OB_SUCC(ret); i++) {
        if (OB_FAIL(datum_utils.get_hash_funcs().at(i).hash_func_(datums_[i], hash, hash))) {

        }
      }
    }
  }
  return ret;
}

int ObDatumRowkey::equal(const ObDatumRowkey &rhs, const ObStorageDatumUtils &datum_utils, bool &is_equal) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid() || !rhs.is_valid() || !datum_utils.is_valid())) {
    ret = OB_INVALID_ARGUMENT;

  } else if (FALSE_IT(is_equal = datum_cnt_ == rhs.datum_cnt_)) {
  } else if (is_equal && datums_ != rhs.datums_) {
    if (datum_utils.get_rowkey_count() < datum_cnt_) {
      ret = OB_ERR_UNEXPECTED;

    } else {
      const ObStoreCmpFuncs &cmp_funcs = datum_utils.get_cmp_funcs();
      int cmp_ret = 0;
      for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < datum_cnt_; i++) {
        if (OB_FAIL(cmp_funcs.at(i).compare(datums_[i], rhs.datums_[i], cmp_ret))) {

        } else {
          is_equal = 0 == cmp_ret;
        }
      }
    }
  }

  return ret;
}

int ObDatumRowkey::compare(const ObDatumRowkey &rhs, const ObStorageDatumUtils &datum_utils, int &cmp_ret,
                           const bool compare_datum_cnt) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid() || !rhs.is_valid() || !datum_utils.is_valid())) {
    ret = OB_INVALID_ARGUMENT;

  } else {
    int64_t cmp_cnt = MIN(datum_cnt_, rhs.datum_cnt_);
    if (datum_utils.get_rowkey_count() < cmp_cnt) {
      ret = OB_ERR_UNEXPECTED;

    } else {
      const ObStoreCmpFuncs &cmp_funcs = datum_utils.get_cmp_funcs();
      cmp_ret = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < cmp_cnt && 0 == cmp_ret; ++i) {
        if (OB_FAIL(cmp_funcs.at(i).compare(datums_[i], rhs.datums_[i], cmp_ret))) {

        }
      }
      if (0 == cmp_ret && compare_datum_cnt) {
        cmp_ret = datum_cnt_ - rhs.datum_cnt_;
      }
    }
  }

  return ret;
}


int ObDatumRowkey::compare(const ObCommonDatumRowkey &rhs, const ObStorageDatumUtils &datum_utils, int &cmp_ret,
                           const bool compare_datum_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rhs.compare(*this, datum_utils, cmp_ret, compare_datum_cnt))) {

  } else {
    cmp_ret = -cmp_ret;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObDatumRowkey)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;

  } else {
    OB_UNIS_ENCODE_ARRAY(datums_, datum_cnt_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDatumRowkey)
{
  int ret = OB_SUCCESS;
  reuse();
  if (OB_ISNULL(datums_)) {
    ret = OB_NOT_INIT;

  } else {
    OB_UNIS_DECODE(datum_cnt_);
    if (datum_cnt_ > OB_INNER_MAX_ROWKEY_COLUMN_NUMBER) {
      ret = OB_ERR_UNEXPECTED;

    }
    OB_UNIS_DECODE_ARRAY(datums_, datum_cnt_);
    hash_ = 0;
    group_idx_ = 0;
    store_rowkey_.reset();
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDatumRowkey)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(datums_, datum_cnt_);
  return len;
}


DEF_TO_STRING(ObDatumRowkey)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(datum_cnt), K_(group_idx), K_(hash));
  J_COMMA();
  J_ARRAY_START();
  if (nullptr != buf && buf_len >= 0) {
    if (nullptr != datums_) {
      for (int64_t i = 0; i < datum_cnt_; ++i) {
        if (i > 0) {
          databuff_printf(buf, buf_len, pos, ", ");
        }
        databuff_printf(buf, buf_len, pos, "idx=%ld:", i);
        pos += datums_[i].storage_to_string(buf + pos, buf_len - pos);
      }
    } else {
      J_EMPTY_OBJ();
    }
  }
  J_ARRAY_END();
  J_COMMA();
  J_KV(K_(store_rowkey));
  J_OBJ_END();
  return pos;
}

void ObDatumRowkey::destroy(ObIAllocator &allocator)
{
  if (OB_NOT_NULL(datums_)) {
    allocator.free(datums_);
    datums_ = nullptr;
  }
  reset();
}

// shallow copy the obj value
int ObDatumRowkey::from_rowkey(const ObRowkey &rowkey, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObStorageDatum *datums = nullptr;

  if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;

  } else if (rowkey.is_max_row()) {
    set_max_rowkey();
  } else if (rowkey.is_min_row()) {
    set_min_rowkey();
  } else {
    datum_cnt_ = rowkey.get_obj_cnt();
    if (OB_ISNULL(datums = reinterpret_cast<ObStorageDatum *>(allocator.alloc(sizeof(ObStorageDatum) * datum_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;

    } else  {
      // maybe we do not need the constructor
      datums = new (datums) ObStorageDatum[datum_cnt_];
      datums_ = datums;
      for (int64_t i = 0; OB_SUCC(ret) && i < datum_cnt_; i++) {
        const ObObj &rowkey_obj = rowkey.get_obj_ptr()[i];
        if (rowkey_obj.is_lob_storage() && !rowkey_obj.has_lob_header()) {
          ret = OB_ERR_UNEXPECTED;

        } else if (OB_FAIL(datums[i].from_obj_enhance(rowkey_obj))) {

        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    group_idx_ = 0;
    hash_ = 0;
    store_rowkey_.reset();
    store_rowkey_.get_rowkey() = rowkey;
  } else if (nullptr != datums) {
    allocator.free(datums);
  }

  return ret;
}

int ObDatumRowkey::to_rowkey(ObRowkey &rowkey, const ObObjMeta* obj_metas, common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObObj *objs = nullptr;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;

  } else if (is_max_rowkey()) {
    rowkey.set_max_row();
  } else if (is_min_rowkey()) {
    rowkey.set_min_row();
  } else {
    if (OB_ISNULL(objs = reinterpret_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * datum_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;

    } else  {
      // maybe we do not need the constructor
      objs = new (objs) ObObj[datum_cnt_];
      for (int64_t i = 0; OB_SUCC(ret) && i < datum_cnt_; i++) {
        if (OB_FAIL(datums_[i].to_obj_enhance(objs[i], obj_metas[i]))) {

        }
      }
    }
    if (OB_SUCC(ret)) {
      rowkey = ObRowkey(objs, datum_cnt_);
    } else if (nullptr != objs) {
      allocator.free(objs);
    }
  }
  return ret;
}

int ObDatumRowkey::from_rowkey(const ObRowkey &rowkey, ObStorageDatumBuffer &datum_buffer)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;

  } else if (rowkey.is_max_row()) {
    set_max_rowkey();
  } else if (rowkey.is_min_row()) {
    set_min_rowkey();
  } else if (OB_FAIL(datum_buffer.reserve(rowkey.get_obj_cnt()))) {

  } else {
    ObStorageDatum *datums = datum_buffer.get_datums();
    datum_cnt_ = rowkey.get_obj_cnt();
    datums_ = datums;
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_cnt_; i++) {
      const ObObj &rowkey_obj = rowkey.get_obj_ptr()[i];
      if (rowkey_obj.is_lob_storage() && !rowkey_obj.has_lob_header()) {
        ret = OB_ERR_UNEXPECTED;

      } else if (OB_FAIL(datums[i].from_obj_enhance(rowkey_obj))) {

      }
    }
  }
  if (OB_SUCC(ret)) {
    group_idx_ = 0;
    hash_ = 0;
    store_rowkey_.reset();
    store_rowkey_.get_rowkey() = rowkey;
  }

  return ret;
}


int ObDatumRowkey::to_store_rowkey(const common::ObIArray<share::schema::ObColDesc> &col_descs,
                                   common::ObIAllocator &allocator,
                                   common::ObStoreRowkey &store_rowkey) const
{
  int ret = OB_SUCCESS;
  common::ObObj *objs = nullptr;
  if (is_max_rowkey()) {
    store_rowkey.set_max();
  } else if (is_min_rowkey()) {
    store_rowkey.set_min();
  } else if (OB_UNLIKELY(!is_valid() || col_descs.count() < datum_cnt_)) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_ISNULL(objs = reinterpret_cast<common::ObObj*>(allocator.alloc(sizeof(common::ObObj) * datum_cnt_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_cnt_; i++) {
      if (OB_FAIL(datums_[i].to_obj_enhance(objs[i], col_descs.at(i).col_type_))) {

      } else if (col_descs.at(i).col_type_.is_lob_storage()) {
        objs[i].set_has_lob_header();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(store_rowkey.assign(objs, datum_cnt_))) {

      }
    }
  }

  return ret;
}

int ObDatumRowkey::to_multi_version_rowkey(const bool min_value,
                                           common::ObIAllocator &allocator,
                                           ObDatumRowkey &dest) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;

  } else if (is_max_rowkey()) {
    dest.set_max_rowkey();
  } else if (is_min_rowkey()) {
    dest.set_min_rowkey();
  } else {
    ObStorageDatum *datums = nullptr;
    // FIXME: hard coding
    const int64_t datum_cnt = datum_cnt_  + 1;
    if (OB_ISNULL(datums = (ObStorageDatum*) allocator.alloc(sizeof(ObStorageDatum) * datum_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN, "Failed to alloc memory for multi version rowkey", K(ret), K(datum_cnt));
    } else {
      datums = new (datums) ObStorageDatum[datum_cnt];
      for (int64_t i = 0; i < datum_cnt_; ++ i) {
        datums[i] = datums_[i];
      }
      if (min_value) {
        datums[datum_cnt_].set_min();
      } else {
        datums[datum_cnt_].set_max();
      }
      if (OB_FAIL(dest.assign(datums, datum_cnt))) {

        dest.reset();
        allocator.free(datums);
        datums = nullptr;
      }
    }
  }

  return ret;
}


int ObDatumRowkey::to_multi_version_range(common::ObIAllocator &allocator, ObDatumRange &dest) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_FAIL(to_multi_version_rowkey(true/*min*/, allocator, dest.start_key_))) {

  } else if (OB_FAIL(to_multi_version_rowkey(false/*max*/, allocator, dest.end_key_))) {

  } else {
    dest.border_flag_.unset_inclusive_end();
    dest.border_flag_.unset_inclusive_start();
    dest.set_group_idx(group_idx_);
  }

  return ret;
}

void ObDatumRowkey::reuse()
{
  group_idx_ = 0;
  store_rowkey_.reset();
  hash_ = 0;
  for (int64_t i = 0; i < datum_cnt_; ++i) {
    datums_[i].reuse();
  }
}

int ObDiscreteDatumRowkey::compare(const ObDatumRowkey &rhs, const ObStorageDatumUtils &datum_utils, int &cmp_ret,
                                   const bool compare_datum_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || !rhs.is_valid() || !datum_utils.is_valid())) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_FAIL(rowkey_vector_->compare_rowkey(rhs, row_idx_, datum_utils, cmp_ret, compare_datum_cnt))) {

  }
  return ret;
}

int ObDiscreteDatumRowkey::compare(const ObDiscreteDatumRowkey &rhs, const ObStorageDatumUtils &datum_utils, int &cmp_ret,
                                   const bool compare_datum_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || !rhs.is_valid() || !datum_utils.is_valid())) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_FAIL(rowkey_vector_->compare_rowkey(rhs, row_idx_, datum_utils, cmp_ret, compare_datum_cnt))) {

  }
  return ret;
}

int ObDiscreteDatumRowkey::compare(const ObCommonDatumRowkey &rhs, const ObStorageDatumUtils &datum_utils, int &cmp_ret,
                                   const bool compare_datum_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || !rhs.is_valid() || !datum_utils.is_valid())) {
    ret = OB_INVALID_ARGUMENT;

  } else if (rhs.is_compact_rowkey()) {
    ret = compare(*rhs.get_compact_rowkey(), datum_utils, cmp_ret, compare_datum_cnt);
  } else {
    ret = compare(*rhs.get_discrete_rowkey(), datum_utils, cmp_ret, compare_datum_cnt);
  }
  return ret;
}

int ObDiscreteDatumRowkey::deep_copy(ObDatumRowkey &dest, common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;

  } else {
    char *buf = nullptr;
    int64_t deep_copy_size = 0;
    if (OB_FAIL(rowkey_vector_->get_deep_copy_rowkey_size(row_idx_, deep_copy_size))) {

    } else if (OB_UNLIKELY(deep_copy_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;

    } else if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(deep_copy_size)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;

    } else if (OB_FAIL(rowkey_vector_->deep_copy_rowkey(row_idx_, dest, buf, deep_copy_size))) {

    }
    if (OB_FAIL(ret) && nullptr != buf) {
      dest.reset();
      allocator.free(buf);
    }
  }
  return ret;
}

int ObDiscreteDatumRowkey::get_column_int(const int64_t col_idx, int64_t &int_val) const
{
  return rowkey_vector_->get_column_int(row_idx_, col_idx, int_val); 
}

int ObCommonDatumRowkey::compare(const ObDatumRowkey &rhs, const ObStorageDatumUtils &datum_utils, int &cmp_ret,
                                 const bool compare_datum_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || !rhs.is_valid() || !datum_utils.is_valid())) {
    ret = OB_INVALID_ARGUMENT;

  } else if (is_compact_rowkey()) {
    if (OB_FAIL(rowkey_->compare(rhs, datum_utils, cmp_ret, compare_datum_cnt))) {

    }
  } else if (OB_FAIL(discrete_rowkey_->compare(rhs, datum_utils, cmp_ret, compare_datum_cnt))) {

  }
  return ret;
}


int ObCommonDatumRowkey::compare(const ObCommonDatumRowkey &rhs, const ObStorageDatumUtils &datum_utils, int &cmp_ret,
                                 const bool compare_datum_cnt) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid() || !rhs.is_valid() || !datum_utils.is_valid())) {
    ret = OB_INVALID_ARGUMENT;

  } else if (is_compact_rowkey()) {
    if (OB_FAIL(rhs.compare(*rowkey_, datum_utils, cmp_ret, compare_datum_cnt))) {

    } else {
      cmp_ret = -cmp_ret;
    }
  } else {
    ret = discrete_rowkey_->compare(rhs, datum_utils, cmp_ret, compare_datum_cnt);
  }
  return ret;
}

int ObCommonDatumRowkey::deep_copy(ObDatumRowkey &dest, common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;

  } else if (is_compact_rowkey()) {
    ret = rowkey_->deep_copy(dest, allocator);
  } else {
    ret = discrete_rowkey_->deep_copy(dest, allocator);
  }
  return ret;
}

int ObCommonDatumRowkey::get_column_int(const int64_t col_idx, int64_t &int_val) const
{
  int ret = OB_SUCCESS;
  int_val = 0;
  if (is_compact_rowkey()) {
    int_val = rowkey_->datums_[col_idx].get_int();
  } else {
    ret = discrete_rowkey_->get_column_int(col_idx, int_val);
  }
  return ret;
}

DEF_TO_STRING(ObCommonDatumRowkey)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(type), K_(key_ptr));
  J_COMMA();
  if (is_compact_rowkey()) {
    J_KV(KPC_(rowkey));
  } else if (is_discrete_rowkey()) {
    J_KV(KPC_(discrete_rowkey));
  }
  J_OBJ_END();
  return pos;
}

int ObDatumRowkeyHelper::convert_datum_rowkey(const common::ObRowkey &rowkey, ObDatumRowkey &datum_rowkey)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_FAIL(datum_rowkey.from_rowkey(rowkey, datum_buffer_))) {

  }

  return ret;
}

int ObDatumRowkeyHelper::convert_store_rowkey(const ObDatumRowkey &datum_rowkey,
                                              const ObIArray<share::schema::ObColDesc> &col_descs,
                                              common::ObStoreRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *objs = nullptr;

  if (OB_UNLIKELY(!datum_rowkey.is_valid() || col_descs.count() < datum_rowkey.get_datum_cnt())) {
    ret = OB_INVALID_ARGUMENT;

  } else if (!obj_buffer_.is_inited() && OB_FAIL(obj_buffer_.init(allocator_))) {

  } else if (OB_FAIL(obj_buffer_.reserve(datum_rowkey.get_datum_cnt()))) {

  } else if (OB_ISNULL(objs = obj_buffer_.get_data())) {
    ret = OB_ERR_UNEXPECTED;

  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_rowkey.get_datum_cnt(); i++) {
      if (OB_FAIL(datum_rowkey.datums_[i].to_obj_enhance(objs[i], col_descs.at(i).col_type_))) {

      } else if (col_descs.at(i).col_type_.is_lob_storage()) {
        objs[i].set_has_lob_header();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rowkey.assign(objs, datum_rowkey.get_datum_cnt()))) {

      }
    }
  }

  return ret;
}

int ObDatumRowkeyHelper::prepare_datum_rowkey(const ObDatumRow &datum_row,
                                              const int key_datum_cnt,
                                              const ObIArray<share::schema::ObColDesc> &col_descs,
                                              ObDatumRowkey &datum_rowkey)
{
  int ret = OB_SUCCESS;

  if (!datum_row.is_valid() || col_descs.count() < datum_row.get_column_count()) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_FAIL(datum_rowkey.assign(datum_row.storage_datums_, key_datum_cnt))) {

  } else if (OB_FAIL(convert_store_rowkey(datum_rowkey, col_descs, datum_rowkey.store_rowkey_))) {

  }

  return ret;
}


int ObDatumRowkeyHelper::reserve(const int64_t rowkey_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(rowkey_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;

  } else if (datum_buffer_.get_capacity() >= rowkey_cnt) {
  } else if (OB_FAIL(datum_buffer_.reserve(rowkey_cnt))) {

  }

  return ret;
}

} // namespace blocksstable
} // namespace oceanbase
