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

#include "ob_admin_cmp_micro_executor.h"
#include "lib/compress/ob_compressor_pool.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
namespace tools
{

int ObAdminCmpMicroExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(parse_cmd(argc, argv))) {

  } else if (OB_FAIL(open_file())) {

  } else if (OB_FAIL(read_header())){

  } else if (OB_FAIL(compare_micro())) {

  }
  return ret;
}

int ObAdminCmpMicroExecutor::open_file()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
    if (0 > (fd_[i] = ::open(data_file_path_[i], O_RDWR))) {
      ret = OB_IO_ERROR;

    } else {

    }
  }
  return ret;
}

int ObAdminCmpMicroExecutor::read_header()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
    int64_t pos = 0;
    ObMacroBlockCommonHeader commo_header;
    if (NULL == (macro_buf_[i] = (char*)::malloc(MACRO_BLOCK_SIZE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;

    } else if (NULL == (uncomp_micro_[i] = (char*)::malloc(MACRO_BLOCK_SIZE))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;

    } else if (0 > pread(fd_[i], macro_buf_[i], MACRO_BLOCK_SIZE, macro_id_[i] * MACRO_BLOCK_SIZE)) {
      ret = OB_IO_ERROR;

    } else if (OB_FAIL(commo_header.deserialize(macro_buf_[i], MACRO_BLOCK_SIZE, pos))) {

    } else {
      header_[i] = reinterpret_cast<ObSSTableMacroBlockHeader*>(macro_buf_[i] + pos);
    }
  }
  if (OB_SUCC(ret) && (header_[0]->micro_block_count_ != header_[1]->micro_block_count_
                      || 0 != strcmp(header_[0]->compressor_name_, header_[1]->compressor_name_))) {
    ret = OB_ERR_UNEXPECTED;

  }
  return ret;
}

int ObAdminCmpMicroExecutor::compare_micro()
{
  int ret = OB_SUCCESS;
  char *micro_data[2];
  micro_data[0] = macro_buf_[0] + header_[0]->micro_block_data_offset_;
  micro_data[1] = macro_buf_[1] + header_[1]->micro_block_data_offset_;
  for (int64_t i = 0; OB_SUCC(ret) && i < header_[0]->micro_block_count_; ++i) {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < 2; ++idx) {
      if (OB_FAIL(read_micro(micro_data[idx], idx))) {

      }
    }
    if (record_header_[0].data_zlength_ != record_header_[1].data_zlength_
        || record_header_[0].data_length_ != record_header_[1].data_length_) {

    }
    if (0 != MEMCMP(micro_buf_[0], micro_buf_[1], record_header_[0].data_zlength_)) {

    }
    if (record_header_[0].data_length_ != record_header_[0].data_zlength_) {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < 2; ++idx) {
        if (OB_FAIL(decompress_micro(idx))) {

        }
      }
      if (0 != MEMCMP(uncomp_micro_[0], uncomp_micro_[1], record_header_[0].data_length_)) {
        print_micro_meta(0);
        print_micro_meta(1);
        for (int64_t c = 0; c < record_header_[0].data_length_; ++c) {
          if (uncomp_micro_[0][c] != uncomp_micro_[1][c]) {

            break;
          }
        }

      }
    }
  }
  return ret;
}

int ObAdminCmpMicroExecutor::read_micro(char *&micro_data, const int64_t idx)
{
  char *cur_micro = micro_data;
  record_header_[idx] = *reinterpret_cast<ObRecordHeaderV3*>(micro_data);
  micro_data += record_header_[idx].get_serialize_size();
  micro_buf_[idx] = micro_data;
  micro_data += record_header_[idx].data_zlength_;
  return ObRecordHeaderV3::deserialize_and_check_record(cur_micro, record_header_[idx].data_zlength_ + record_header_[idx].get_serialize_size(), MICRO_BLOCK_HEADER_MAGIC);
}

int ObAdminCmpMicroExecutor::decompress_micro(const int64_t idx)
{
  int ret = OB_SUCCESS;
  ObCompressor *compressor = NULL;
  int64_t uncomp_size = 0;
  if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(header_[idx]->compressor_name_, compressor))) {

  } else if (OB_FAIL(compressor->decompress(micro_buf_[idx], record_header_[idx].data_zlength_, uncomp_micro_[idx], MACRO_BLOCK_SIZE, uncomp_size))) {

  } else if (uncomp_size != record_header_[idx].data_length_) {
    ret = OB_ERR_UNEXPECTED;

  } else {

  }
  return ret;
}

int ObAdminCmpMicroExecutor::parse_cmd(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  int opt = 0;
  const char* opt_string = "a:b:c:d:";

  struct option longopts[] = {
    // commands
    { "data_dir_0", 1, NULL, 'a' },
    // options
    { "data_dir_1", 1, NULL, 'b' },
    { "macro-id-0", 1, NULL, 'c' },
    { "macro-id-1", 1, NULL, 'd' },
  };

  int index = -1;
  while ((opt = getopt_long(argc, argv, opt_string, longopts, &index)) != -1) {
    switch (opt) {
    case 'a': {
      if (OB_FAIL(databuff_printf(data_file_path_[0], OB_MAX_FILE_NAME_LENGTH, "%s/%s/%s",
          optarg, BLOCK_SSTBALE_DIR_NAME, BLOCK_SSTBALE_FILE_NAME))) {

      }
      break;
    }
    case 'b': {
      if (OB_FAIL(databuff_printf(data_file_path_[1], OB_MAX_FILE_NAME_LENGTH, "%s/%s/%s",
          optarg, BLOCK_SSTBALE_DIR_NAME, BLOCK_SSTBALE_FILE_NAME))) {

      }
      break;
    }
    case 'c': {
      macro_id_[0] = strtoll(optarg, NULL, 10);
      break;
    }
    case 'd': {
      macro_id_[1] = strtoll(optarg, NULL, 10);
      break;
    }
    default: {
      exit(1);
    }
    }
  }
  return ret;
}

void ObAdminCmpMicroExecutor::print_micro_meta(const int64_t idx)
{
  ObMicroBlockHeader *micro_header = reinterpret_cast<ObMicroBlockHeader*>(uncomp_micro_[idx]);
  const int64_t encoding_meta_offset = sizeof(ObMicroBlockHeader) + sizeof(ObColumnHeader) * header_[idx]->column_count_;

}

}
}
