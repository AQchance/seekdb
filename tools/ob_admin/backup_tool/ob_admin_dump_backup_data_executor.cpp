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

#include "ob_admin_dump_backup_data_executor.h"

#include "src/share/ob_device_manager.h"
#include "../dumpsst/ob_admin_dumpsst_print_helper.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::backup;
using namespace oceanbase::blocksstable;
using namespace oceanbase::rootserver;
#define HELP_FMT "\t%-30s%-12s\n"

namespace oceanbase {
namespace tools {

/* ObAdminDumpBackupDataUtil */

int ObAdminDumpBackupDataUtil::read_archive_info_file(const common::ObString &backup_path, const common::ObString &storage_info_str,
    share::ObIBackupSerializeProvider &serializer)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t file_length = 0;
  char * buf = nullptr;
  int64_t read_size = 0;
  ObArenaAllocator allocator;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  ObBackupSerializeHeaderWrapper serializer_wrapper(&serializer);
  if (OB_FAIL(storage_info.set(backup_path.ptr(), storage_info_str.ptr()))) {

  } else if (OB_FAIL(util.get_file_length(backup_path.ptr(), &storage_info, file_length))) {
    if (OB_OBJECT_NOT_EXIST != ret) {

    } else {

    }
  } else if (0 == file_length) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(util.read_single_file(backup_path.ptr(), &storage_info, buf, file_length, read_size,
                                           ObStorageIdMod::get_default_id_mod()))) {

  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_FAIL(serializer_wrapper.deserialize(buf, file_length, pos))) {

  } else {

  }
  return ret;
}

int ObAdminDumpBackupDataUtil::read_backup_file_header(const common::ObString &backup_path, const common::ObString &storage_info_str,
    backup::ObBackupFileHeader &file_header)
{
  int ret = OB_SUCCESS;
  file_header.reset();
  char *buf = NULL;
  int64_t file_length = 0;
  ObArenaAllocator allocator;
  const int64_t header_len = sizeof(ObBackupFileHeader);
  if (OB_FAIL(get_backup_file_length(backup_path, storage_info_str, file_length))) {

  } else if (OB_UNLIKELY(file_length <= sizeof(header_len))) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(header_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(pread_file(backup_path, storage_info_str, 0, header_len, buf))) {

  } else {
    ObBufferReader buffer_reader(buf, header_len);
    const ObBackupFileHeader *header = NULL;
    if (OB_FAIL(buffer_reader.get(header))) {

    } else if (OB_ISNULL(header)) {
      ret = OB_ERR_UNEXPECTED;

    } else {
      file_header = *header;
    }
  }

  return ret;
}

int ObAdminDumpBackupDataUtil::read_data_file_trailer(const common::ObString &backup_path,
    const common::ObString &storage_info_str, backup::ObBackupDataFileTrailer &file_trailer)
{
  int ret = OB_SUCCESS;
  file_trailer.reset();
  char *buf = NULL;
  int64_t file_length = 0;
  ObArenaAllocator allocator;
  const int64_t trailer_len = sizeof(ObBackupDataFileTrailer);
  if (OB_FAIL(get_backup_file_length(backup_path, storage_info_str, file_length))) {

  } else if (OB_UNLIKELY(file_length <= sizeof(trailer_len))) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(trailer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(pread_file(backup_path, storage_info_str, file_length - trailer_len, trailer_len, buf))) {

  } else {
    ObBufferReader buffer_reader(buf, trailer_len);
    const ObBackupDataFileTrailer *trailer = NULL;
    if (OB_FAIL(buffer_reader.get(trailer))) {

    } else if (OB_ISNULL(trailer)) {
      ret = OB_ERR_UNEXPECTED;

    } else if (OB_FAIL(trailer->check_valid())) {

    } else {
      file_trailer = *trailer;

    }
  }
  return ret;
}

int ObAdminDumpBackupDataUtil::read_index_file_trailer(const common::ObString &backup_path,
    const common::ObString &storage_info_str, backup::ObBackupMultiLevelIndexTrailer &index_trailer)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t file_length = 0;
  ObArenaAllocator allocator;
  const int64_t trailer_len = sizeof(ObBackupMultiLevelIndexTrailer);
  if (OB_FAIL(get_backup_file_length(backup_path, storage_info_str, file_length))) {

  } else if (OB_UNLIKELY(file_length <= sizeof(trailer_len))) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(trailer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(pread_file(backup_path, storage_info_str, file_length - trailer_len, trailer_len, buf))) {

  } else {
    ObBufferReader buffer_reader(buf, trailer_len);
    const ObBackupMultiLevelIndexTrailer *trailer = NULL;
    if (OB_FAIL(buffer_reader.get(trailer))) {

    } else if (OB_ISNULL(trailer)) {
      ret = OB_ERR_UNEXPECTED;

    } else {
      index_trailer = *trailer;
    }
  }
  return ret;
}

int ObAdminDumpBackupDataUtil::read_tablet_metas_file_trailer(const common::ObString &backup_path, 
    const common::ObString &storage_info_str, backup::ObTabletInfoTrailer &tablet_meta_trailer)
{
  int ret = OB_SUCCESS;
  int64_t file_length = 0;
  char *buf = NULL;
  ObArenaAllocator allocator;
  ObBackupSerializeHeaderWrapper serializer_wrapper(&tablet_meta_trailer);
  const int64_t trailer_len = serializer_wrapper.get_serialize_size();
  int64_t pos = 0;
  if (OB_FAIL(get_backup_file_length(backup_path, storage_info_str, file_length))) {

  } else if (OB_UNLIKELY(file_length <= trailer_len)) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(trailer_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(pread_file(backup_path, storage_info_str, file_length - trailer_len, trailer_len, buf))) {

  } else if (OB_FAIL(serializer_wrapper.deserialize(buf, trailer_len, pos))) {

  } 
  return ret;
}

int ObAdminDumpBackupDataUtil::pread_file(const common::ObString &backup_path, const common::ObString &storage_info_str,
    const int64_t offset, const int64_t read_size, char *buf)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  int64_t real_read_size = 0;
  share::ObBackupStorageInfo storage_info;
  if (OB_UNLIKELY(0 == backup_path.length())) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_UNLIKELY(read_size <= 0)) {

  } else if (OB_FAIL(storage_info.set(backup_path.ptr(), storage_info_str.ptr()))) {

  } else if (OB_FAIL(util.read_part_file(backup_path, &storage_info, buf, read_size, offset, real_read_size,
                                         ObStorageIdMod::get_default_id_mod()))) {

  } else if (OB_UNLIKELY(real_read_size != read_size)) {
    ret = OB_ERR_UNEXPECTED;

  }
  return ret;
}

int ObAdminDumpBackupDataUtil::get_backup_file_length(
    const common::ObString &backup_path, const common::ObString &storage_info_str, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  file_length = 0;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  if (OB_FAIL(storage_info.set(backup_path.ptr(), storage_info_str.ptr()))) {

  } else if (OB_FAIL(util.get_file_length(backup_path, &storage_info, file_length))) {

  }
  return ret;
}

int ObAdminDumpBackupDataUtil::get_common_header(
    blocksstable::ObBufferReader &buffer_reader, const share::ObBackupCommonHeader *&common_header)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buffer_reader.get(common_header))) {

  } else if (OB_ISNULL(common_header)) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_FAIL(common_header->check_valid())) {

  } else if (common_header->data_zlength_ > buffer_reader.remain()) {
    ret = OB_BUF_NOT_ENOUGH;

  } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_zlength_))) {

  }
  return ret;
}

template <class IndexType>
int ObAdminDumpBackupDataUtil::parse_from_index_blocks(blocksstable::ObBufferReader &buffer_reader,
    const std::function<int(const share::ObBackupCommonHeader &)> &print_func1,
    const std::function<int(const common::ObIArray<IndexType> &)> &print_func2)
{
  int ret = OB_SUCCESS;
  ObArray<IndexType> index_list;
  const share::ObBackupCommonHeader *common_header = NULL;
  while (OB_SUCC(ret) && buffer_reader.remain() > 0) {
    common_header = NULL;
    index_list.reset();
    int64_t start_pos = buffer_reader.pos();
    int64_t data_zlength = 0;
    if (OB_FAIL(ObAdminDumpBackupDataUtil::get_common_header(buffer_reader, common_header))) {

    } else {
      data_zlength = common_header->data_zlength_;
      const int64_t original_size = common_header->data_length_;
      const ObCompressorType &compressor_type = static_cast<ObCompressorType>(common_header->compressor_type_);
      ObBufferReader new_buffer_reader(buffer_reader.data(), data_zlength);
      if (OB_FAIL(uncompress_and_decode_block(compressor_type, data_zlength, original_size, new_buffer_reader, index_list))) {

      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(print_func1(*common_header))) {

      } else if (OB_FAIL(print_func2(index_list))) {

      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(buffer_reader.advance(data_zlength + common_header->align_length_))) {

      }
    }
  }
  return ret;
}

template <class IndexType>
int ObAdminDumpBackupDataUtil::parse_from_data_file_index_blocks(
    blocksstable::ObBufferReader &buffer_reader, common::ObIArray<IndexType> &index_list)
{
  int ret = OB_SUCCESS;
  index_list.reset();
  const share::ObBackupCommonHeader *common_header = NULL;
  while (OB_SUCC(ret) && buffer_reader.remain() > 0) {
    common_header = NULL;
    int64_t start_pos = buffer_reader.pos();
    if (OB_FAIL(ObAdminDumpBackupDataUtil::get_common_header(buffer_reader, common_header))) {

    } else {
      int64_t end_pos = buffer_reader.pos() + common_header->data_length_;
      for (int64_t i = 0; OB_SUCC(ret) && buffer_reader.pos() < end_pos; ++i) {
        IndexType index;
        if (OB_FAIL(buffer_reader.read_serialize(index))) {

        } else if (OB_FAIL(index_list.push_back(index))) {

        }
      }
    }
    if (OB_SUCC(ret) && common_header->align_length_ > 0) {
      if (OB_FAIL(buffer_reader.advance(common_header->align_length_))) {

      }
    }
  }
  return ret;
}

template <class IndexType>
int ObAdminDumpBackupDataUtil::parse_from_index_index_blocks(blocksstable::ObBufferReader &buffer_reader,
    const std::function<int(const share::ObBackupCommonHeader &)> &print_func1,
    const std::function<int(const backup::ObBackupMultiLevelIndexHeader &)> &print_func2,
    const std::function<int(const common::ObIArray<IndexType> &)> &print_func3)
{
  int ret = OB_SUCCESS;
  ObArray<IndexType> index_list;
  const share::ObBackupCommonHeader *common_header = NULL;
  while (OB_SUCC(ret) && buffer_reader.remain() > 0) {
    common_header = NULL;
    index_list.reset();
    int64_t start_pos = buffer_reader.pos();
    backup::ObBackupMultiLevelIndexHeader index_header;
    int64_t data_zlength = 0;
    if (OB_FAIL(ObAdminDumpBackupDataUtil::get_common_header(buffer_reader, common_header))) {

    } else {
      const int64_t pos = buffer_reader.pos();
      if (OB_FAIL(buffer_reader.read_serialize(index_header))) {

      } else {
        data_zlength = common_header->data_zlength_ - (buffer_reader.pos() - pos);
        const int64_t original_size = common_header->data_length_ - (buffer_reader.pos() - pos);
        const ObCompressorType &compressor_type = static_cast<ObCompressorType>(common_header->compressor_type_);
        ObBufferReader new_buffer_reader(buffer_reader.data() + buffer_reader.pos(), data_zlength);
        if (OB_FAIL(uncompress_and_decode_block(compressor_type, data_zlength, original_size, new_buffer_reader, index_list))) {

        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(print_func1(*common_header))) {

      } else if (OB_FAIL(print_func2(index_header))) {

      } else if (OB_FAIL(print_func3(index_list))) {

      } else {
        index_list.reset();
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(buffer_reader.advance(data_zlength + common_header->align_length_))) {

      } else {

      }
    }
  }
  return ret;
}

template <class IndexType, class IndexIndexType>
int ObAdminDumpBackupDataUtil::parse_from_index_index_blocks(blocksstable::ObBufferReader &buffer_reader,
    const std::function<int(const share::ObBackupCommonHeader &)> &print_func1,
    const std::function<int(const backup::ObBackupMultiLevelIndexHeader &)> &print_func2,
    const std::function<int(const common::ObIArray<IndexType> &)> &print_func3,
    const std::function<int(const common::ObIArray<IndexIndexType> &)> &print_func4)
{
  int ret = OB_SUCCESS;
  ObArray<IndexType> index_list;
  ObArray<IndexIndexType> index_index_list;
  const share::ObBackupCommonHeader *common_header = NULL;
  while (OB_SUCC(ret) && buffer_reader.remain() > 0) {
    common_header = NULL;
    index_list.reset();
    int64_t start_pos = buffer_reader.pos();
    backup::ObBackupMultiLevelIndexHeader index_header;
    int64_t data_zlength = 0;
    if (OB_FAIL(ObAdminDumpBackupDataUtil::get_common_header(buffer_reader, common_header))) {

    } else {
      const int64_t pos = buffer_reader.pos();
      if (OB_FAIL(buffer_reader.read_serialize(index_header))) {

      } else {
        data_zlength = common_header->data_zlength_ - (buffer_reader.pos() - pos);
        const int64_t original_size = common_header->data_length_ - (buffer_reader.pos() - pos);
        const ObCompressorType &compressor_type = static_cast<ObCompressorType>(common_header->compressor_type_);
        ObBufferReader new_buffer_reader(buffer_reader.data() + buffer_reader.pos(), data_zlength);
        if (OB_BACKUP_MULTI_LEVEL_INDEX_BASE_LEVEL == index_header.index_level_) {
          if (OB_FAIL(uncompress_and_decode_block(compressor_type, data_zlength, original_size, new_buffer_reader, index_list))) {

          }
        } else {
          if (OB_FAIL(uncompress_and_decode_block(compressor_type, data_zlength, original_size, new_buffer_reader, index_index_list))) {

          }
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(print_func1(*common_header))) {

      } else if (OB_FAIL(print_func2(index_header))) {

      } else {
        if (OB_BACKUP_MULTI_LEVEL_INDEX_BASE_LEVEL == index_header.index_level_) {
          if (OB_FAIL(print_func3(index_list))) {

          } else {
            index_list.reset();
          }
        } else {
          if (OB_FAIL(print_func4(index_index_list))) {

          } else {
            index_index_list.reset();
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(buffer_reader.advance(data_zlength + common_header->align_length_))) {

      } else {

      }
    }
  }
  return ret;
}

template <typename BackupSmallFileType>
int ObAdminDumpBackupDataUtil::read_backup_info_file(const common::ObString &backup_path, const common::ObString &storage_info_str,
    BackupSmallFileType &file_info)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t file_length = 0;
  char *buf = nullptr;
  int64_t read_size = 0;
  ObArenaAllocator allocator;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  ObBackupSerializeHeaderWrapper serializer_wrapper(&file_info);

  if (OB_FAIL(storage_info.set(backup_path.ptr(), storage_info_str.ptr()))) {

  } else if (OB_FAIL(util.get_file_length(backup_path.ptr(), &storage_info, file_length))) {
    if (OB_OBJECT_NOT_EXIST != ret) {

    } else {

    }
  } else if (0 == file_length) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator.alloc(file_length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(util.read_single_file(ObString(backup_path.ptr()), &storage_info, buf,
                          file_length, read_size, ObStorageIdMod::get_default_id_mod()))) {

  } else if (file_length != read_size) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_FAIL(serializer_wrapper.deserialize(buf, file_length, pos))) {

  } else {

  }
  return ret;
}

template <class IndexType>
int ObAdminDumpBackupDataUtil::uncompress_and_decode_block(
    const ObCompressorType &compressor_type, const int64_t data_zlength, const int64_t original_size,
    ObBufferReader &buffer_reader, ObIArray<IndexType> &index_list)
{
  int ret = OB_SUCCESS;
  ObBackupIndexBlockCompressor compressor;
  const int64_t block_size = OB_BACKUP_COMPRESS_BLOCK_SIZE;
  const char *out_buf = NULL;
  int64_t out_size = 0;
  if (OB_FAIL(compressor.init(block_size, compressor_type))) {

  } else if (OB_FAIL(compressor.decompress(buffer_reader.data(),
      data_zlength, original_size, out_buf, out_size))) {

  } else {
    ObBufferReader new_buffer_reader(out_buf, out_size);
    IndexType index;
    while (OB_SUCC(ret) && new_buffer_reader.remain() > 0) {
      index.reset();
      if (OB_FAIL(new_buffer_reader.read_serialize(index))) {

      } else if (OB_FAIL(index_list.push_back(index))) {

      }
    }
  }
  return ret;
}

/* ObAdminDumpBackupDataExecutor */

ObAdminDumpBackupDataExecutor::ObAdminDumpBackupDataExecutor() : offset_(), length_(), file_type_(), is_quiet_(false),
    allocator_()
{
  MEMSET(backup_path_, 0, common::OB_MAX_URI_LENGTH);
  MEMSET(storage_info_, 0, common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH);
}

ObAdminDumpBackupDataExecutor::~ObAdminDumpBackupDataExecutor()
{}

int ObAdminDumpBackupDataExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  lib::set_memory_limit(96 * 1024 * 1024 * 1024LL);
  lib::set_tenant_memory_limit(500, 96 * 1024 * 1024 * 1024LL);
  int64_t data_type = 0;
  ObBackupDestType::TYPE path_type;
  if (OB_FAIL(parse_cmd_(argc, argv))) {

  } else if (is_quiet_) {
    OB_LOGGER.set_log_level("ERROR");
  } else {
    OB_LOGGER.set_log_level("INFO");
  }

  if (FAILEDx(ObDeviceManager::get_instance().init_devices_env())) {

  } else if (OB_FAIL(ObIOManager::get_instance().init())) {

  } else if (OB_FAIL(ObIOManager::get_instance().start())) {

  } else if (OB_FAIL(ObObjectStorageInfo::register_cluster_version_mgr(&ObClusterVersionBaseMgr::get_instance()))) {

  } else if (check_exist_) {
    // ob_admin dump_backup -d'xxxxx' -c
    if (OB_FAIL(do_check_exist_())) {

    }
  } else if (OB_FAIL(check_tenant_backup_path_type_(backup_path_, storage_info_, path_type))) {

  } else if (path_type == ObBackupDestType::TYPE::DEST_TYPE_BACKUP_DATA) {
    // if backup path is tenant backup path. dump the tenant backup infos of the latest backup set dir.
    if (OB_FAIL(dump_tenant_backup_path_())) {

    }
  } else if (path_type == ObBackupDestType::TYPE::DEST_TYPE_ARCHIVE_LOG) {
    // if backup path is tenant archive path. dump the archive round info.
    if (OB_FAIL(dump_tenant_archive_path_())) {

    }
  } else {
    bool is_table_list_dir = false;
    if (OB_FAIL(check_is_table_list_dir_(is_table_list_dir))) {

    } 
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(check_file_exist_(backup_path_, storage_info_))) {

    } else if (OB_FAIL(get_backup_file_type_())) {

    } else if (OB_FAIL(do_execute_())) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::parse_cmd_(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  int opt = 0;
  int index = -1;
  const char *opt_str = "h:d:s:o:l:qce:i:";
  struct option longopts[] = {{"help", 0, NULL, 'h'},
      {"backup_path", 1, NULL, 'd'},
      {"storage_info", 1, NULL, 's'},
      {"file_uri", 1, NULL, 'f'},
      {"offset", 1, NULL, 'o'},
      {"quiet", 0, NULL, 'q' },
      {"check_exist", 0, NULL, 'c'},
      {"length", 1, NULL, 'l'},
      {"s3_url_encode_type", 0, NULL, 'e'},
      {"sts_credential", 0, NULL, 'i'},
      {NULL, 0, NULL, 0}};
  while (OB_SUCC(ret) && -1 != (opt = getopt_long(argc, argv, opt_str, longopts, &index))) {
    switch (opt) {
      case 'h': {
        print_usage_();
        exit(1);
      }
      case 'd': {
        if (OB_FAIL(databuff_printf(backup_path_, sizeof(backup_path_), "%s", optarg))) {

        }
        break;
      }
      case 's': {
        if (OB_FAIL(databuff_printf(storage_info_, sizeof(storage_info_), "%s", optarg))) {

        }
        break;
      }
      case 'f': {
        if (OB_FAIL(get_backup_file_path_())) {

        }
        break;
      }
      case 'o': {
        offset_ = strtoll(optarg, NULL, 10);
        break;
      }
      case 'q': {
        is_quiet_ = true;
        break;
      }
      case 'c': {
        check_exist_ = true;
        break;
      }
      case 'l': {
        length_ = strtoll(optarg, NULL, 10);
        break;
      }
      case 'e': {
        if (OB_FAIL(set_s3_url_encode_type(optarg))) {

        }
        break;
      }
      case 'i': {
        if (OB_FAIL(set_sts_credential_key(optarg))) {

        }
        break;
      }
      default: {
        print_usage_();
        exit(1);
      }
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::do_check_exist_()
{
  int ret = OB_SUCCESS;
  bool is_exist = true; ;
  if (OB_FAIL(check_file_exist_(backup_path_, storage_info_))) {
    if (OB_OBJECT_NOT_EXIST == ret) {
      is_exist = false;
      ret = OB_SUCCESS;
    } else {

    }
  }

  if (OB_SUCC(ret) && !is_exist) {
    if (OB_FAIL(check_dir_exist_(backup_path_, storage_info_, is_exist))) {

    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dump_check_exist_result_(backup_path_, storage_info_, is_exist))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::check_dir_exist_(
    const char *backup_path, const char *storage_info_str, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  bool is_empty_dir = true;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  if (OB_FAIL(storage_info.set(backup_path, storage_info_str))) {

  } else if (OB_FAIL(util.is_empty_directory(backup_path, &storage_info, is_empty_dir))) {
    if (OB_IO_ERROR == ret) {
      is_exist = false;
      ret = OB_SUCCESS;

    } else {

    }
  } else if (!is_empty_dir) {
    is_exist = true;
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::check_file_exist_(const char *backup_path, const char *storage_info_str)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  if (OB_FAIL(storage_info.set(backup_path, storage_info_str))) {

  } else if (OB_FAIL(util.is_exist(backup_path, &storage_info, exist))) {

  } else if (OB_UNLIKELY(!exist)) {
    ret = OB_OBJECT_NOT_EXIST;

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::do_execute_()
{
  int ret = OB_SUCCESS;
  switch (file_type_) {
    case backup::ObBackupFileType::BACKUP_DATA_FILE: {
      if (OB_FAIL(print_backup_data_file_())) {

      }
      break;
    }
    case backup::ObBackupFileType::BACKUP_MACRO_RANGE_INDEX_FILE: {
      if (OB_FAIL(print_macro_range_index_file())) {

      }
      break;
    }
    case backup::ObBackupFileType::BACKUP_META_INDEX_FILE: {
      if (OB_FAIL(print_meta_index_file())) {

      }
      break;
    }
    case backup::ObBackupFileType::BACKUP_MACRO_BLOCK_INDEX_FILE: {
      if (OB_FAIL(print_macro_block_index_file())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_LS_INFO: {
      if (OB_FAIL(print_ls_attr_info_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_TABLET_TO_LS_INFO: {
      if (OB_FAIL(print_tablet_to_ls_info_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_DELETED_TABLET_INFO: {
      if (OB_FAIL(print_deleted_tablet_info_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_TENANT_LOCALITY_INFO: {
      if (OB_FAIL(print_tenant_locality_info_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_PARAMETERS_INFO: {
      if (OB_FAIL(print_parameters_info_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_SET_INFO: {
      if (OB_FAIL(print_backup_set_info_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_TENANT_DIAGNOSE_INFO: {
      if (OB_FAIL(print_tenant_diagnose_info_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_DATA_PLACEHOLDER: {
      if (OB_FAIL(print_place_holder_info_())) {

      }
      break;
    }
    // archive
    case share::ObBackupFileType::BACKUP_ARCHIVE_ROUND_START_INFO: {
      if (OB_FAIL(print_archive_round_start_file_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_ARCHIVE_ROUND_END_INFO: {
      if (OB_FAIL(print_archive_round_end_file_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_ARCHIVE_PIECE_START_INFO: {
      if (OB_FAIL(print_archive_piece_start_file_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_ARCHIVE_PIECE_END_INFO: {
      if (OB_FAIL(print_archive_piece_end_file_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_ARCHIVE_SINGLE_PIECE_INFO: {
      if (OB_FAIL(print_archive_single_piece_file_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_PIECE_INNER_PLACEHOLDER_INFO: {
      if (OB_FAIL(print_archive_piece_inner_placeholder_file_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_PIECE_SINGLE_LS_FILE_LIST_INFO: {
      if (OB_FAIL(print_archive_single_ls_info_file_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_PIECE_FILE_LIST_INFO: {
      if (OB_FAIL(print_archive_piece_list_info_file_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_FORMAT_FILE: {
      if (OB_FAIL(print_backup_format_file_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_TENANT_SET_INFOS: {
      if (OB_FAIL(print_tenant_backup_set_infos_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_LS_META_INFOS_FILE: {
      if (OB_FAIL(print_backup_ls_meta_infos_file_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_TENANT_ARCHIVE_PIECE_INFOS: {
      if (OB_FAIL(print_tenant_archive_piece_infos_file_())) {

      }
      break;
    }
    case share::ObBackupFileType::BACKUP_TABLET_METAS_INFO: {
      if (OB_FAIL(print_ls_tablet_meta_tablets_())) {

      }
      break;
    }
    default: {
      ret = OB_ERR_SYS;

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::get_backup_file_path_()
{
  int ret = OB_SUCCESS;
  char current_absolute_path[MAX_PATH_SIZE] = "";
  if (optarg[0] != '/') {
    int64_t length = 0;
    if (OB_ISNULL(getcwd(current_absolute_path, MAX_PATH_SIZE))) {
      ret = OB_ERR_UNEXPECTED;

    } else if ((length = strlen(current_absolute_path)) >= MAX_PATH_SIZE) {
      ret = OB_SIZE_OVERFLOW;

    } else {
      current_absolute_path[length] = '/';
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(databuff_printf(
            backup_path_, sizeof(backup_path_), "%s%s%s", OB_FILE_PREFIX, current_absolute_path, optarg))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::get_backup_file_type_()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t file_length = 0;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  ObArenaAllocator allocator;
  const int64_t type_len = sizeof(uint16_t);
  if (OB_FAIL(ObAdminDumpBackupDataUtil::get_backup_file_length(backup_path, storage_info, file_length))) {

  } else if (OB_UNLIKELY(file_length <= sizeof(type_len))) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(type_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, 0, type_len, buf))) {

  } else {
    uint16_t *type = reinterpret_cast<uint16_t*>(buf);
    file_type_ = *type;

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_usage_()
{
  int ret = OB_SUCCESS;
  printf("\n");
  printf("Usage: dump_backup command [command args] [options]\n");
  printf("commands:\n");
  printf(HELP_FMT, "-h,--help", "display this message.");

  printf("options:\n");
  printf(HELP_FMT, "-d,--backup-file-path", "absolute backup file path with file prefix");
  printf(HELP_FMT, "-s,--storage-info", "oss/s3 should provide storage info");
  printf(HELP_FMT, "-f,--file-path", "relative data file path");
  printf(HELP_FMT, "-o,--offset", "data offset");
  printf(HELP_FMT, "-l,--length", "data length");
  printf(HELP_FMT, "-c,--check-exist", "check file is exist or not");
  printf(HELP_FMT, "-e,--s3_url_encode_type", "set S3 protocol url encode type");
  printf(HELP_FMT, "-i, --sts_credential", "set STS credential");
  printf("samples:\n");
  printf("  dump meta: \n");
  printf("\tob_admin dump_backup -dfile:///home/admin/backup_info \n");
  printf("  dump macro: \n");
  printf("\tob_admin dump_backup -dfile:///home/admin/macro_block_1.0 -o1024 -l2048\n");
  printf("  dump data with -f: \n");
  printf("\tob_admin dump_backup -f/home/admin/macro_block_1.0 -o1024 -l2048\n");
  printf("  dump data with -s: \n");
  printf("\tob_admin dump_backup -d'oss://home/admin/backup_info' "
         "-s'host=xxx.com&access_id=111&access_key=222'\n");
  printf("\tob_admin dump_backup -d's3://home/admin/backup_info' "
         "-s'host=xxx.com&access_id=111&access_key=222&s3_region=333' "
         "-e'compliantRfc3986Encoding'\n");
  printf("\tob_admin dump_backup -d's3://home/admin/backup_info' "
         "-s'host=xxx.com&role_arn=xxx' "
         "-i'sts_url=xxx&sts_ak=aaa&sts_sk=bbb'\n");
  printf("\tob_admin dump_backup -d's3://home/admin/backup_info' "
         "-s'host=xxx.com&role_arn=xxx&external_id=xxx' "
         "-i'sts_url=xxx&sts_ak=aaa&sts_sk=bbb'\n");
  return ret;
}

int ObAdminDumpBackupDataExecutor::check_tenant_backup_path_(const char *data_path, const char *storage_info_str, bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObBackupPathString path;
  share::ObBackupDest backup_dest;
  share::ObBackupStore backup_store;
  ObBackupFormatDesc desc;
  is_exist = false;
  if (OB_FAIL(backup_dest.set(data_path, storage_info_str))) {

  } else if (OB_FAIL(backup_dest.get_backup_dest_str(path.ptr(), path.capacity()))) {

  } else if (OB_FAIL(backup_store.init(path.ptr()))) {

  } else if (OB_FAIL(backup_store.is_format_file_exist(is_exist))) {

  } else if (is_exist) {
    if (OB_FAIL(backup_store.read_format_file(desc))) {

    } else if (desc.dest_type_ != ObBackupDestType::TYPE::DEST_TYPE_BACKUP_DATA) {
      is_exist = false;
    }
  }

  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_tenant_backup_path_()
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  storage::ObBackupSetFilter op;
  share::ObBackupPath path;
  share::ObBackupStorageInfo storage_info;
  storage::ObTenantBackupSetInfosDesc tenant_backup_set_infos;
  ObSArray<share::ObBackupSetDesc> backup_set_array;
  ObArray<share::ObBackupSetFileDesc> target_backup_set;
  if (OB_FAIL(get_backup_set_placeholder_dir_path(path))) {

  } else if (OB_FAIL(storage_info.set(backup_path_, storage_info_))) {

  } else if (OB_FAIL(util.list_files(path.get_ptr(), &storage_info, op))) {

  } else if (OB_FAIL(op.get_backup_set_array(backup_set_array))) {

  } else if (!backup_set_array.empty()) {
    storage::ObBackupDataStore::ObBackupSetDescComparator cmp;
    lib::ob_sort(backup_set_array.begin(), backup_set_array.end(), cmp);
    for (int64_t i = backup_set_array.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      path.reset();
      const share::ObBackupSetDesc &backup_set_dir = backup_set_array.at(i);
      if (OB_FAIL(get_tenant_backup_set_infos_path_(backup_set_dir, path))) {

      } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(path.get_obstr(), ObString(storage_info_), tenant_backup_set_infos))) {
        if (OB_OBJECT_NOT_EXIST == ret) {

          ret = OB_SUCCESS;
        } else {

        }
      } else if (OB_FAIL(filter_backup_set_(tenant_backup_set_infos, backup_set_array, target_backup_set))) {

      } else if (OB_FAIL(inner_print_common_header_(path.get_obstr(), ObString(storage_info_)))) {

      } else if (OB_FAIL(dump_tenant_backup_set_infos_(target_backup_set))) {

      } else {
        break;
      }
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_backup_data_file_()
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  backup::ObBackupDataFileTrailer file_trailer;
  ObArray<backup::ObBackupMetaIndex> meta_index_list;
  if (OB_FAIL(print_backup_file_header_())) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_data_file_trailer(backup_path, storage_info, file_trailer))) {

  } else {
    if (OB_SUCC(ret)) {
      char *buf = NULL;
      const int64_t offset = file_trailer.macro_index_offset_;
      const int64_t length = file_trailer.macro_index_length_;
      ObArray<backup::ObBackupMacroBlockIndex> macro_index_list;
      if (0 == length) {
        // do nothing
      } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;

      } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {

      } else {
        ObBufferReader buffer_reader(buf, length);
        if (OB_FAIL(ObAdminDumpBackupDataUtil::parse_from_data_file_index_blocks<backup::ObBackupMacroBlockIndex>(
                buffer_reader, macro_index_list))) {

        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < macro_index_list.count(); ++i) {
            const backup::ObBackupMacroBlockIndex &index = macro_index_list.at(i);
            if (OB_FAIL(inner_print_macro_block_(index.offset_, index.length_, i))) {

            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      char *buf = NULL;
      const int64_t offset = file_trailer.meta_index_offset_;
      const int64_t length = file_trailer.meta_index_length_;
      ObArray<backup::ObBackupMetaIndex> meta_index_list;
      if (0 == length) {
        // do nothing
      } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;

      } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {

      } else {
        ObBufferReader buffer_reader(buf, length);
        if (OB_FAIL(ObAdminDumpBackupDataUtil::parse_from_data_file_index_blocks<backup::ObBackupMetaIndex>(
                buffer_reader, meta_index_list))) {

        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < meta_index_list.count(); ++i) {
            const backup::ObBackupMetaIndex &index = meta_index_list.at(i);
            if (backup::BACKUP_SSTABLE_META == index.meta_key_.meta_type_) {
              if (OB_FAIL(inner_print_sstable_metas_(index.offset_, index.length_))) {

              }
            } else if (backup::BACKUP_TABLET_META == index.meta_key_.meta_type_) {
              if (OB_FAIL(inner_print_tablet_meta_(index.offset_, index.length_))) {

              }
            } else if (backup::BACKUP_MACRO_BLOCK_ID_MAPPING_META == index.meta_key_.meta_type_) {
              if (OB_FAIL(inner_print_backup_macro_block_id_mapping_metas_(index.offset_, index.length_))) {

              }
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(inner_print_macro_block_index_list_(
              file_trailer.macro_index_offset_, file_trailer.macro_index_length_))) {

      } else if (OB_FAIL(
                     inner_print_meta_index_list_(file_trailer.meta_index_offset_, file_trailer.meta_index_length_))) {

      } else if (OB_FAIL(dump_data_file_trailer_(file_trailer))) {

      }
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_macro_range_index_file()
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  const int64_t offset = 0;
  int64_t length = 0;
  char *buf = NULL;
  backup::ObBackupMultiLevelIndexTrailer index_trailer;
  if (OB_FAIL(print_backup_file_header_())) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::get_backup_file_length(backup_path, storage_info, length))) {

  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_index_file_trailer(backup_path, storage_info, index_trailer))) {

  } else {
    const int64_t header_size = DIO_READ_ALIGN_SIZE;
    const int64_t trailer_size = sizeof(backup::ObBackupMultiLevelIndexTrailer);
    buf += header_size;
    ObBufferReader buffer_reader(buf, length - header_size - trailer_size);
    auto print_func1 = std::bind(&ObAdminDumpBackupDataExecutor::dump_common_header_, this, std::placeholders::_1);
    auto print_func2 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_multi_level_index_header_, this, std::placeholders::_1);
    auto print_func3 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_macro_range_index_list_, this, std::placeholders::_1);
    auto print_func4 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_macro_range_index_index_list_, this, std::placeholders::_1);
    auto ParseFunc = ObAdminDumpBackupDataUtil::parse_from_index_index_blocks<backup::ObBackupMacroRangeIndex,
        backup::ObBackupMacroRangeIndexIndex>;
    if (OB_FAIL(ParseFunc(buffer_reader, print_func1, print_func2, print_func3, print_func4))) {

    } else if (OB_FAIL(dump_index_file_trailer_(index_trailer))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_meta_index_file()
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  const int64_t offset = 0;
  int64_t length = 0;
  char *buf = NULL;
  backup::ObBackupMultiLevelIndexTrailer index_trailer;
  if (OB_FAIL(print_backup_file_header_())) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::get_backup_file_length(backup_path, storage_info, length))) {

  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_index_file_trailer(backup_path, storage_info, index_trailer))) {

  } else {
    const int64_t header_size = DIO_READ_ALIGN_SIZE;
    const int64_t trailer_size = sizeof(backup::ObBackupMultiLevelIndexTrailer);
    buf += header_size;
    ObBufferReader buffer_reader(buf, length - header_size - trailer_size);
    auto print_func1 = std::bind(&ObAdminDumpBackupDataExecutor::dump_common_header_, this, std::placeholders::_1);
    auto print_func2 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_multi_level_index_header_, this, std::placeholders::_1);
    auto print_func3 = std::bind(&ObAdminDumpBackupDataExecutor::dump_meta_index_list_, this, std::placeholders::_1);
    auto print_func4 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_meta_index_index_list_, this, std::placeholders::_1);
    auto ParseFunc = ObAdminDumpBackupDataUtil::parse_from_index_index_blocks<backup::ObBackupMetaIndex,
        backup::ObBackupMetaIndexIndex>;
    if (OB_FAIL(ParseFunc(buffer_reader, print_func1, print_func2, print_func3, print_func4))) {

    } else if (OB_FAIL(dump_index_file_trailer_(index_trailer))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_macro_block_index_file()
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  const int64_t offset = 0;
  int64_t length = 0;
  char *buf = NULL;
  backup::ObBackupMultiLevelIndexTrailer index_trailer;
  if (OB_FAIL(print_backup_file_header_())) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::get_backup_file_length(backup_path, storage_info, length))) {

  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_index_file_trailer(backup_path, storage_info, index_trailer))) {

  } else {
    const int64_t header_size = DIO_READ_ALIGN_SIZE;
    const int64_t trailer_size = sizeof(backup::ObBackupMultiLevelIndexTrailer);
    buf += header_size;
    ObBufferReader buffer_reader(buf, length - header_size - trailer_size);

    std::function<int(const share::ObBackupCommonHeader &)> print_func1 = 
        std::bind(&ObAdminDumpBackupDataExecutor::dump_common_header_, this, std::placeholders::_1);
    std::function<int(const backup::ObBackupMultiLevelIndexHeader &)> print_func2 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_multi_level_index_header_, this, std::placeholders::_1);
    std::function<int(const common::ObIArray<backup::ObBackupMacroBlockIndex> &)> print_func3 = 
        std::bind(&ObAdminDumpBackupDataExecutor::dump_macro_block_index_list_, this, std::placeholders::_1);
    std::function<int(const common::ObIArray<backup::ObBackupMacroBlockIndexIndex> &)> print_func4 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_macro_block_index_index_list_, this, std::placeholders::_1);
    
    typedef int(*ParseFuncType)(blocksstable::ObBufferReader &,
                                const std::function<int(const share::ObBackupCommonHeader &)> &,
                                const std::function<int(const backup::ObBackupMultiLevelIndexHeader &)> &,
                                const std::function<int(const common::ObIArray<backup::ObBackupMacroBlockIndex> &)> &,
                                const std::function<int(const common::ObIArray<backup::ObBackupMacroBlockIndexIndex> &)> &);

    ParseFuncType ParseFunc = ObAdminDumpBackupDataUtil::parse_from_index_index_blocks<backup::ObBackupMacroBlockIndex, backup::ObBackupMacroBlockIndexIndex>;

    if (OB_FAIL(ParseFunc(buffer_reader, print_func1, print_func2, print_func3, print_func4))) {

    } else if (OB_FAIL(dump_index_file_trailer_(index_trailer))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_backup_file_header_()
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  backup::ObBackupFileHeader file_header;
  if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_file_header(backup_path, storage_info, file_header))) {

  } else if (OB_FAIL(dump_backup_file_header_(file_header))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_macro_block_()
{
  int ret = OB_SUCCESS;
  const int64_t offset = offset_;
  const int64_t length = length_;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_FAIL(inner_print_macro_block_(offset, length))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_tablet_meta_()
{
  int ret = OB_SUCCESS;
  const int64_t offset = offset_;
  const int64_t length = length_;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_FAIL(inner_print_tablet_meta_(offset, length))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_sstable_metas_()
{
  int ret = OB_SUCCESS;
  const int64_t offset = offset_;
  const int64_t length = length_;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_FAIL(inner_print_sstable_metas_(offset, length))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_macro_block_index_list_()
{
  int ret = OB_SUCCESS;
  const int64_t offset = offset_;
  const int64_t length = length_;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_FAIL(inner_print_macro_block_index_list_(offset, length))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_meta_index_list_()
{
  int ret = OB_SUCCESS;
  const int64_t offset = offset_;
  const int64_t length = length_;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_FAIL(inner_print_meta_index_list_(offset, length))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_data_file_trailer_()
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  backup::ObBackupDataFileTrailer file_trailer;
  if (OB_FAIL(ObAdminDumpBackupDataUtil::read_data_file_trailer(backup_path, storage_info, file_trailer))) {

  } else if (OB_FAIL(dump_data_file_trailer_(file_trailer))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_index_file_trailer_()
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  backup::ObBackupMultiLevelIndexTrailer file_trailer;
  if (OB_FAIL(ObAdminDumpBackupDataUtil::read_index_file_trailer(backup_path, storage_info, file_trailer))) {

  } else if (OB_FAIL(dump_index_file_trailer_(file_trailer))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_macro_range_index_index_list_()
{
  int ret = OB_SUCCESS;
  const int64_t offset = offset_;
  const int64_t length = length_;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_FAIL(inner_print_macro_range_index_index_list_(offset, length))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_meta_index_index_list_()
{
  int ret = OB_SUCCESS;
  const int64_t offset = offset_;
  const int64_t length = length_;
  if (offset < 0 || length <= 0) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_FAIL(inner_print_meta_index_index_list_(offset, length))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_ls_attr_info_()
{
  int ret = OB_SUCCESS;
  storage::ObBackupDataLSAttrDesc ls_attr_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(ObString(backup_path_), ObString(storage_info_), ls_attr_desc))) {

  } else {
    ARRAY_FOREACH_X(ls_attr_desc.ls_attr_array_, i , cnt, OB_SUCC(ret)) {
      const share::ObLSAttr ls_attr = ls_attr_desc.ls_attr_array_.at(i);
      if (OB_FAIL(dump_ls_attr_info_(ls_attr))) {

      }
    }
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_tablet_to_ls_info_()
{
  int ret = OB_SUCCESS;
  storage::ObBackupDataTabletToLSDesc tablet_to_ls_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(ObString(backup_path_), ObString(storage_info_), tablet_to_ls_desc))) {

  } else {
    ARRAY_FOREACH_X(tablet_to_ls_desc.tablet_to_ls_, i , cnt, OB_SUCC(ret)) {
      const storage::ObBackupDataTabletToLSInfo tablet_to_ls = tablet_to_ls_desc.tablet_to_ls_.at(i);
      if (OB_FAIL(dump_tablet_to_ls_info_(tablet_to_ls))) {

      }
    }
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_deleted_tablet_info_()
{
  int ret = OB_SUCCESS;
  storage::ObBackupDeletedTabletToLSDesc deleted_tablet_to_ls;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(ObString(backup_path_), ObString(storage_info_), deleted_tablet_to_ls))) {

  } else {
    ARRAY_FOREACH_X(deleted_tablet_to_ls.deleted_tablet_to_ls_, i , cnt, OB_SUCC(ret)) {
      const storage::ObBackupDataTabletToLSInfo tablet_to_ls = deleted_tablet_to_ls.deleted_tablet_to_ls_.at(i);
      if (OB_FAIL(dump_tablet_to_ls_info_(tablet_to_ls))) {

      }
    }
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_tenant_locality_info_()
{
  int ret = OB_SUCCESS;
  storage::ObExternTenantLocalityInfoDesc locality_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(ObString(backup_path_), ObString(storage_info_), locality_desc))) {

  } else if (OB_FAIL(dump_tenant_locality_info_(locality_desc))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_parameters_info_()
{
  int ret = OB_SUCCESS;
  storage::ObExternParamInfoDesc param_info_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(ObString(backup_path_),
    ObString(storage_info_), param_info_desc))) {

  } else if (OB_FAIL(dump_parameters_info_(param_info_desc))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_tenant_diagnose_info_()
{
  int ret = OB_SUCCESS;
  storage::ObExternTenantDiagnoseInfoDesc diagnose_info;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(ObString(backup_path_), ObString(storage_info_), diagnose_info))) {

  } else if (OB_FAIL(dump_tenant_diagnose_info_(diagnose_info))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_backup_set_info_()
{
  int ret = OB_SUCCESS;
  storage::ObExternBackupSetInfoDesc backup_set_info_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(ObString(backup_path_), ObString(storage_info_), backup_set_info_desc))) {

  } else if (OB_FAIL(dump_backup_set_info(backup_set_info_desc.backup_set_file_))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_place_holder_info_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_archive_round_start_file_()
{
  int ret = OB_SUCCESS;
  share::ObRoundStartDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {

  } else if (OB_FAIL(dump_archive_round_start_file_(file_desc))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_archive_round_end_file_()
{
  int ret = OB_SUCCESS;
  share::ObRoundEndDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {

  } else if (OB_FAIL(dump_archive_round_end_file_(file_desc))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_archive_piece_start_file_()
{
  int ret = OB_SUCCESS;
  share::ObPieceStartDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {

  } else if (OB_FAIL(dump_archive_piece_start_file_(file_desc))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_archive_piece_end_file_()
{
  int ret = OB_SUCCESS;
  share::ObPieceEndDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {

  } else if (OB_FAIL(dump_archive_piece_end_file_(file_desc))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_archive_single_piece_file_()
{
  int ret = OB_SUCCESS;
  share::ObSinglePieceDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {

  } else if (OB_FAIL(dump_archive_single_piece_file_(file_desc))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_archive_piece_inner_placeholder_file_()
{
  int ret = OB_SUCCESS;
  share::ObPieceInnerPlaceholderDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {

  } else if (OB_FAIL(dump_archive_piece_inner_placeholder_file_(file_desc))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_archive_single_ls_info_file_()
{
  int ret = OB_SUCCESS;
  share::ObSingleLSInfoDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {

  } else if (OB_FAIL(dump_archive_single_ls_info_file_(file_desc))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_archive_piece_list_info_file_()
{
  int ret = OB_SUCCESS;
  share::ObPieceInfoDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {

  } else if (OB_FAIL(dump_archive_piece_list_info_file_(file_desc))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_tenant_archive_piece_infos_file_()
{
  int ret = OB_SUCCESS;
  share::ObTenantArchivePieceInfosDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {

  } else if (OB_FAIL(dump_tenant_archive_piece_infos_file_(file_desc))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_ls_tablet_meta_tablets_()
{
  int ret = OB_SUCCESS;
  backup::ObTabletInfoTrailer tablet_meta_trailer;
  if (OB_FAIL(ObAdminDumpBackupDataUtil::read_tablet_metas_file_trailer(backup_path_, storage_info_, tablet_meta_trailer))) {

  } else {
    common::ObArenaAllocator allocator;
    const int64_t DEFAULT_BUF_LEN = 2 * MAX_BACKUP_TABLET_META_SERIALIZE_SIZE;
    int64_t cur_buf_offset = tablet_meta_trailer.offset_;
    int64_t cur_total_len = 0;
    char *buf = nullptr;
    while (OB_SUCC(ret) && cur_buf_offset < tablet_meta_trailer.length_) {
      const uint64_t buf_len = tablet_meta_trailer.length_ - cur_buf_offset < DEFAULT_BUF_LEN ? 
                               tablet_meta_trailer.length_ - cur_buf_offset : DEFAULT_BUF_LEN;
      if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;

      } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path_, storage_info_, cur_buf_offset, buf_len, buf))) {

      } else {
        int64_t tablet_cnt = 0;
        backup::ObBackupTabletMeta tablet_meta;
        blocksstable::ObBufferReader buffer_reader(buf, buf_len);
        while (OB_SUCC(ret)) {
          tablet_meta.reset();
          int64_t pos = 0;
          const ObBackupCommonHeader *common_header = nullptr;
          if (buffer_reader.remain() == 0) {
            cur_total_len = buffer_reader.capacity();

            break;
          } else if (OB_FAIL(buffer_reader.get(common_header))) {

          } else if (OB_ISNULL(common_header)) {
            ret = OB_ERR_UNEXPECTED;

          } else if (OB_FAIL(common_header->check_valid())) {

          } else if (common_header->data_zlength_ > buffer_reader.remain()) {
            cur_total_len = buffer_reader.pos() - sizeof(ObBackupCommonHeader);
            if (0 == tablet_cnt) {
              ret = OB_ERR_UNEXPECTED;

            } else {

            }
            break;
          } else if (OB_FAIL(common_header->check_data_checksum(buffer_reader.current(), common_header->data_zlength_))) {

          } else if (OB_FAIL(tablet_meta.tablet_meta_.deserialize(buffer_reader.current(), common_header->data_zlength_, pos))) {

          } else if (OB_FAIL(buffer_reader.advance(common_header->data_length_ + common_header->align_length_))) {

          } else {
            ++tablet_cnt;
            tablet_meta.tablet_id_ = tablet_meta.tablet_meta_.tablet_id_;
            if (OB_FAIL(dump_common_header_(*common_header))) {

            } else if (OB_FAIL(dump_backup_tablet_meta_(tablet_meta))) {

            }
          }
        }
        if (OB_SUCC(ret)) {
          cur_buf_offset += cur_total_len;
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(dump_tablet_trailer_(tablet_meta_trailer))) {

    } 
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_backup_format_file_()
{
  int ret = OB_SUCCESS;
  share::ObBackupFormatDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_archive_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {

  } else if (OB_FAIL(dump_backup_format_file_(file_desc))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_tenant_backup_set_infos_()
{
 int ret = OB_SUCCESS;
  ObBackupDest backup_tenant_dest;
  storage::ObTenantBackupSetInfosDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {

  } else if(!file_desc.backup_set_infos_.empty()) {
    if (OB_FAIL(dump_tenant_backup_set_infos_file_(file_desc.backup_set_infos_))) {

      }
  } 
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_backup_ls_meta_infos_file_()
{
  int ret = OB_SUCCESS;
  storage::ObBackupLSMetaInfosDesc file_desc;
  if (OB_FAIL(inner_print_common_header_(backup_path_, storage_info_))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(ObString(backup_path_), ObString(storage_info_), file_desc))) {

  } else if (OB_FAIL(dump_backup_ls_meta_infos_file_(file_desc))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_macro_block_(
    const int64_t offset, const int64_t length, const int64_t idx)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {

  } else {
    ObBufferReader buffer_reader(buf, length);
    const share::ObBackupCommonHeader *common_header = NULL;
    if (OB_FAIL(ObAdminDumpBackupDataUtil::get_common_header(buffer_reader, common_header))) {

    } else if (OB_FAIL(dump_common_header_(*common_header))) {

    } else {
      const int64_t data_length = common_header->data_zlength_;
      PrintHelper::print_dump_title("Macro Block");
      PrintHelper::print_dump_line("Idx", idx);
      PrintHelper::print_dump_line("MacroBlockLength", data_length);
      PrintHelper::print_end_line();
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_tablet_meta_(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {

  } else {
    int64_t pos = 0;
    ObBufferReader buffer_reader(buf, length);
    backup::ObBackupTabletMeta tablet_meta;
    const share::ObBackupCommonHeader *common_header = NULL;
    if (OB_FAIL(ObAdminDumpBackupDataUtil::get_common_header(buffer_reader, common_header))) {

    } else if (OB_FAIL(dump_common_header_(*common_header))) {

    } else if (OB_FAIL(tablet_meta.deserialize(buffer_reader.current(), common_header->data_zlength_, pos))) {

    } else if (OB_FAIL(dump_backup_tablet_meta_(tablet_meta))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_sstable_metas_(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {

  } else {
    int64_t pos = 0;
    ObBufferReader buffer_reader(buf, length);
    const share::ObBackupCommonHeader *common_header = NULL;
    if (OB_FAIL(ObAdminDumpBackupDataUtil::get_common_header(buffer_reader, common_header))) {

    } else if (OB_FAIL(dump_common_header_(*common_header))) {

    } else {
      int64_t total_pos = 0;
      char *tmp_buf = buffer_reader.current();
      const int64_t total_data_size = common_header->data_zlength_;
      ObBackupSSTableMeta sstable_meta;
      while (OB_SUCC(ret) && total_data_size - total_pos > 0) {
        int64_t pos = 0;
        sstable_meta.reset();
        if (OB_FAIL(sstable_meta.deserialize(tmp_buf + total_pos, total_data_size - total_pos, pos))) {

        } else if (OB_FAIL(dump_backup_sstable_meta_(sstable_meta))) {

        } else {
          total_pos += pos;
        }
      }
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_backup_macro_block_id_mapping_metas_(
    const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {

  } else {
    int64_t pos = 0;
    ObBufferReader buffer_reader(buf, length);
    backup::ObBackupMacroBlockIDMappingsMeta mapping_meta;
    const share::ObBackupCommonHeader *common_header = NULL;
    if (OB_FAIL(ObAdminDumpBackupDataUtil::get_common_header(buffer_reader, common_header))) {

    } else if (OB_FAIL(dump_common_header_(*common_header))) {

    } else if (OB_FAIL(mapping_meta.deserialize(buffer_reader.current(), common_header->data_zlength_, pos))) {

    } else if (OB_FAIL(dump_backup_macro_block_id_mapping_meta_(mapping_meta))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_macro_block_index_list_(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (0 == length) {
    // do nothing
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {

  } else {
    ObBufferReader buffer_reader(buf, length);
    auto print_func1 = std::bind(&ObAdminDumpBackupDataExecutor::dump_common_header_, this, std::placeholders::_1);
    auto print_func2 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_macro_block_index_list_, this, std::placeholders::_1);
    if (OB_FAIL(ObAdminDumpBackupDataUtil::parse_from_index_blocks<backup::ObBackupMacroBlockIndex>(
            buffer_reader, print_func1, print_func2))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_meta_index_list_(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (0 == length) {
    // do nothing
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {

  } else {
    ObBufferReader buffer_reader(buf, length);
    auto print_func1 = std::bind(&ObAdminDumpBackupDataExecutor::dump_common_header_, this, std::placeholders::_1);
    auto print_func2 = std::bind(&ObAdminDumpBackupDataExecutor::dump_meta_index_list_, this, std::placeholders::_1);
    if (OB_FAIL(ObAdminDumpBackupDataUtil::parse_from_index_blocks<backup::ObBackupMetaIndex>(
            buffer_reader, print_func1, print_func2))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_macro_range_index_list_(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {

  } else {
    ObBufferReader buffer_reader(buf, length);
    auto print_func1 = std::bind(&ObAdminDumpBackupDataExecutor::dump_common_header_, this, std::placeholders::_1);
    auto print_func2 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_multi_level_index_header_, this, std::placeholders::_1);
    auto print_func3 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_macro_range_index_list_, this, std::placeholders::_1);
    if (OB_FAIL(ObAdminDumpBackupDataUtil::parse_from_index_index_blocks<backup::ObBackupMacroRangeIndex>(
            buffer_reader, print_func1, print_func2, print_func3))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_macro_range_index_index_list_(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {

  } else {
    ObBufferReader buffer_reader(buf, length);
    auto print_func1 = std::bind(&ObAdminDumpBackupDataExecutor::dump_common_header_, this, std::placeholders::_1);
    auto print_func2 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_multi_level_index_header_, this, std::placeholders::_1);
    auto print_func3 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_macro_range_index_index_list_, this, std::placeholders::_1);
    if (OB_FAIL(ObAdminDumpBackupDataUtil::parse_from_index_index_blocks<backup::ObBackupMacroRangeIndexIndex>(
            buffer_reader, print_func1, print_func2, print_func3))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_meta_index_index_list_(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  const common::ObString backup_path(backup_path_);
  const common::ObString storage_info(storage_info_);
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(length)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, offset, length, buf))) {

  } else {
    ObBufferReader buffer_reader(buf, length);
    auto print_func1 = std::bind(&ObAdminDumpBackupDataExecutor::dump_common_header_, this, std::placeholders::_1);
    auto print_func2 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_multi_level_index_header_, this, std::placeholders::_1);
    auto print_func3 =
        std::bind(&ObAdminDumpBackupDataExecutor::dump_meta_index_index_list_, this, std::placeholders::_1);
    if (OB_FAIL(ObAdminDumpBackupDataUtil::parse_from_index_index_blocks<backup::ObBackupMetaIndexIndex>(
            buffer_reader, print_func1, print_func2, print_func3))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_common_header_(const char *data_path, const char *storage_info_str)
{
  int ret = OB_SUCCESS;
  common::ObString tmp_data_path(data_path);
  common::ObString tmp_storage_info(storage_info_str);
  if (OB_FAIL(inner_print_common_header_(tmp_data_path, tmp_storage_info))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::inner_print_common_header_(const common::ObString &backup_path,
    const common::ObString &storage_info)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t file_length = 0;
  share::ObBackupCommonHeader *header = nullptr;
  ObArenaAllocator allocator;
  const int64_t header_len = sizeof(share::ObBackupCommonHeader);

  if (OB_FAIL(ObAdminDumpBackupDataUtil::get_backup_file_length(backup_path, storage_info, file_length))) {

  } else if (OB_UNLIKELY(file_length <= sizeof(header_len))) {
    ret = OB_ERR_UNEXPECTED;

  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(header_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::pread_file(backup_path, storage_info, 0, header_len, buf))) {

  } else if (OB_FALSE_IT(header = reinterpret_cast<share::ObBackupCommonHeader *>(buf))) {
  } else if (OB_FAIL(header->check_valid())) {

  } else if (OB_FAIL(dump_common_header_(*header))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_backup_file_header_(const backup::ObBackupFileHeader &file_header)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup File Header");
  PrintHelper::print_dump_line("magic", file_header.magic_);
  PrintHelper::print_dump_line("version", file_header.version_);
  PrintHelper::print_dump_line("file_type", file_header.file_type_);
  PrintHelper::print_dump_line("compressor_type", file_header.reserved_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_common_header_(const share::ObBackupCommonHeader &common_header)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup Common Header");
  PrintHelper::print_dump_line("data_type", common_header.data_type_);
  PrintHelper::print_dump_line("header_version", common_header.header_version_);
  PrintHelper::print_dump_line("data_version", common_header.data_version_);
  PrintHelper::print_dump_line("compressor_type", common_header.compressor_type_);
  PrintHelper::print_dump_line("header_length", common_header.header_length_);
  PrintHelper::print_dump_line("header_checksum", common_header.header_checksum_);
  PrintHelper::print_dump_line("data_length", common_header.data_length_);
  PrintHelper::print_dump_line("data_zlength", common_header.data_zlength_);
  PrintHelper::print_dump_line("data_checksum", common_header.data_checksum_);
  PrintHelper::print_dump_line("align_length", common_header.align_length_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_data_file_trailer_(const backup::ObBackupDataFileTrailer &trailer)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Data File Trailer");
  PrintHelper::print_dump_line("data_type", trailer.data_type_);
  PrintHelper::print_dump_line("data_version", trailer.data_version_);
  PrintHelper::print_dump_line("macro_block_count", trailer.macro_block_count_);
  PrintHelper::print_dump_line("meta_count", trailer.meta_count_);
  PrintHelper::print_dump_line("macro_index_offset", trailer.macro_index_offset_);
  PrintHelper::print_dump_line("macro_index_length", trailer.macro_index_length_);
  PrintHelper::print_dump_line("meta_index_offset", trailer.meta_index_offset_);
  PrintHelper::print_dump_line("meta_index_length", trailer.meta_index_length_);
  PrintHelper::print_dump_line("offset", trailer.offset_);
  PrintHelper::print_dump_line("length", trailer.length_);
  PrintHelper::print_dump_line("data_accumulate_checksum", trailer.data_accumulate_checksum_);
  PrintHelper::print_dump_line("trailer_checksum", trailer.trailer_checksum_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_tablet_trailer_(const backup::ObTabletInfoTrailer &tablet_meta_trailer)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Tablet Meta Trailer");
  PrintHelper::print_dump_line("file_id", tablet_meta_trailer.file_id_);
  PrintHelper::print_dump_line("tablet_count", tablet_meta_trailer.tablet_cnt_);
  PrintHelper::print_dump_line("offset", tablet_meta_trailer.offset_);
  PrintHelper::print_dump_line("length", tablet_meta_trailer.length_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_index_file_trailer_(const backup::ObBackupMultiLevelIndexTrailer &trailer)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Index File Trailer");
  PrintHelper::print_dump_line("file_type", trailer.file_type_);
  PrintHelper::print_dump_line("tree_height", trailer.tree_height_);
  PrintHelper::print_dump_line("last_block_offset", trailer.last_block_offset_);
  PrintHelper::print_dump_line("last_block_length", trailer.last_block_length_);
  PrintHelper::print_dump_line("checksum", trailer.checksum_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_multi_level_index_header_(const backup::ObBackupMultiLevelIndexHeader &header)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup Multi Level Index Header");
  PrintHelper::print_dump_line("magic", header.magic_);
  PrintHelper::print_dump_line("backup_type", header.backup_type_);
  PrintHelper::print_dump_line("index_level", header.index_level_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_macro_block_index_(const backup::ObBackupMacroBlockIndex &index)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup Macro Block Index");
  ObCStringHelper helper;
  PrintHelper::print_dump_line("logic_id", helper.convert(index.logic_id_));
  PrintHelper::print_dump_line("backup_set_id", index.backup_set_id_);
  PrintHelper::print_dump_line("ls_id", index.ls_id_.id());
  PrintHelper::print_dump_line("turn_id", index.turn_id_);
  PrintHelper::print_dump_line("retry_id", index.retry_id_);
  PrintHelper::print_dump_line("file_id", index.file_id_);
  PrintHelper::print_dump_line("offset", index.offset_);
  PrintHelper::print_dump_line("length", index.length_);
  PrintHelper::print_dump_line("reusable", index.reusable_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_macro_block_index_list_(
    const common::ObIArray<ObBackupMacroBlockIndex> &index_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const backup::ObBackupMacroBlockIndex &index = index_list.at(i);
    if (OB_FAIL(dump_macro_block_index_(index))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_macro_block_index_index_(const backup::ObBackupMacroBlockIndexIndex &index)
{
  int ret = OB_SUCCESS;
  ObCStringHelper helper;
  PrintHelper::print_dump_title("Backup Macro Block Index Index");
  PrintHelper::print_dump_line("logic_id", helper.convert(index.end_key_.logic_id_));
  PrintHelper::print_dump_line("backup_set_id", index.end_key_.backup_set_id_);
  PrintHelper::print_dump_line("ls_id", index.end_key_.ls_id_.id());
  PrintHelper::print_dump_line("turn_id", index.end_key_.turn_id_);
  PrintHelper::print_dump_line("retry_id", index.end_key_.retry_id_);
  PrintHelper::print_dump_line("file_id", index.end_key_.file_id_);
  PrintHelper::print_dump_line("offset", index.offset_);
  PrintHelper::print_dump_line("length", index.length_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_macro_block_index_index_list_(
    const common::ObIArray<ObBackupMacroBlockIndexIndex> &index_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const backup::ObBackupMacroBlockIndexIndex &index = index_list.at(i);
    if (OB_FAIL(dump_macro_block_index_index_(index))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_macro_range_index_(const backup::ObBackupMacroRangeIndex &index)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup Macro Range Index");
  PrintHelper::print_dump_line("start_key:tablet_id", index.start_key_.tablet_id_);
  PrintHelper::print_dump_line("start_key:logic_version", index.start_key_.logic_version_);
  PrintHelper::print_dump_line("start_key:data_seq", index.start_key_.data_seq_.macro_data_seq_);
  PrintHelper::print_dump_line("end_key:tablet_id", index.end_key_.tablet_id_);
  PrintHelper::print_dump_line("end_key:logic_version", index.end_key_.logic_version_);
  PrintHelper::print_dump_line("end_key:data_seq", index.end_key_.data_seq_.macro_data_seq_);
  PrintHelper::print_dump_line("backup_set_id", index.backup_set_id_);
  PrintHelper::print_dump_line("ls_id", index.ls_id_.id());
  PrintHelper::print_dump_line("turn_id", index.turn_id_);
  PrintHelper::print_dump_line("retry_id", index.retry_id_);
  PrintHelper::print_dump_line("file_id", index.file_id_);
  PrintHelper::print_dump_line("offset", index.offset_);
  PrintHelper::print_dump_line("length", index.length_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_macro_range_index_list_(
    const common::ObIArray<backup::ObBackupMacroRangeIndex> &index_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const backup::ObBackupMacroRangeIndex &index = index_list.at(i);
    if (OB_FAIL(dump_macro_range_index_(index))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_macro_range_index_index_(const backup::ObBackupMacroRangeIndexIndex &index)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup Macro Range Index Index");
  ObCStringHelper helper;
  PrintHelper::print_dump_line("end_key:start_key", helper.convert(index.end_key_.start_key_));
  PrintHelper::print_dump_line("end_key:end_key", helper.convert(index.end_key_.end_key_));
  PrintHelper::print_dump_line("end_key:backup_set_id", index.end_key_.backup_set_id_);
  PrintHelper::print_dump_line("end_key:ls_id", index.end_key_.ls_id_.id());
  PrintHelper::print_dump_line("end_key:turn_id", index.end_key_.turn_id_);
  PrintHelper::print_dump_line("end_key:retry_id", index.end_key_.retry_id_);
  PrintHelper::print_dump_line("end_key:file_id", index.end_key_.file_id_);
  PrintHelper::print_dump_line("end_key:offset", index.end_key_.offset_);
  PrintHelper::print_dump_line("end_key:length", index.end_key_.length_);
  PrintHelper::print_dump_line("offset", index.offset_);
  PrintHelper::print_dump_line("length", index.length_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_macro_range_index_index_list_(
    const common::ObIArray<backup::ObBackupMacroRangeIndexIndex> &index_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const backup::ObBackupMacroRangeIndexIndex &index = index_list.at(i);
    if (OB_FAIL(dump_macro_range_index_index_(index))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_meta_index_(const backup::ObBackupMetaIndex &index)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup Meta Index");
  ObCStringHelper helper;
  PrintHelper::print_dump_line("meta_key", helper.convert(index.meta_key_));
  PrintHelper::print_dump_line("backup_set_id", index.backup_set_id_);
  PrintHelper::print_dump_line("ls_id", index.ls_id_.id());
  PrintHelper::print_dump_line("turn_id", index.turn_id_);
  PrintHelper::print_dump_line("retry_id", index.retry_id_);
  PrintHelper::print_dump_line("file_id", index.file_id_);
  PrintHelper::print_dump_line("offset", index.offset_);
  PrintHelper::print_dump_line("length", index.length_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_meta_index_list_(const common::ObIArray<backup::ObBackupMetaIndex> &index_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const backup::ObBackupMetaIndex &index = index_list.at(i);
    if (OB_FAIL(dump_meta_index_(index))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_meta_index_index_(const backup::ObBackupMetaIndexIndex &index)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup Meta Index Index");
  PrintHelper::print_dump_line("tablet_id", index.end_key_.meta_key_.tablet_id_.id());
  PrintHelper::print_dump_line("meta_type", index.end_key_.meta_key_.meta_type_);
  PrintHelper::print_dump_line("backup_set_id", index.end_key_.backup_set_id_);
  PrintHelper::print_dump_line("ls_id", index.end_key_.ls_id_.id());
  PrintHelper::print_dump_line("turn_id", index.end_key_.turn_id_);
  PrintHelper::print_dump_line("retry_id", index.end_key_.retry_id_);
  PrintHelper::print_dump_line("file_id", index.end_key_.file_id_);
  PrintHelper::print_dump_line("offset", index.end_key_.offset_);
  PrintHelper::print_dump_line("length", index.end_key_.length_);
  PrintHelper::print_dump_line("offset", index.offset_);
  PrintHelper::print_dump_line("length", index.length_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_meta_index_index_list_(
    const common::ObIArray<backup::ObBackupMetaIndexIndex> &index_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_list.count(); ++i) {
    const backup::ObBackupMetaIndexIndex &index = index_list.at(i);
    if (OB_FAIL(dump_meta_index_index_(index))) {

    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_backup_tablet_meta_(const backup::ObBackupTabletMeta &tablet_meta)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup Tablet Meta");
  PrintHelper::print_dump_line("tablet_id", tablet_meta.tablet_id_.id());
  PrintHelper::print_dump_line("tablet_meta:ls_id", tablet_meta.tablet_meta_.ls_id_.id());
  PrintHelper::print_dump_line("tablet_meta:tablet_id", tablet_meta.tablet_meta_.tablet_id_.id());
  PrintHelper::print_dump_line("tablet_meta:data_tablet_id", tablet_meta.tablet_meta_.data_tablet_id_.id());
  PrintHelper::print_dump_line("tablet_meta:ref_tablet_id", tablet_meta.tablet_meta_.ref_tablet_id_.id());
  PrintHelper::print_dump_line("tablet_meta:clog_checkpoint_scn", tablet_meta.tablet_meta_.clog_checkpoint_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("tablet_meta:ddl_checkpoint_scn", tablet_meta.tablet_meta_.ddl_checkpoint_scn_.get_val_for_logservice());
  // TODO yq: print migration param tablet status
  // PrintHelper::print_dump_line("tablet_meta:tablet_status", tablet_meta.tablet_meta_.tx_data_.tablet_status_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_backup_sstable_meta_(const backup::ObBackupSSTableMeta &sstable_meta)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup SSTable Meta");
  PrintHelper::print_dump_line("tablet_id", sstable_meta.tablet_id_.id());
  ObCStringHelper helper;
  PrintHelper::print_dump_line("table_key", helper.convert(sstable_meta.sstable_meta_.table_key_));
  PrintHelper::print_dump_line("sstable_meta:row_count", sstable_meta.sstable_meta_.basic_meta_.row_count_);
  PrintHelper::print_dump_line("sstable_meta:occupy_size", sstable_meta.sstable_meta_.basic_meta_.occupy_size_);
  PrintHelper::print_dump_line("sstable_meta:original_size", sstable_meta.sstable_meta_.basic_meta_.original_size_);
  PrintHelper::print_dump_line("sstable_meta:data_checksum", sstable_meta.sstable_meta_.basic_meta_.data_checksum_);
  PrintHelper::print_dump_line("sstable_meta:index_type", sstable_meta.sstable_meta_.basic_meta_.index_type_);
  PrintHelper::print_dump_line("sstable_meta:rowkey_column_count", sstable_meta.sstable_meta_.basic_meta_.rowkey_column_count_);
  PrintHelper::print_dump_line("sstable_meta:column_cnt", sstable_meta.sstable_meta_.basic_meta_.column_cnt_);
  PrintHelper::print_dump_line("sstable_meta:data_macro_block_count", sstable_meta.sstable_meta_.basic_meta_.data_macro_block_count_);
  PrintHelper::print_dump_list_start("logic_id_array");
  for (int64_t i = 0; OB_SUCC(ret) && i < sstable_meta.logic_id_list_.count(); ++i) {
    const blocksstable::ObLogicMacroBlockId &macro_id = sstable_meta.logic_id_list_.at(i);
    ObCStringHelper helper;
    const char *macro_id_str = helper.convert(macro_id);
    if (nullptr == macro_id_str) {
      macro_id_str = "NULL";
    }
    PrintHelper::print_dump_list_value(macro_id_str, i == sstable_meta.logic_id_list_.count() - 1);
  }
  PrintHelper::print_dump_list_end();
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_backup_macro_block_id_mapping_meta_(
    const backup::ObBackupMacroBlockIDMappingsMeta &mapping_meta)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("Backup SSTable Macro Block ID Mapping Meta");
  PrintHelper::print_dump_line("version", mapping_meta.version_);
  PrintHelper::print_dump_line("sstable_count", mapping_meta.sstable_count_);
  ObCStringHelper helper;
  if (OB_UNLIKELY(mapping_meta.sstable_count_ != mapping_meta.id_map_list_.count())) {
    ret = OB_ERR_UNEXPECTED;

  }

  for (int64_t i = 0; OB_SUCC(ret) && i < mapping_meta.sstable_count_; ++i) {
    if (OB_ISNULL(mapping_meta.id_map_list_[i])) {
      ret = OB_ERR_UNEXPECTED;

    } else {
      const ObBackupMacroBlockIDMapping &item = *mapping_meta.id_map_list_[i];
      const ObITable::TableKey &table_key = item.table_key_;
      int64_t num_of_entries = item.id_pair_list_.count();
      PrintHelper::print_dump_line("table_key", helper.convert(table_key));
      PrintHelper::print_dump_line("num_of_entries", num_of_entries);
      for (int64_t j = 0; OB_SUCC(ret) && j < num_of_entries; ++j) {
        const ObCompatBackupMacroBlockIDPair &pair = item.id_pair_list_.at(j);
        const blocksstable::ObLogicMacroBlockId &logic_id = pair.logic_id_;
        const ObBackupPhysicalID &physical_id = pair.physical_id_;
        PrintHelper::print_dump_line("logic_id", helper.convert(logic_id));
        PrintHelper::print_dump_line("physical_id", helper.convert(physical_id));
      }
    }
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_ls_attr_info_(const share::ObLSAttr &ls_attr)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("ls_attr info");
  PrintHelper::print_dump_line("ls_id", ls_attr.get_ls_id().id());
  PrintHelper::print_dump_line("ls_group_id", ls_attr.get_ls_group_id());
  PrintHelper::print_dump_line("flag", ls_attr.get_ls_flag().get_flag_value());
  PrintHelper::print_dump_line("status", ls_attr.get_ls_status());
  PrintHelper::print_dump_line("operation_type", ls_attr.get_ls_operation_type());
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_tablet_to_ls_info_(const storage::ObBackupDataTabletToLSInfo &tablet_to_ls_info)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("tablet_to_ls info");
  PrintHelper::print_dump_line("ls_id", tablet_to_ls_info.ls_id_.id());
  PrintHelper::print_dump_list_start("tablet_id");
  ARRAY_FOREACH_X(tablet_to_ls_info.tablet_id_list_, i , cnt, OB_SUCC(ret)) {
    const common::ObTabletID &tablet_id = tablet_to_ls_info.tablet_id_list_.at(i);
    ObCStringHelper helper;
    const char *tablet_id_str = helper.convert(tablet_id);
    if (nullptr == tablet_id_str) {
      tablet_id_str = "NULL";
    }
    PrintHelper::print_dump_list_value(tablet_id_str, i == cnt - 1);
  }
  PrintHelper::print_dump_list_end();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_tenant_locality_info_(const storage::ObExternTenantLocalityInfoDesc &locality_info)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("locality info");
  PrintHelper::print_dump_line("tenant_id", locality_info.tenant_id_);
  PrintHelper::print_dump_line("tenant_name", locality_info.tenant_name_.ptr());
  PrintHelper::print_dump_line("backup_set_id", locality_info.backup_set_id_);
  PrintHelper::print_dump_line("cluster_id", locality_info.cluster_id_);
  PrintHelper::print_dump_line("cluster_name", locality_info.cluster_name_.ptr());
  PrintHelper::print_dump_line("compat_mode", static_cast<int64_t>(locality_info.compat_mode_));
  PrintHelper::print_dump_line("locality", locality_info.locality_.ptr());
  PrintHelper::print_dump_line("primary_zone", locality_info.primary_zone_.ptr());
  PrintHelper::print_dump_line("sys_time_zone", locality_info.sys_time_zone_.ptr());
  if (OB_FAIL(dump_locality_resource_pool_infos_(locality_info.resource_pool_infos_))) {
    STORAGE_LOG(WARN, "fail to dump locality resource pool info",
      K(ret), K(locality_info.resource_pool_infos_));
  } else {
    PrintHelper::print_end_line();
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_locality_resource_pool_infos_(
  const ObSArray<ObBackupResourcePool> &resource_pool_infos)
{
  int ret = OB_SUCCESS;
  char buf[OB_MAX_TEXT_LENGTH] = { 0 };
  char order_buf[OB_MAX_INTEGER_DISPLAY_WIDTH] = { 0 };

  PrintHelper::print_dump_line("resource_pool_list", "");
  ARRAY_FOREACH(resource_pool_infos, i) {
    int64_t pos = 0;
    const share::ObResourcePool &resource_pool = resource_pool_infos.at(i).resource_pool_;
    const share::ObUnitConfig &unit_config = resource_pool_infos.at(i).unit_config_;
    if (OB_FAIL(databuff_printf(buf, OB_MAX_TEXT_LENGTH, pos, "{resource_name: %s, zone_list: ",
      resource_pool.name_.ptr()))) {

    }
    ARRAY_FOREACH(resource_pool.zone_list_, j) {
      if (OB_FAIL(databuff_printf(buf, OB_MAX_TEXT_LENGTH, pos, "%s%s", (0 == j ? "" : ";"),
        resource_pool.zone_list_.at(j).ptr()))) {

      }
    }
    if (FAILEDx(databuff_printf(buf, OB_MAX_TEXT_LENGTH, pos, ", unit_count: %ld, unit: {"
      "name: %s, max_cpu: %.2f, min_cpu: %.2f, memory_size: %ld, log_disk_size: %ld, "
      "max_iops: %ld, min_iops: %ld, iops_weight: %ld}}",
      resource_pool.unit_count_, unit_config.name().ptr(), unit_config.max_cpu(),
      unit_config.min_cpu(), unit_config.memory_size(), unit_config.log_disk_size(),
      unit_config.max_iops(), unit_config.min_iops(), unit_config.iops_weight()))) {
      STORAGE_LOG(WARN, "failed to format resource pool info", K(ret), K(resource_pool),
        K(unit_config));
    } else if (OB_FAIL(databuff_printf(order_buf, OB_MAX_INTEGER_DISPLAY_WIDTH, "%ld", i))){

    } else {
      PrintHelper::print_dump_line(order_buf, buf);
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_parameters_info_(const storage::ObExternParamInfoDesc &param_info)
{
  int ret = OB_SUCCESS;
  char buf[OB_MAX_TEXT_LENGTH] = { 0 };
  char order_buf[OB_MAX_INTEGER_DISPLAY_WIDTH] = { 0 };

  PrintHelper::print_dump_title("parameter_list");
  ARRAY_FOREACH(param_info.param_array(), i) {
    int64_t pos = 0;
    const ObBackupParam &param = param_info.param_array().at(i);
    if (OB_FAIL(databuff_printf(buf, OB_MAX_TEXT_LENGTH, pos, "{name:%s, value:%.*s}",
      param.name_.ptr(), static_cast<int>(param.value_.length()), param.value_.ptr()))) {

    } else if (OB_FAIL(databuff_printf(order_buf, OB_MAX_INTEGER_DISPLAY_WIDTH, "%ld", i))){

    } else {
      PrintHelper::print_dump_line(order_buf, buf);
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    PrintHelper::print_end_line();
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_tenant_diagnose_info_(const storage::ObExternTenantDiagnoseInfoDesc &diagnose_info)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("diagnose info");
  if (OB_FAIL(dump_tenant_locality_info_(diagnose_info.tenant_locality_info_))) {

  } else if (OB_FAIL(dump_backup_set_info(diagnose_info.backup_set_file_))) {

  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_tenant_backup_set_infos_(const ObIArray<oceanbase::share::ObBackupSetFileDesc> &backup_set_infos)
{
  int ret = OB_SUCCESS;
  storage::ObBackupDataStore store;
  ObTimeZoneInfoWrap time_zone_wrap;
  ObBackupDest backup_set_dest;

  if (OB_FAIL(backup_set_dest.set(backup_path_, storage_info_))) {

  } else if (OB_FAIL(store.init(backup_set_dest))) {

  } else if (OB_FAIL(store.get_backup_sys_time_zone_wrap(time_zone_wrap))) {

  } else {
    PrintHelper::print_dump_title("tenant backup set infos");
    ARRAY_FOREACH_X(backup_set_infos, i , cnt, OB_SUCC(ret)) {
      const share::ObBackupSetFileDesc &backup_set_desc = backup_set_infos.at(i);
      int64_t pos = 0;
      char buf[OB_MAX_INTEGER_DISPLAY_WIDTH] = { 0 };
      char str_buf[OB_MAX_TEXT_LENGTH] = { 0 };
      char min_restore_scn_str_buf[OB_MAX_TIME_STR_LENGTH] = { 0 };
      if (OB_FAIL(ObTimeConverter::scn_to_str(backup_set_desc.min_restore_scn_.get_val_for_inner_table_field(), 
                                              time_zone_wrap.get_time_zone_info(),
                                              min_restore_scn_str_buf,
                                              OB_MAX_TIME_STR_LENGTH,
                                              pos))) {

      } else if (OB_FAIL(databuff_printf(buf, OB_MAX_INTEGER_DISPLAY_WIDTH, "%ld", i+1))) {

      } else if (OB_FALSE_IT(backup_set_desc.to_string(min_restore_scn_str_buf, str_buf, OB_MAX_TEXT_LENGTH))) {
      } else {
        PrintHelper::print_dump_line(buf, str_buf);
      }
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_tenant_backup_set_infos_file_(const ObIArray<oceanbase::share::ObBackupSetFileDesc> &backup_set_infos)
{
  int ret = OB_SUCCESS;
  if (share::ObBackupFileType::BACKUP_TENANT_SET_INFOS == file_type_) {
    PrintHelper::print_dump_title("tenant backup set infos");
    ARRAY_FOREACH_X(backup_set_infos, i , cnt, OB_SUCC(ret)) {
      const share::ObBackupSetFileDesc &backup_set_desc = backup_set_infos.at(i);
      char buf[OB_MAX_CHAR_LEN] = { 0 };
      char str_buf[OB_MAX_TEXT_LENGTH] = { 0 };
      if (OB_FAIL(databuff_printf(buf, OB_MAX_CHAR_LEN, "%ld", i+1))) {

      } else if (OB_FALSE_IT(backup_set_desc.to_string(str_buf, OB_MAX_TEXT_LENGTH))) {
      } else {
        PrintHelper::print_dump_line(buf, str_buf);
      }
    }
    PrintHelper::print_end_line();
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_backup_ls_meta_infos_file_(const storage::ObBackupLSMetaInfosDesc &ls_meta_infos)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("ls meta infos");
  ARRAY_FOREACH_X(ls_meta_infos.ls_meta_packages_, i , cnt, OB_SUCC(ret)) {
    const storage::ObLSMetaPackage &meta_package = ls_meta_infos.ls_meta_packages_.at(i);
    char buf[OB_MAX_INTEGER_DISPLAY_WIDTH] = { 0 };
    char str_buf[OB_MAX_TEXT_LENGTH] = { 0 };
    if (OB_FAIL(databuff_printf(buf, OB_MAX_INTEGER_DISPLAY_WIDTH, "%ld", i+1))) {

    } else if (OB_FALSE_IT(meta_package.to_string(str_buf, OB_MAX_TEXT_LENGTH))) {
    } else {
      PrintHelper::print_dump_line(buf, str_buf);
    }
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_backup_set_info(const share::ObBackupSetFileDesc &backup_set_info)
{
  int ret = OB_SUCCESS;
  char tenant_version_display[OB_CLUSTER_VERSION_LENGTH] = "";
  char cluster_version_display[OB_CLUSTER_VERSION_LENGTH] = "";
  int64_t pos =  ObClusterVersion::print_version_str(
    tenant_version_display, OB_CLUSTER_VERSION_LENGTH, backup_set_info.tenant_compatible_);
  pos =  ObClusterVersion::print_version_str(
    cluster_version_display, OB_CLUSTER_VERSION_LENGTH, backup_set_info.cluster_version_);

  PrintHelper::print_dump_title("backup set info");
  PrintHelper::print_dump_line("tenant_id", backup_set_info.tenant_id_);
  PrintHelper::print_dump_line("backup_set_id", backup_set_info.backup_set_id_);
  PrintHelper::print_dump_line("dest_id", backup_set_info.dest_id_);
  PrintHelper::print_dump_line("incarnation", backup_set_info.incarnation_);
  PrintHelper::print_dump_line("backup_type", backup_set_info.backup_type_.get_backup_type_str());
  PrintHelper::print_dump_line("backup_date", backup_set_info.date_);
  PrintHelper::print_dump_line("prev_full_backup_set_id", backup_set_info.prev_full_backup_set_id_);
  PrintHelper::print_dump_line("prev_inc_backup_set_id", backup_set_info.prev_inc_backup_set_id_);
  PrintHelper::print_dump_line("input_bytes", backup_set_info.stats_.input_bytes_);
  PrintHelper::print_dump_line("output_bytes", backup_set_info.stats_.output_bytes_);
  PrintHelper::print_dump_line("tablet_count", backup_set_info.stats_.tablet_count_);
  PrintHelper::print_dump_line("finish tablet count", backup_set_info.stats_.finish_tablet_count_);
  PrintHelper::print_dump_line("macro block count", backup_set_info.stats_.macro_block_count_);
  PrintHelper::print_dump_line("finish macro block count", backup_set_info.stats_.finish_macro_block_count_);
  PrintHelper::print_dump_line("extra_meta_bytes", backup_set_info.stats_.extra_bytes_);
  PrintHelper::print_dump_line("finish file count", backup_set_info.stats_.finish_file_count_);
  PrintHelper::print_dump_line("start_time", backup_set_info.start_time_);
  PrintHelper::print_dump_line("end_time", backup_set_info.end_time_);
  PrintHelper::print_dump_line("status", backup_set_info.status_);
  PrintHelper::print_dump_line("result", backup_set_info.result_);
  PrintHelper::print_dump_line("encryption_mode", backup_set_info.encryption_mode_);
  PrintHelper::print_dump_line("passwd", backup_set_info.passwd_.ptr());
  PrintHelper::print_dump_line("file_status", backup_set_info.file_status_);
  PrintHelper::print_dump_line("backup_path", backup_set_info.backup_path_.ptr());
  PrintHelper::print_dump_line("start_replay_scn", backup_set_info.start_replay_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("min_restore_scn", backup_set_info.min_restore_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("tenant_compatible", tenant_version_display);
  PrintHelper::print_dump_line("cluster_version", cluster_version_display);
  PrintHelper::print_dump_line("backup_compatible", backup_set_info.backup_compatible_);
  PrintHelper::print_dump_line("meta_turn_id", backup_set_info.meta_turn_id_);
  PrintHelper::print_dump_line("data_turn_id", backup_set_info.data_turn_id_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_archive_round_start_file_(const share::ObRoundStartDesc &round_start_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive round start info");
  PrintHelper::print_dump_line("dest_id", round_start_file.dest_id_);
  PrintHelper::print_dump_line("round_id", round_start_file.round_id_);
  PrintHelper::print_dump_line("start_scn", round_start_file.start_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("base_piece_id", round_start_file.base_piece_id_);
  PrintHelper::print_dump_line("piece_switch_interval", round_start_file.piece_switch_interval_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_archive_round_end_file_(const share::ObRoundEndDesc round_end_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive round end info");
  PrintHelper::print_dump_line("dest_id", round_end_file.dest_id_);
  PrintHelper::print_dump_line("round_id", round_end_file.round_id_);
  PrintHelper::print_dump_line("start_scn", round_end_file.start_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("checkpoint_scn", round_end_file.checkpoint_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("base_piece_id", round_end_file.base_piece_id_);
  PrintHelper::print_dump_line("piece_switch_interval", round_end_file.piece_switch_interval_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_archive_piece_start_file_(const share::ObPieceStartDesc &piece_start_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive piece start info");
  PrintHelper::print_dump_line("dest_id", piece_start_file.dest_id_);
  PrintHelper::print_dump_line("round_id", piece_start_file.round_id_);
  PrintHelper::print_dump_line("piece_id", piece_start_file.piece_id_);
  PrintHelper::print_dump_line("start_scn", piece_start_file.start_scn_.get_val_for_logservice());
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_archive_piece_end_file_(const share::ObPieceEndDesc &piece_end_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive piece end info");
  PrintHelper::print_dump_line("dest_id", piece_end_file.dest_id_);
  PrintHelper::print_dump_line("round_id", piece_end_file.round_id_);
  PrintHelper::print_dump_line("piece_id", piece_end_file.piece_id_);
  PrintHelper::print_dump_line("end_scn", piece_end_file.end_scn_.get_val_for_logservice());
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_tenant_archive_piece_infos_file_(const share::ObTenantArchivePieceInfosDesc &piece_infos_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive piece infos");
  PrintHelper::print_dump_line("dest_id", piece_infos_file.dest_id_);
  PrintHelper::print_dump_line("round_id", piece_infos_file.round_id_);
  PrintHelper::print_dump_line("piece_id", piece_infos_file.piece_id_);
  PrintHelper::print_dump_line("incarnation", piece_infos_file.incarnation_);
  PrintHelper::print_dump_line("dest_no", piece_infos_file.dest_no_);
  PrintHelper::print_dump_line("compatible", static_cast<int64_t>(piece_infos_file.compatible_.version_));
  PrintHelper::print_dump_line("start_scn", piece_infos_file.start_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("end_scn", piece_infos_file.end_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("path", piece_infos_file.path_.ptr());
  ARRAY_FOREACH_X(piece_infos_file.his_frozen_pieces_, i , cnt, OB_SUCC(ret)) {
    const ObTenantArchivePieceAttr &piece = piece_infos_file.his_frozen_pieces_.at(i);
    char buf[OB_MAX_INTEGER_DISPLAY_WIDTH] = { 0 };
    char str_buf[OB_MAX_TEXT_LENGTH] = { 0 };
    if (OB_FAIL(databuff_printf(buf, OB_MAX_INTEGER_DISPLAY_WIDTH, "%ld", i+1))) {

    } else if (OB_FALSE_IT(piece.to_string(str_buf, OB_MAX_TEXT_LENGTH))) {
    } else {
      PrintHelper::print_dump_line(buf, str_buf);
    }
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_one_piece_(const share::ObTenantArchivePieceAttr &piece)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_line("tenant_id", piece.key_.tenant_id_);
  PrintHelper::print_dump_line("dest_id", piece.key_.dest_id_);
  PrintHelper::print_dump_line("round_id", piece.key_.round_id_);
  PrintHelper::print_dump_line("piece_id", piece.key_.piece_id_);
  PrintHelper::print_dump_line("incarnation", piece.incarnation_);
  PrintHelper::print_dump_line("dest_no", piece.dest_no_);
  PrintHelper::print_dump_line("file_count", piece.file_count_);
  PrintHelper::print_dump_line("start_scn", piece.start_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("checkpoint_scn", piece.checkpoint_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("max_scn", piece.max_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("end_scn", piece.end_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("compatible", static_cast<int64_t>(piece.compatible_.version_));
  PrintHelper::print_dump_line("input_bytes", piece.input_bytes_);
  PrintHelper::print_dump_line("output_bytes", piece.output_bytes_);
  PrintHelper::print_dump_line("status", piece.status_.to_status_str());
  PrintHelper::print_dump_line("file_status", ObBackupFileStatus::get_str(piece.file_status_));
  PrintHelper::print_dump_line("cp_file_id", piece.cp_file_id_);
  PrintHelper::print_dump_line("cp_file_offset", piece.cp_file_offset_);
  PrintHelper::print_dump_line("path", piece.path_.ptr());
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_archive_single_piece_file_(const share::ObSinglePieceDesc &piece_single_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive single piece info");
  dump_one_piece_(piece_single_file.piece_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_archive_piece_inner_placeholder_file_(const share::ObPieceInnerPlaceholderDesc &piece_inner_placeholder)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive piece inner placeholder info");
  PrintHelper::print_dump_line("dest_id", piece_inner_placeholder.dest_id_);
  PrintHelper::print_dump_line("round_id", piece_inner_placeholder.round_id_);
  PrintHelper::print_dump_line("piece_id", piece_inner_placeholder.piece_id_);
  PrintHelper::print_dump_line("start_scn", piece_inner_placeholder.start_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("checkpoint_scn", piece_inner_placeholder.checkpoint_scn_.get_val_for_logservice());
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_archive_single_ls_info_file_(const share::ObSingleLSInfoDesc &single_ls_info_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive single ls info");
  PrintHelper::print_dump_line("dest_id", single_ls_info_file.dest_id_);
  PrintHelper::print_dump_line("round_id", single_ls_info_file.round_id_);
  PrintHelper::print_dump_line("piece_id", single_ls_info_file.piece_id_);
  PrintHelper::print_dump_line("ls_id", single_ls_info_file.ls_id_.id());
  PrintHelper::print_dump_line("checkpoint_scn", single_ls_info_file.checkpoint_scn_.get_val_for_logservice());
  PrintHelper::print_dump_line("max_lsn", single_ls_info_file.max_lsn_);
  ARRAY_FOREACH_X(single_ls_info_file.filelist_, i , cnt, OB_SUCC(ret)) {
    const ObSingleLSInfoDesc::OneFile &one_file = single_ls_info_file.filelist_.at(i);
    PrintHelper::print_dump_title("archive file", i, 1);
    PrintHelper::print_dump_line("file id", one_file.file_id_);
    PrintHelper::print_dump_line("bytes", one_file.size_bytes_);
    PrintHelper::print_end_line();
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_archive_piece_list_info_file_(const share::ObPieceInfoDesc &piece_info_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("archive piece list info");
  PrintHelper::print_dump_line("dest_id", piece_info_file.dest_id_);
  PrintHelper::print_dump_line("round_id", piece_info_file.round_id_);
  PrintHelper::print_dump_line("piece_id", piece_info_file.piece_id_);
  ARRAY_FOREACH_X(piece_info_file.filelist_, i , cnt, OB_SUCC(ret)) {
    const share::ObSingleLSInfoDesc &one_file = piece_info_file.filelist_.at(i);
    if (OB_FAIL(dump_archive_single_ls_info_file_(one_file))) {

    }
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_backup_format_file_(const share::ObBackupFormatDesc &format_file)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("backup format info");
  PrintHelper::print_dump_line("cluster_name", format_file.cluster_name_.ptr());
  PrintHelper::print_dump_line("cluster_id", format_file.cluster_id_);
  PrintHelper::print_dump_line("tenant_name", format_file.tenant_name_.ptr());
  PrintHelper::print_dump_line("tenant_id", format_file.tenant_id_);
  PrintHelper::print_dump_line("path", format_file.path_.ptr());
  PrintHelper::print_dump_line("incarnation", format_file.incarnation_);
  PrintHelper::print_dump_line("dest_id", format_file.dest_id_);
  PrintHelper::print_dump_line("dest_type", format_file.dest_type_);
  PrintHelper::print_dump_line("ts", format_file.ts_);
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_check_exist_result_(
    const char *data_path, const char *storage_info_str, const bool is_exist)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("check_exist");
  PrintHelper::print_dump_line("uri", data_path);
  PrintHelper::print_dump_line("is_exist", is_exist ? "true" : "false");
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_tablet_autoinc_seq_(const share::ObTabletAutoincSeq &autoinc_seq)
{
  int ret = OB_SUCCESS;
  const share::ObTabletAutoincInterval *intervals = autoinc_seq.get_intervals();
  for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_seq.get_intervals_count(); ++i) {
    const share::ObTabletAutoincInterval &interval = intervals[i];
    PrintHelper::print_dump_line("tablet_meta:autoinc_seq:start", interval.start_);
    PrintHelper::print_dump_line("tablet_meta:autoinc_seq:end", interval.end_);
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::get_tenant_backup_set_infos_path_(
    const share::ObBackupSetDesc &backup_set_dir_name,
    share::ObBackupPath &target_path)
{
  int ret = OB_SUCCESS;
  target_path.reset();
  if (OB_FAIL(target_path.init(backup_path_))) {

  } else if (OB_FAIL(target_path.join_backup_set(backup_set_dir_name))) {

  } else if (OB_FAIL(target_path.join(OB_STR_TENANT_BACKUP_SET_INFOS, ObBackupFileSuffix::BACKUP))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::get_backup_set_placeholder_dir_path(
    share::ObBackupPath &path)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest dest;
  if (OB_FAIL(dest.set(backup_path_, storage_info_))) {

  } else if (OB_FAIL(share::ObBackupPathUtil::get_backup_sets_dir_path(dest, path))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::filter_backup_set_(
    const storage::ObTenantBackupSetInfosDesc &tenant_backup_set_infos,
    const ObSArray<share::ObBackupSetDesc> &placeholder_infos,
    ObIArray<share::ObBackupSetFileDesc> &target_backup_set)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH_X(tenant_backup_set_infos.backup_set_infos_, i, cnt, OB_SUCC(ret)) {
    const share::ObBackupSetFileDesc &backup_set_info = tenant_backup_set_infos.backup_set_infos_.at(i);
    share::ObBackupSetDesc backup_set_dir_name;
    backup_set_dir_name.backup_set_id_ = backup_set_info.backup_set_id_;
    backup_set_dir_name.backup_type_.type_ = backup_set_info.backup_type_.type_;
    ARRAY_FOREACH_X(placeholder_infos, j, cnt, OB_SUCC(ret)) {
      const share::ObBackupSetDesc &tmp_name = placeholder_infos.at(j);
      if (tmp_name == backup_set_dir_name) {
        if (OB_FAIL(target_backup_set.push_back(backup_set_info))) {

        }
      }
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::read_locality_info_file(const char *tenant_backup_path, share::ObBackupSetDesc latest_backup_set_desc, storage::ObExternTenantLocalityInfoDesc &locality_info) {
  int ret = OB_SUCCESS;
  share::ObBackupDest tenant_backup_dest;
  share::ObBackupPath locality_info_path;
  if (OB_FAIL(tenant_backup_dest.set(tenant_backup_path, storage_info_))) {

  } else if (OB_FAIL(ObBackupPathUtil::get_locality_info_path(tenant_backup_dest, latest_backup_set_desc, locality_info_path))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(locality_info_path.get_obstr(), ObString(storage_info_), locality_info))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::check_tenant_backup_path_type_(const char *data_path, const char *storage_info_str, ObBackupDestType::TYPE &type)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_dest;
  share::ObBackupStore backup_store;
  ObBackupFormatDesc desc;
  bool is_exist = false;
  if (OB_FAIL(backup_dest.set(data_path, storage_info_str))) {

  } else if (OB_FAIL(backup_store.init(backup_dest))) {

  } else if (OB_FAIL(backup_store.is_format_file_exist(is_exist))) {

  } else if (!is_exist) {
    type = ObBackupDestType::TYPE::DEST_TYPE_MAX;
  } else if (OB_FAIL(backup_store.read_format_file(desc))) {

  } else {
    type = static_cast<ObBackupDestType::TYPE>(desc.dest_type_);
  }

  return ret;
}

int ObAdminDumpBackupDataExecutor::build_rounds_info_(
    const share::ObPieceKey &first_piece,
    const common::ObIArray<share::ObTenantArchivePieceAttr> &pieces,
    common::ObIArray<share::ObTenantArchiveRoundAttr> &rounds)
{
  int ret = OB_SUCCESS;
  ObTenantArchiveRoundAttr round;
  for (int64_t i = 0; OB_SUCC(ret) && i < pieces.count(); i++) {
    const ObTenantArchivePieceAttr &current_piece = pieces.at(i);
    if (current_piece.key_.piece_id_ < first_piece.piece_id_) {
      // May be it is cleaned.
      continue;
    }

    if (current_piece.key_.dest_id_ != first_piece.dest_id_) {
      // This piece is archived to other path.
      continue;
    }

    if (round.round_id_ != current_piece.key_.round_id_) {
      if (round.is_valid() && OB_FAIL(rounds.push_back(round))) {

      } else {
        round.key_.tenant_id_ = current_piece.key_.tenant_id_;
        round.key_.dest_no_ = current_piece.dest_no_;
        round.incarnation_ = current_piece.incarnation_;
        round.dest_id_ = current_piece.key_.dest_id_;
        round.round_id_ = current_piece.key_.round_id_;

        round.start_scn_ = current_piece.start_scn_;
        round.checkpoint_scn_ = current_piece.checkpoint_scn_;
        round.max_scn_ = current_piece.max_scn_;
        round.compatible_ = current_piece.compatible_;
        round.base_piece_id_ = current_piece.key_.piece_id_;
        round.used_piece_id_ = current_piece.key_.piece_id_;
        round.piece_switch_interval_ = current_piece.end_scn_.convert_to_ts() - current_piece.start_scn_.convert_to_ts();
        round.path_ = current_piece.path_;

        if (current_piece.is_active()) {
          round.state_.set_doing();
          round.active_input_bytes_ = current_piece.input_bytes_;
          round.active_output_bytes_ = current_piece.output_bytes_;
          round.frozen_input_bytes_ = 0;
          round.frozen_output_bytes_ = 0;
        } else {
          round.state_.set_stop();
          round.active_input_bytes_ = 0;
          round.active_output_bytes_ = 0;
          round.frozen_input_bytes_ = current_piece.input_bytes_;
          round.frozen_output_bytes_ = current_piece.output_bytes_;
        }
      }
    } else {
      round.used_piece_id_ = current_piece.key_.piece_id_;
      round.checkpoint_scn_ = current_piece.checkpoint_scn_;
      round.max_scn_ = current_piece.max_scn_;
      if (current_piece.is_active()) {
        round.state_.set_doing();
        round.active_input_bytes_ = current_piece.input_bytes_;
        round.active_output_bytes_ = current_piece.output_bytes_;
      } else {
        round.state_.set_stop();
        round.frozen_input_bytes_ = current_piece.input_bytes_;
        round.frozen_output_bytes_ = current_piece.output_bytes_;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!round.is_valid()) {

  } else if (OB_FAIL(rounds.push_back(round))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_tenant_archive_path_()
{
  int ret = OB_SUCCESS;
  ObArchiveStore store;
  ObBackupDest backup_dest;
  ObArray<ObPieceKey> piece_keys;
  ObArray<ObTenantArchiveRoundAttr> rounds;
  if (OB_FAIL(backup_dest.set(backup_path_, storage_info_))) {

  } else if (OB_FAIL(store.init(backup_dest))) {

  } else if (OB_FAIL(store.get_all_piece_keys(piece_keys))) {

  } else {
    bool is_empty_piece = true;
    ObExternPieceWholeInfo piece_whole_info;
    for (int64_t i = piece_keys.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      const ObPieceKey &key = piece_keys.at(i);
      if (OB_FAIL(store.get_whole_piece_info(key.dest_id_, key.round_id_, key.piece_id_, is_empty_piece, piece_whole_info))) {

      } else if (!is_empty_piece) {
        break;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (is_empty_piece) {
    } else if (OB_FAIL(piece_whole_info.his_frozen_pieces_.push_back(piece_whole_info.current_piece_))) {

    } else if (OB_FAIL(build_rounds_info_(piece_keys.at(0), piece_whole_info.his_frozen_pieces_, rounds))) {

    }
  }

  if (OB_FAIL(ret)) {
  } else {
    PrintHelper::print_dump_title("tenant archive round infos");
    ARRAY_FOREACH_X(rounds, i , cnt, OB_SUCC(ret)) {
      const ObTenantArchiveRoundAttr &round = rounds.at(i);
      char buf[OB_MAX_INTEGER_DISPLAY_WIDTH] = { 0 };
      char str_buf[OB_MAX_TEXT_LENGTH] = { 0 };
      if (OB_FAIL(databuff_printf(buf, OB_MAX_INTEGER_DISPLAY_WIDTH, "%ld", i+1))) {

      } else if (OB_FALSE_IT(round.to_string(str_buf, OB_MAX_TEXT_LENGTH))) {
      } else {
        PrintHelper::print_dump_line(buf, str_buf);
      }
    }
    PrintHelper::print_end_line();
  }

  return ret;
}

int ObAdminDumpBackupDataExecutor::check_is_table_list_dir_(bool &is_table_list_dir) {
  int ret = OB_SUCCESS;
  bool is_exist = false;
  is_table_list_dir = false;

  if (OB_FAIL(check_dir_exist_(backup_path_, storage_info_, is_exist))) {

  } else if (is_exist) {
    char tmp[OB_MAX_BACKUP_PATH_LENGTH] = { 0 };
    char *token = NULL;
    char *saveptr = NULL;
    const char *delimeter = "/";
    int64_t info_len = strlen(backup_path_); 
    int64_t pos = 0;
    if (OB_FAIL(databuff_printf(tmp, OB_MAX_BACKUP_PATH_LENGTH, pos, "%s", backup_path_))) {

    } else {
      token = strtok_r(tmp, delimeter, &saveptr);
      while (token != NULL) {
        char *next_token = strtok_r(NULL, delimeter, &saveptr);
        if (next_token == NULL) {
          break;
        }
        token = next_token;
      }
      if (OB_ISNULL(token)) {
        ret = OB_INVALID_ARGUMENT;

      } else if (0 == strcmp(OB_STR_TABLE_LIST, token)) {
        is_table_list_dir = true;
      }
    }
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_table_list_meta_info_(const uint64_t scn_val)
{
  int ret = OB_SUCCESS;
  ObBackupTableListMetaInfoDesc desc;

  if (OB_FAIL(read_table_list_meta_info_(scn_val, desc))) {

  } else if (OB_FAIL(dump_table_list_meta_info_(desc))) {

  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::read_table_list_meta_info_(const uint64_t scn_val, ObBackupTableListMetaInfoDesc &desc)
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  share::SCN scn;

  if (OB_FAIL(full_path.init(ObString(backup_path_)))) {

  } else if (OB_FAIL(scn.convert_for_inner_table_field(scn_val))) {

  } else if (OB_FAIL(full_path.join_table_list_meta_info_file(scn))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(full_path.get_obstr(), ObString(storage_info_), desc))) {

  } 
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_table_list_items_(const uint64_t scn_val)
{
  int ret = OB_SUCCESS;
  ObBackupTableListMetaInfoDesc meta_desc;

  if (OB_FAIL(read_table_list_meta_info_(scn_val, meta_desc))) {

  } else if (!meta_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;

  } else {
    PrintHelper::print_dump_title("table list");
    if (offset_ >= meta_desc.count_) {

    } else {
      int64_t print_order = 0;
      int64_t start_part_no = offset_ / meta_desc.batch_size_ + 1; // part_no starts from 1
      int64_t end_part_no = (offset_ + length_) < meta_desc.count_ ?
                        (offset_ + length_ - 1) / meta_desc.batch_size_ + 1 : (meta_desc.count_ - 1) / meta_desc.batch_size_ + 1;
      int64_t start_part_no_key = offset_ % meta_desc.batch_size_; // key starts from 0
      int64_t end_part_no_key = (offset_ + length_) < meta_desc.count_ ?
                            (offset_ + length_ - 1) % meta_desc.batch_size_ : (meta_desc.count_ - 1) % meta_desc.batch_size_;
      ObBackupPartialTableListDesc desc;

      for(int64_t part_no = start_part_no; OB_SUCC(ret) && part_no <= end_part_no; part_no++) {
        int64_t start_key = part_no == start_part_no ? start_part_no_key : 0;
        int64_t end_key = part_no  == end_part_no ? end_part_no_key : meta_desc.batch_size_ - 1;
        if (OB_FAIL(read_table_list_part_file_(scn_val, part_no, desc))) {

        } else {
          for (; start_key <= end_key; start_key++) {
            if (OB_FAIL(print_table_list_item_(desc, start_key, ++print_order))) {

            }
          }
        }
      }
    }
  }
  PrintHelper::print_end_line();
  return ret;
}

int ObAdminDumpBackupDataExecutor::print_table_list_item_(const ObBackupPartialTableListDesc &partial_desc,
                                                          const int64_t partial_offset, const int64_t print_order)
{
  int ret = OB_SUCCESS;
  char buf[OB_MAX_TEXT_LENGTH] = { 0 };
  ObString escape_str;
  char order_buf[OB_MAX_INTEGER_DISPLAY_WIDTH] = { 0 };
  ObArenaAllocator allocator(ObModIds::BACKUP);

  if (!partial_desc.is_valid() || partial_offset >= partial_desc.count()) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_FAIL(databuff_printf(order_buf, OB_MAX_INTEGER_DISPLAY_WIDTH, "%ld", print_order))) {

  } else if (OB_FALSE_IT(partial_desc.items_.at(partial_offset).to_string(buf, OB_MAX_TEXT_LENGTH))) {
  } else if (OB_FAIL((sql::ObSQLUtils::generate_new_name_with_escape_character(
                   allocator,
                   buf,
                   escape_str,
                   true)))) {

  } else {
    PrintHelper::print_dump_line(order_buf, buf);
  }
  return ret;
}

int ObAdminDumpBackupDataExecutor::read_table_list_part_file_(const uint64_t scn_val, const int64_t part_no, ObBackupPartialTableListDesc &desc)
{
  int ret = OB_SUCCESS;
  ObBackupPath full_path;
  share::SCN scn;

  if (part_no <= 0) {
    ret = OB_INVALID_ARGUMENT;

  } else if (OB_FAIL(scn.convert_for_inner_table_field(scn_val))) {

  } else if (OB_FAIL(full_path.init(ObString(backup_path_)))) {

  } else if (OB_FAIL(full_path.join_table_list_part_file(scn, part_no))) {

  } else if (OB_FAIL(ObAdminDumpBackupDataUtil::read_backup_info_file(full_path.get_obstr(), ObString(storage_info_), desc))) {

  } 
  return ret;
}

int ObAdminDumpBackupDataExecutor::dump_table_list_meta_info_(ObBackupTableListMetaInfoDesc &desc)
{
  int ret = OB_SUCCESS;
  PrintHelper::print_dump_title("table list");
  PrintHelper::print_dump_line("count", desc.count_);
  PrintHelper::print_dump_line("batch_size", desc.batch_size_);
  PrintHelper::print_end_line();
  return ret;
}

}  // namespace tools
}  // namespace oceanbase
