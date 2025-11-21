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

#ifndef OCEANBASE_STORAGE_RPC_IPP_
#define OCEANBASE_STORAGE_RPC_IPP_

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace blocksstable;
namespace storage
{
template <ObRpcPacketCode RPC_CODE>
ObStorageStreamRpcReader<RPC_CODE>::ObStorageStreamRpcReader() : is_inited_(false)
                                                 , bandwidth_throttle_(NULL)
                                                 , rpc_buffer_()
                                                 , rpc_buffer_parse_pos_(0)
                                                 , allocator_("PartitionMigrat")
                                                 , last_send_time_(0)
                                                 , data_size_(0)
{
}

template <ObRpcPacketCode RPC_CODE>
int ObStorageStreamRpcReader<RPC_CODE>::init(
  ObInOutBandwidthThrottle &bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_size = 0;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;

  } else {
    bandwidth_throttle_ = &bandwidth_throttle;
    buf_size = OB_MALLOC_BIG_BLOCK_SIZE;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      buf_size = tenant_config->_storage_stream_rpc_buffer_size;
    }
  }

  if (OB_SUCC(ret)) {
    if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(buf_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;

    } else if (!rpc_buffer_.set_data(buf, buf_size)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;

    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObStorageStreamRpcReader<RPC_CODE>::fetch_next_buffer_if_need()
{
  int ret = OB_SUCCESS;
  bool need_fetch = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;

  } else if (OB_FAIL(check_need_fetch_next_buffer(need_fetch))) {

  } else if (need_fetch && OB_FAIL(fetch_next_buffer())) {
    if (OB_ITER_END != ret) {

    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObStorageStreamRpcReader<RPC_CODE>::fetch_and_decode(Data &data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;

  } else if (OB_FAIL(fetch_next_buffer_if_need())) {
    if (OB_ITER_END != ret) {

    }
  } else if (OB_FAIL(serialization::decode(rpc_buffer_.get_data(),
                                           rpc_buffer_.get_position(),
                                           rpc_buffer_parse_pos_,
                                           data))) {

  } else {

  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObStorageStreamRpcReader<RPC_CODE>::fetch_and_decode(ObIAllocator& allocator, Data &data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;

  } else if (OB_FAIL(fetch_next_buffer_if_need())) {
    if (OB_ITER_END != ret) {

    }
  } else if (OB_FAIL(data.deserialize(allocator,
                                      rpc_buffer_.get_data(),
                                      rpc_buffer_.get_position(),
                                      rpc_buffer_parse_pos_))) {

  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObStorageStreamRpcReader<RPC_CODE>::fetch_and_decode_list(ObIAllocator& allocator,
                                                       ObIArray<Data> &data_list)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;

  } else {
    Data tmp_data;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(fetch_next_buffer_if_need())) {
        if (OB_ITER_END != ret) {

        }
      } else if (OB_FAIL(tmp_data.deserialize(allocator,
                                              rpc_buffer_.get_data(),
                                              rpc_buffer_.get_position(),
                                              rpc_buffer_parse_pos_))) {

      } else if (OB_FAIL(data_list.push_back(tmp_data))) {

      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
template <typename Data>
int ObStorageStreamRpcReader<RPC_CODE>::fetch_and_decode_list(const int64_t data_list_count, ObIArray<Data> &data_list)
{
  int ret = OB_SUCCESS;
  int64_t index = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;

  } else if (data_list_count < 0) {
    ret = OB_INVALID_ARGUMENT;

  } else {
    Data tmp_data;
    while (OB_SUCC(ret) && index < data_list_count) {
      if (OB_FAIL(fetch_next_buffer_if_need())) {
        if (OB_ITER_END != ret) {

        }
      } else if (OB_FAIL(tmp_data.deserialize(rpc_buffer_.get_data(),
                                              rpc_buffer_.get_position(),
                                              rpc_buffer_parse_pos_))) {

      } else if (OB_FAIL(data_list.push_back(tmp_data))) {

      } else {
        index++;
      }
    }
  }

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  
  if (OB_SUCC(ret)) {
    if (data_list.count() != data_list_count) {
      ret = OB_ERR_UNEXPECTED;

    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObStorageStreamRpcReader<RPC_CODE>::check_need_fetch_next_buffer(bool &need_fetch)
{
  int ret = OB_SUCCESS;
  need_fetch = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;

  } else if (rpc_buffer_parse_pos_ < 0 || last_send_time_ < 0) {
    ret = OB_INVALID_ARGUMENT;

  } else if (rpc_buffer_.get_position() - rpc_buffer_parse_pos_ > 0) {
    // do nothing
    need_fetch = false;

  } else {
    need_fetch = true;
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int ObStorageStreamRpcReader<RPC_CODE>::fetch_next_buffer()
{
  int ret = OB_SUCCESS;
  const int64_t max_idle_time = OB_DEFAULT_STREAM_WAIT_TIMEOUT - OB_DEFAULT_STREAM_RESERVE_TIME;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;

  } else {
    int tmp_ret = bandwidth_throttle_->limit_in_and_sleep(rpc_buffer_.get_position(),
                                                          last_send_time_, max_idle_time);
    if (OB_SUCCESS != tmp_ret) {

    }

    rpc_buffer_.get_position() = 0;
    rpc_buffer_parse_pos_ = 0;
    if (handle_.has_more()) {
      handle_.reset_timeout();
      if (OB_FAIL(handle_.get_more(rpc_buffer_))) {

      } else if (rpc_buffer_.get_position() <= 0) {
        ret = OB_ERR_SYS;

      } else {

        data_size_ += rpc_buffer_.get_position();
      }
      last_send_time_ = ObTimeUtility::current_time();
    } else {
      ret = OB_ITER_END;

    }
  }
  return ret;
}

template <ObRpcPacketCode RPC_CODE>
int do_fetch_next_buffer_if_need(
    ObInOutBandwidthThrottle &bandwidth_throttle,
    ObDataBuffer &rpc_buffer,
    int64_t &rpc_buffer_parse_pos,
    ObStorageRpcProxy::SSHandle<RPC_CODE> &handle,
    int64_t &last_send_time,
    int64_t &total_data_size)
{
  int ret = OB_SUCCESS;
  const int64_t max_idle_time = OB_DEFAULT_STREAM_WAIT_TIMEOUT - OB_DEFAULT_STREAM_RESERVE_TIME;

  if (rpc_buffer_parse_pos < 0 || last_send_time < 0) {
    ret = OB_INVALID_ARGUMENT;

  } else if (rpc_buffer.get_position() - rpc_buffer_parse_pos > 0) {
    // do nothing

  } else {
    int tmp_ret = bandwidth_throttle.limit_in_and_sleep(rpc_buffer.get_position(), last_send_time, max_idle_time);
    if (OB_SUCCESS != tmp_ret) {

    }

    rpc_buffer.get_position() = 0;
    rpc_buffer_parse_pos = 0;
    if (handle.has_more()) {
      handle.reset_timeout();
      if (OB_FAIL(handle.get_more(rpc_buffer))) {

      } else if (rpc_buffer.get_position() < 0) {
        ret = OB_ERR_SYS;

      } else if (0 == rpc_buffer.get_position()) {
        if (!handle.has_more()) {
          ret = OB_ITER_END;

        } else {
          ret = OB_ERR_SYS;

        }
      } else {

        total_data_size += rpc_buffer.get_position();
      }
      last_send_time = ObTimeUtility::current_time();
    } else {
      ret = OB_ITER_END;

    }
  }
  return ret;
}

} // End of namespace storage
} // End of namespace oceanbase

#endif // OCEANBASE_STORAGE_RPC_IPP_
