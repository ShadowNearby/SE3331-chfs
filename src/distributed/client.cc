#include "distributed/client.h"
#include <algorithm>
#include "common/logger.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"
namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address, u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
    case ServerType::DATA_SERVER:
      num_data_servers += 1;
      data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(address, port, reliable)});
      break;
    case ServerType::METADATA_SERVER:
      metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
      break;
    default:
      std::cerr << "Unknown Type" << std::endl;
      exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent, const std::string &name) -> ChfsResult<inode_id_t> {
  auto call_res = metadata_server_->call("mknode", (u8)type, parent, name);
  if (call_res.is_err()) {
    return ChfsResult<inode_id_t>{call_res.unwrap_error()};
  }
  auto res = call_res.unwrap()->as<inode_id_t>();
  if (res == 0) {
    return ChfsResult<inode_id_t>{ErrorType::AlreadyExist};
  }
  return ChfsResult<inode_id_t>{res};
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name) -> ChfsNullResult {
  auto call_res = metadata_server_->call("unlink", parent, name);
  if (call_res.is_err()) {
    return ChfsNullResult{call_res.unwrap_error()};
  }
  auto res = call_res.unwrap()->as<bool>();
  if (!res) {
    return ChfsNullResult{call_res.unwrap_error()};
  }
  return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name) -> ChfsResult<inode_id_t> {
  auto call_res = metadata_server_->call("lookup", parent, name);
  //  LOG_FORMAT_INFO("parent {} name {}", parent, name);
  if (call_res.is_err()) {
    return ChfsResult<inode_id_t>{call_res.unwrap_error()};
  }
  auto res = call_res.unwrap()->as<inode_id_t>();
  //  LOG_FORMAT_INFO("lookup result {}", res);
  //  if (res == 0) {
  //    return ChfsResult<inode_id_t>{ErrorType::INVALID};
  //  }
  return ChfsResult<inode_id_t>{res};
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id) -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  auto call_res = metadata_server_->call("readdir", id);
  if (call_res.is_err()) {
    return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>{call_res.unwrap_error()};
  }
  auto res = call_res.unwrap()->as<std::vector<std::pair<std::string, inode_id_t>>>();
  return ChfsResult<std::vector<std::pair<std::string, inode_id_t>>>{res};
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id) -> ChfsResult<std::pair<InodeType, FileAttr>> {
  auto call_res = metadata_server_->call("get_type_attr", id);
  if (call_res.is_err()) {
    return ChfsResult<std::pair<InodeType, FileAttr>>{call_res.unwrap_error()};
  }
  auto res = call_res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
  auto inode_type = (InodeType)std::get<4>(res);
  auto file_size = std::get<0>(res);
  if (inode_type == InodeType::FILE) {
    auto block_result = metadata_server_->call("get_block_map", id);
    if (block_result.is_err()) {
      return {block_result.unwrap_error()};
    }
    auto block_maps = block_result.unwrap()->as<std::vector<BlockInfo>>();
    file_size = block_maps.size() * DiskBlockSize;
  }
  LOG_FORMAT_INFO("file size {} type {}", file_size, (u8)inode_type);
  return ChfsResult<std::pair<InodeType, FileAttr>>{
      std::make_pair(inode_type, FileAttr{std::get<1>(res), std::get<2>(res), std::get<3>(res), file_size})};
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size) -> ChfsResult<std::vector<u8>> {
  auto call_result = metadata_server_->call("get_block_map", id);
  if (call_result.is_err()) {
    return ChfsResult<std::vector<u8>>{call_result.unwrap_error()};
  }
  auto block_maps = call_result.unwrap()->as<std::vector<BlockInfo>>();
  auto result = std::vector<u8>(size);
  usize read_size = 0;
  for (const auto &[block_id, mac_id, version] : block_maps) {
    if (offset > DiskBlockSize) {
      offset -= DiskBlockSize;
      continue;
    }
    if (DiskBlockSize - offset > size) {
      auto data_call = data_servers_[mac_id]->call("read_data", block_id, offset, size, version);
      if (data_call.is_err()) {
        return ChfsResult<std::vector<u8>>{data_call.unwrap_error()};
      }
      auto data = data_call.unwrap()->as<std::vector<u8>>();
      std::move(data.begin(), data.end(), result.begin() + read_size);
      break;
    }
    auto data_call = data_servers_[mac_id]->call("read_data", block_id, offset, DiskBlockSize - offset, version);
    if (data_call.is_err()) {
      return ChfsResult<std::vector<u8>>{data_call.unwrap_error()};
    }
    auto data = data_call.unwrap()->as<std::vector<u8>>();
    std::move(data.begin(), data.end(), result.begin() + read_size);
    size -= DiskBlockSize - offset;
    read_size += DiskBlockSize - offset;
    offset = 0;
  }
  return ChfsResult<std::vector<u8>>{result};
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data) -> ChfsNullResult {
  auto block_map_call = metadata_server_->call("get_block_map", id);
  if (block_map_call.is_err()) {
    return ChfsNullResult{block_map_call.unwrap_error()};
  }
  auto block_maps = block_map_call.unwrap()->as<std::vector<BlockInfo>>();
  auto write_loop = [&](block_id_t block_id, mac_id_t mac_id) {
    if (DiskBlockSize - offset > data.size()) {
      auto data_call = data_servers_[mac_id]->call("write_data", block_id, offset, data);
      if (data_call.is_err()) {
        return ChfsNullResult{data_call.unwrap_error()};
      }
      data.clear();
      return KNullOk;
    }
    auto partial_data = std::vector<u8>(data.begin(), data.begin() + DiskBlockSize - offset);
    auto data_call = data_servers_[mac_id]->call("write_data", block_id, offset, partial_data);
    if (data_call.is_err()) {
      return ChfsNullResult{data_call.unwrap_error()};
    }
    std::move(data.begin() + DiskBlockSize - offset, data.end(), data.begin());
    data.erase(data.end() - (DiskBlockSize - offset), data.end());
    offset = 0;
    return KNullOk;
  };
  for (const auto &block_map : block_maps) {
    if (data.empty()) {
      return KNullOk;
    }
    if (offset > DiskBlockSize) {
      offset -= DiskBlockSize;
      continue;
    }
    block_id_t block_id = std::get<0>(block_map);
    mac_id_t mac_id = std::get<1>(block_map);
    auto loop_res = write_loop(block_id, mac_id);
    if (loop_res.is_err()) {
      return ChfsNullResult{loop_res.unwrap_error()};
    }
  }
  while (!data.empty()) {
    auto alloc_call = metadata_server_->call("alloc_block", id);
    if (alloc_call.is_err()) {
      return ChfsNullResult{alloc_call.unwrap_error()};
    }
    if (offset > DiskBlockSize) {
      offset -= DiskBlockSize;
      continue;
    }
    auto [block_id, mac_id, version] = alloc_call.unwrap()->as<BlockInfo>();
    auto loop_res = write_loop(block_id, mac_id);
    if (loop_res.is_err()) {
      return ChfsNullResult{loop_res.unwrap_error()};
    }
  }
  return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id, mac_id_t mac_id) -> ChfsNullResult {
  auto call = metadata_server_->call("free_block", id, block_id, mac_id);
  if (call.is_err()) {
    return ChfsNullResult{call.unwrap_error()};
  }
  return KNullOk;
}

}  // namespace chfs