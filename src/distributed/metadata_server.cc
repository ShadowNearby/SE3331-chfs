#include "distributed/metadata_server.h"
#include <fstream>
#include "common/util.h"
#include "filesystem/directory_op.h"

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode", [this](u8 type, inode_id_t parent, std::string const &name) {
    return this->mknode(type, parent, name);
  });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) { return this->unlink(parent, name); });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) { return this->lookup(parent, name); });
  server_->bind("get_block_map", [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block", [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block", [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
    return this->free_block(id, block, machine_id);
  });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr", [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually." << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager, DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers = 0;  // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_) operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_, is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path, bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed), is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_, is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port, const std::string &data_path, bool is_log_enabled,
                               bool is_checkpoint_enabled, bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed), is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_, is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name) -> inode_id_t {
  std::scoped_lock<std::shared_mutex> lock(meta_mtx_);
  auto res = operation_->mk_helper(parent, name.c_str(), InodeType(type));
  if (commit_log != nullptr) {
    commit_log->append_log(CommitLog::curr_txn_id_, CommitLog::ops_);
    commit_log->commit_log(CommitLog::curr_txn_id_);
    commit_log->checkpoint();
    CommitLog::curr_txn_id_++;
    if (CommitLog::txn_fail_) {
      CommitLog::txn_fail_ = false;
      return KInvalidInodeID;
    }
  }
  if (res.is_err()) {
    return KInvalidInodeID;
  }
  return res.unwrap();
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name) -> bool {
  std::scoped_lock<std::shared_mutex> lock(meta_mtx_);
  auto res = operation_->unlink(parent, name.c_str());
  if (commit_log != nullptr) {
    commit_log->append_log(CommitLog::curr_txn_id_, CommitLog::ops_);
    commit_log->commit_log(CommitLog::curr_txn_id_);
    commit_log->checkpoint();
    CommitLog::curr_txn_id_++;
    if (CommitLog::txn_fail_) {
      CommitLog::txn_fail_ = false;
      return KInvalidInodeID;
    }
  }

  if (res.is_ok()) {
    return true;
  }
  return false;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name) -> inode_id_t {
  std::scoped_lock<std::shared_mutex> lock(meta_mtx_);
  inode_id_t res_val;
  auto res = operation_->lookup(parent, name.c_str());
  if (res.is_err()) {
    return KInvalidInodeID;
  }
  res_val = res.unwrap();
  return res_val;
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  auto buffer = std::vector<u8>();
  {
    meta_mtx_.lock_shared();
    buffer = operation_->read_file(id).unwrap();
    meta_mtx_.unlock_shared();
  }
  std::vector<BlockInfo> result{};
  for (u32 i = 0; i < buffer.size() / (sizeof(BlockInfo) / sizeof(u8)); ++i) {
    auto info = *((BlockInfo *)buffer.data() + i);
    if (std::get<0>(info) != 0) {
      result.emplace_back(info);
    }
  }
  return result;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  auto mac_id = generator.rand(1, num_data_servers);
  auto call_res = ChfsResult<std::shared_ptr<RpcResponse>>{std::make_shared<RpcResponse>()};
  {
    server_mtx_[mac_id - 1].lock();
    call_res = clients_[mac_id]->call("alloc_block");
    server_mtx_[mac_id - 1].unlock();
  }
  auto res = call_res.unwrap()->as<std::pair<block_id_t, version_t>>();
  auto res_block_info = BlockInfo{res.first, mac_id, res.second};
  {
    std::scoped_lock<std::shared_mutex> lock(meta_mtx_);
    auto buffer = std::vector<u8>();
    buffer = operation_->read_file(id).unwrap();
    u32 i;
    for (i = 0; i < buffer.size() / (sizeof(BlockInfo) / sizeof(u8)); ++i) {
      auto [block, mac, version] = *((BlockInfo *)buffer.data() + i);
      if (block == 0 && mac == 0 && version == 0) {
        *((BlockInfo *)buffer.data() + i) = res_block_info;
        break;
      }
    }
    if (i == buffer.size() / (sizeof(BlockInfo) / sizeof(u8))) {
      auto res_buffer = std::vector<u8>(sizeof(BlockInfo));
      *((BlockInfo *)res_buffer.data()) = res_block_info;
      buffer.insert(buffer.end(), res_buffer.begin(), res_buffer.end());
    }
    operation_->write_file(id, buffer);
    return res_block_info;
  }
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id, mac_id_t machine_id) -> bool {
  {
    server_mtx_[machine_id - 1].lock();
    auto call_res = clients_[machine_id]->call("free_block", block_id).unwrap()->as<bool>();
    if (!call_res) {
      return false;
    }
    server_mtx_[machine_id - 1].unlock();
  }
  {
    std::scoped_lock<std::shared_mutex> lock(meta_mtx_);
    auto buffer = std::vector<u8>();
    buffer = operation_->read_file(id).unwrap();
    for (u32 i = 0; i < buffer.size() / sizeof(BlockInfo); ++i) {
      auto [block, mac, version] = *((BlockInfo *)buffer.data() + i);
      if (block == block_id && mac == machine_id) {
        *((BlockInfo *)buffer.data() + i) = std::make_tuple(0, 0, 0);
        break;
      }
    }
    operation_->write_file(id, buffer);
    return true;
  }
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node) -> std::vector<std::pair<std::string, inode_id_t>> {
  std::scoped_lock<std::shared_mutex> lock(meta_mtx_);
  std::list<DirectoryEntry> list;
  std::vector<std::pair<std::string, inode_id_t>> result;
  read_directory(operation_.get(), node, list);
  for (const auto &item : list) {
    result.emplace_back(item.name, item.id);
  }
  return result;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id) -> std::tuple<u64, u64, u64, u64, u8> {
  std::scoped_lock<std::shared_mutex> lock(meta_mtx_);
  auto result = std::make_pair<InodeType, FileAttr>({}, {});
  result = operation_->inode_manager_->get_type_attr(id).unwrap();
  auto [type, attr] = result;
  return {attr.size, attr.atime, attr.mtime, attr.ctime, static_cast<u64>(type)};
}

auto MetadataServer::reg_server(const std::string &address, u16 port, bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));
  return true;
}

auto MetadataServer::run() -> bool {
  if (running) return false;

  // Currently we only support async start
  server_mtx_ = std::vector<std::shared_mutex>(num_data_servers);
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

}  // namespace chfs