#include "distributed/dataserver.h"

#include "common/util.h"
#include <memory>

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  if (is_initialized) {
    auto version_blocks_cnt =
        (KDefaultBlockCnt * sizeof(version_t)) / DiskBlockSize;
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, version_blocks_cnt, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    auto version_blocks_cnt =
        (KDefaultBlockCnt * sizeof(version_t)) / DiskBlockSize;
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, version_blocks_cnt, true);
    for (block_id_t i = 0; i < version_blocks_cnt; i++) {
      bm->zero_block(i);
    }
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  auto buffer = std::vector<u8>(DiskBlockSize);
  auto block_index = block_id / (DiskBlockSize / sizeof(version_t));
  auto version_index = block_id % (DiskBlockSize / sizeof(version_t));
  block_allocator_->bm->read_block(block_index, buffer.data());
  auto value = *(version_t *)(buffer.data() +
                              version_index * (sizeof(version_t) / sizeof(u8)));
  if (value != version) {
    return std::vector<u8>(0);
  }
  block_allocator_->bm->read_block(block_id, buffer.data());
  auto result = std::vector<u8>(len);
  std::move(buffer.begin() + offset, buffer.begin() + offset + len,
            result.begin());
  return result;
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  block_allocator_->bm->write_partial_block(block_id, buffer.data(), offset,
                                            buffer.size());
  return true;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  auto res = block_allocator_->allocate();
  block_id_t block_id = res.unwrap();
  auto buffer = std::vector<u8>(DiskBlockSize);
  auto block_index = block_id / (DiskBlockSize / sizeof(version_t));
  auto version_index = block_id % (DiskBlockSize / sizeof(version_t));
  block_allocator_->bm->read_block(block_index, buffer.data());
  auto value_ptr =
      (version_t *)(buffer.data() +
                    version_index * (sizeof(version_t) / sizeof(u8)));
  auto value = *value_ptr;
  *value_ptr = value + 1;
  block_allocator_->bm->write_block(block_index, buffer.data());
  return {block_id, value + 1};
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  auto res = block_allocator_->deallocate(block_id);
  if (res.is_err()) {
    return false;
  }
  auto buffer = std::vector<u8>(DiskBlockSize);
  auto block_index = block_id / (DiskBlockSize / sizeof(version_t));
  auto version_index = block_id % (DiskBlockSize / sizeof(version_t));
  block_allocator_->bm->read_block(block_index, buffer.data());
  auto value_ptr =
      (version_t *)(buffer.data() +
                    version_index * (sizeof(version_t) / sizeof(u8)));
  auto value = *value_ptr;
  *value_ptr = value + 1;
  block_allocator_->bm->write_block(block_index, buffer.data());
  return true;
}
} // namespace chfs