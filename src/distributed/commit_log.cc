#include <algorithm>

#include <chrono>
#include <utility>
#include "common/bitmap.h"
#include "common/logger.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"

namespace chfs {
/**
 * `CommitLog` part
 */
// {Your code here}
CommitLog::CommitLog(std::shared_ptr<BlockManager> bm, bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(std::move(bm)) {}

CommitLog::~CommitLog() {}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize { return txn_.size(); }

// {Your code here}
auto CommitLog::append_log(txn_id_t txn_id, std::vector<std::shared_ptr<BlockOperation>> ops) -> void {
  auto buffer = std::vector<u8>(DiskBlockSize);
  auto log_bitmap_block_id = bm_->total_blocks();
  bm_->read_block(log_bitmap_block_id, buffer.data());
  auto log_bitmap = Bitmap(buffer.data(), DiskBlockSize);
  for (const auto &op : ops) {
    auto target_block_index = log_bitmap.find_first_free_w_bound(1023).value();
    log_bitmap.set(target_block_index);
    auto target_block_id = log_bitmap_block_id + target_block_index;
    txn_[txn_id].emplace_back(op->block_id_, target_block_id);
    bm_->write_block(target_block_id, op->new_block_state_.data());
    //    LOG_FORMAT_INFO("{} {}", op->block_id_, target_block_id);
  }
  bm_->write_block(log_bitmap_block_id, buffer.data());
  txn_finish_[txn_id] = false;
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void { txn_finish_[txn_id] = true; }

// {Your code here}
auto CommitLog::checkpoint() -> void {
  auto buffer = std::vector<u8>(DiskBlockSize);
  auto bitmap_buffer = std::vector<u8>(DiskBlockSize);
  auto log_bitmap_block_id = bm_->total_blocks();
  bm_->read_block(log_bitmap_block_id, bitmap_buffer.data());
  auto log_bitmap = Bitmap(bitmap_buffer.data(), DiskBlockSize);
  std::vector<txn_id_t> finished;
  for (const auto &txn : txn_) {
    if (txn_finish_[txn.first]) {
      for (const auto &[op_block, log_block] : txn.second) {
        bm_->read_block(log_block, buffer.data());
        bm_->write_block(op_block, buffer.data());
        log_bitmap.clear(log_block - log_bitmap_block_id);
      }
      finished.emplace_back(txn.first);
    }
  }
  for (const auto &txn_id : finished) {
    txn_.erase(txn_id);
  }
  bm_->write_block(log_bitmap_block_id, bitmap_buffer.data());
  ops_.clear();
}

// {Your code here}
auto CommitLog::recover() -> void {
  auto buffer = std::vector<u8>(DiskBlockSize);
  auto bitmap_buffer = std::vector<u8>(DiskBlockSize);
  auto log_bitmap_block_id = bm_->total_blocks();
  bm_->read_block(log_bitmap_block_id, bitmap_buffer.data());
  auto log_bitmap = Bitmap(bitmap_buffer.data(), DiskBlockSize);
  std::vector<txn_id_t> finished;
  for (const auto &txn : txn_) {
    finished.emplace_back(txn.first);
    for (const auto &[op_block, log_block] : txn.second) {
      bm_->read_block(log_block, buffer.data());
      bm_->write_block(op_block, buffer.data());
      log_bitmap.clear(log_block - log_bitmap_block_id);
    }
  }
  for (const auto &txn_id : finished) {
    txn_.erase(txn_id);
  }
  bm_->write_block(log_bitmap_block_id, bitmap_buffer.data());
}
};  // namespace chfs