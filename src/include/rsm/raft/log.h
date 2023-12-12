#pragma once

#include <cstring>
#include <mutex>
#include <sstream>
#include <utility>
#include <vector>
#include "block/manager.h"
#include "common/macros.h"

namespace chfs {

/**
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command>
struct Entry {
  int term;
  Command command;
  std::string to_string() const { return fmt::format("{} {}", term, command.value); }
};
template <typename Command>

std::string entries_to_str(std::vector<chfs::Entry<Command>> entries) {
  std::string entries_str = "[";
  for (const chfs::Entry<Command> &entry : entries) {
    entries_str += entry.to_string() + ",";
  }
  if (entries_str.size() != 1) {
    entries_str.pop_back();
  }
  entries_str += "]";
  return entries_str;
}

template <typename Command>
class RaftLog {
 public:
  explicit RaftLog(std::shared_ptr<BlockManager> bm);
  ~RaftLog();

  /* Lab3: Your code here */
  auto Size() -> int {
    std::scoped_lock<std::mutex> lock(mtx_);
    return (int)data_.size();
  }
  auto Empty() -> bool {
    std::scoped_lock<std::mutex> lock(mtx_);
    return data_.empty();
  }
  auto Append(Entry<Command> entry) -> void {
    std::scoped_lock<std::mutex> lock(mtx_);
    data_.emplace_back(entry);
  }
  auto Insert(int index, Entry<Command> entry) {
    std::scoped_lock<std::mutex> lock(mtx_);
    if (index >= data_.size()) {
      std::vector<Entry<Command>> new_data{index - data_.size() + 1};
      data_.insert(data_.end(), new_data.begin(), new_data.end());
    }
    data_[index] = entry;
  }
  auto Data() {
    std::scoped_lock<std::mutex> lock(mtx_);
    return data_;
  }
  auto Back() -> Entry<Command> {
    std::scoped_lock<std::mutex> lock(mtx_);
    return data_.back();
  }
  auto At(int index) -> Entry<Command> {
    std::scoped_lock<std::mutex> lock(mtx_);
    if (index >= data_.size()) {
      for (int i = 0; i < index - data_.size() + 1; ++i) {
        data_.emplace_back(Entry<Command>{0, 0});
      }
      //      LOG_FORMAT_ERROR("error in here {} {}", index, data_.size());
    }
    return data_.at(index);
  }
  auto EraseAllAfterIndex(int index) -> void {
    std::scoped_lock<std::mutex> lock(mtx_);
    if (index >= data_.size()) {
      return;
    }
    data_.erase(data_.begin() + index, data_.end());
  }
  auto GetAllAfterIndex(int index) -> std::vector<Entry<Command>> {
    std::scoped_lock<std::mutex> lock(mtx_);
    if (data_.size() <= index) {
      return {};
    }
    auto begin = data_.begin() + index + 1;
    auto end = data_.end();
    return {begin, end};
  }
  auto Persist() -> int {
    std::scoped_lock<std::mutex> lock(mtx_);
    std::vector<uint8_t> buffer(bm_->block_size());
    auto block_idx = 1;
    //    LOG_FORMAT_INFO("persist {}", entries_to_str(data_));
    int sum = 0;
    int entry_per_block = bm_->block_size() / sizeof(Entry<Command>);
    int i = 0;
    auto size = data_.size();
    while (true) {
      auto index = i + (block_idx - 1) * entry_per_block;
      if (index >= data_.size()) {
        LOG_FORMAT_ERROR("index {} i {} block_idx {} per {} sum {} size {}", index, i, block_idx, entry_per_block, sum,
                         data_.size());
      }
      auto entry = data_.at(index);
      *((Entry<Command> *)buffer.data() + i) = entry;
      if (i == entry_per_block - 1) {
        bm_->write_block(block_idx, buffer.data());
        block_idx++;
        buffer.clear();
        buffer.resize(bm_->block_size());
        i = 0;
      } else {
        i++;
      }
      sum++;
      if (sum == size) {
        break;
      }
    }
    bm_->write_block(block_idx, buffer.data());
    return block_idx;
  }
  void Recover(int block_num) {
    std::scoped_lock<std::mutex> lock(mtx_);
    data_.clear();
    std::vector<uint8_t> buffer(bm_->block_size());
    bool first = true;
    for (int i = 1; i <= block_num; ++i) {
      bm_->read_block(i, buffer.data());
      for (int j = 0; j < bm_->block_size() / sizeof(Entry<Command>); ++j) {
        Entry<Command> entry = *((Entry<Command> *)buffer.data() + j);
        if (entry.term == 0 && entry.command.value == 0) {
          if (!first) {
            break;
          }
          first = false;
        }
        data_.emplace_back(entry);
      }
    }
  }
  void SaveSnapshot(int node_id, int last_included_index, int last_included_term, int offset, std::vector<u8> data,
                    bool done) {
    int current_snapshot_index = snapshot_index + 1;
    std::string file_name = fmt::format("/tmp/raft_log/node_{}_index_{}.snapshot", node_id, current_snapshot_index);
    std::fstream fs(file_name, std::ios::binary);
    if ((!fs.good() && offset != 0) || (fs.good() && offset == 0)) {
      return;
    }
    fs.open(file_name, std::ios::binary | std::ios::out);
    fs.seekp(offset, std::ios::beg);
    fs.write(reinterpret_cast<char *>(data.data()), data.size());
    fs.close();
    if (!done) {
      return;
    }
    for (int i = 0; i < current_snapshot_index; ++i) {
      std::remove(fmt::format("/tmp/raft_log/node_{}_index_{}.snapshot", node_id, i).c_str());
    }
    snapshot_index = current_snapshot_index;
    //    if (last_included_index <= (int)data_.size() - 1 && last_included_term == data_.at(last_included_index).term)
    //    {
    //      data_.erase(data_.begin() + 1, data_.begin() + last_included_index + 1);
    //    } else {
    //      data_.erase(data_.begin() + 1, data_.end());
    //    }
    //    LOG_FORMAT_INFO("after save snapshot {}", entries_to_str(data_));
  }
  std::vector<u8> GetSnapshot() {
    std::stringstream ss;
    int size = data_.size();
    ss << size << ' ';
    for (int i = 0; i < size; ++i) {
      ss << data_.at(i).command.value << ' ';
    }
    auto str = ss.str();
    return std::vector<u8>{str.begin(), str.end()};
  }
  int GetSnapshotIndex() { return snapshot_index; }

 private:
  std::shared_ptr<BlockManager> bm_;
  std::mutex mtx_;
  std::vector<Entry<Command>> data_;
  int snapshot_index{-1};
  /* Lab3: Your code here */
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm) : bm_(bm), mtx_{}, data_{} {
  /* Lab3: Your code here */
}

template <typename Command>
RaftLog<Command>::~RaftLog() {
  /* Lab3: Your code here */
}

/* Lab3: Your code here */

} /* namespace chfs */
