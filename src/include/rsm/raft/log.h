#pragma once

#include <cstring>
#include <mutex>
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
  std::string to_string() const { return fmt::format("term:{} command:{}", term, command.value); }
};

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

 private:
  std::shared_ptr<BlockManager> bm_;
  std::mutex mtx_;
  std::vector<Entry<Command>> data_;
  /* Lab3: Your code here */
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm) : bm_(std::move(bm)), mtx_{}, data_{} {
  /* Lab3: Your code here */
}

template <typename Command>
RaftLog<Command>::~RaftLog() {
  /* Lab3: Your code here */
}

/* Lab3: Your code here */

} /* namespace chfs */
