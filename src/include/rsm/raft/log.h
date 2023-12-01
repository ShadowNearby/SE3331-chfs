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
};

template <typename Command>
class RaftLog {
 public:
  explicit RaftLog(std::shared_ptr<BlockManager> bm);
  ~RaftLog();

  /* Lab3: Your code here */
  auto Size() -> int { return (int)data_.size(); }
  auto Empty() -> bool { return data_.empty(); }
  auto Append(Entry<Command> entry) -> void { data_.emplace_back(entry); }
  auto Data() -> Entry<Command> * { return data_.data(); }
  auto Back() -> Entry<Command> { return data_.back(); }
  auto At(int index) -> Entry<Command> { return data_.at(index); }
  auto EraseAllAfterIndex(int index) -> void {
    if (index >= data_.size()) {
      return;
    }
    data_.erase(data_.begin() + index, data_.end());
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
