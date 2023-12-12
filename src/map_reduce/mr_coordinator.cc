#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <string>
#include <vector>

#include "common/logger.h"
#include "map_reduce/protocol.h"

namespace mapReduce {
std::tuple<int, int, std::string> Coordinator::askTask(int) {
  // Lab4 : Your code goes here.
  // Free to change the type of return value.
  if (!map_task.empty()) {
    auto file_index = map_task.back();
    map_task.pop_back();
    auto file = files[file_index];
    return {static_cast<int>(MAP), file_index, file};
  }
  if (finished_map_task != files.size()) {
    return {static_cast<int>(NONE), -1, ""};
  }
  if (!reduce_task.empty()) {
    auto index = reduce_task.back();
    reduce_task.pop_back();
    return {static_cast<int>(REDUCE), index, std::to_string(files.size())};
  }
  return {static_cast<int>(NONE), -1, ""};
}

int Coordinator::submitTask(int taskType, int index) {
  // Lab4 : Your code goes here.
  auto type = static_cast<mr_tasktype>(taskType);
  if (type == MAP) {
    finished_map_task++;
  }
  if (type == REDUCE) {
    isFinished = true;
  }
  return 0;
}

// mr_coordinator calls Done() periodically to find out
// if the entire job has finished.
bool Coordinator::Done() {
  std::unique_lock<std::mutex> uniqueLock(this->mtx);
  return this->isFinished;
}

// create a Coordinator.
// nReduce is the number of reduce tasks to use.
Coordinator::Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce) {
  this->files = files;
  this->isFinished = false;
  // Lab4: Your code goes here (Optional).

  for (int i = 0; i < files.size(); ++i) {
    map_task.emplace_back(i);
  }
  reduce_task.emplace_back(0);

  for (int i = 0; i < files.size(); ++i) {
    config.client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "intermediate_" + std::to_string(i));
  }

  rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
  rpc_server->bind(ASK_TASK, [this](int i) { return this->askTask(i); });
  rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index) { return this->submitTask(taskType, index); });
  rpc_server->run(true, 1);
}
}  // namespace mapReduce