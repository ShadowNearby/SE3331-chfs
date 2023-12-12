#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/logger.h"
#include "map_reduce/protocol.h"

namespace mapReduce {

Worker::Worker(MR_CoordinatorConfig config) {
  mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
  outPutFile = config.resultFile;
  chfs_client = config.client;
  work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
  // Lab4: Your code goes here (Optisonal).
}

void Worker::doMap(int index, const std::string &filename) {
  // Lab4: Your code goes here.
  auto file_inode_id = chfs_client->lookup(1, filename).unwrap();
  auto [type, attr] = chfs_client->get_type_attr(file_inode_id).unwrap();
  auto content = chfs_client->read_file(file_inode_id, 0, attr.size).unwrap();
  auto str_content = std::string(content.begin(), content.end());
  auto count_map = CountMap(str_content);

  auto intermediate_filename = "intermediate_" + std::to_string(index);
  auto intermediate_file_inode_id = chfs_client->lookup(1, intermediate_filename).unwrap();
  chfs_client->write_file(intermediate_file_inode_id, 0, SerializeCountMap(count_map));
}

void Worker::doReduce(int index, int nfiles) {
  // Lab4: Your code goes here.
  std::map<std::string, int> count_map;
  for (int i = 0; i < nfiles; ++i) {
    auto intermediate_filename = "intermediate_" + std::to_string(i);
    auto intermediate_file_inode_id = chfs_client->lookup(1, intermediate_filename).unwrap();
    auto [intermediate_type, intermediate_attr] = chfs_client->get_type_attr(intermediate_file_inode_id).unwrap();
    auto intermediate_content = chfs_client->read_file(intermediate_file_inode_id, 0, intermediate_attr.size).unwrap();
    DeserializeCountMap(intermediate_content, count_map);
  }
  auto file_inode_id = chfs_client->lookup(1, outPutFile).unwrap();
  chfs_client->write_file(file_inode_id, 0, SerializeCountMap(count_map));
}

void Worker::doSubmit(mr_tasktype taskType, int index) {
  // Lab4: Your code goes here.
  mr_client->call(SUBMIT_TASK, static_cast<int>(taskType), index);
}

void Worker::stop() {
  shouldStop = true;
  work_thread->join();
}

void Worker::doWork() {
  while (!shouldStop) {
    // Lab4: Your code goes here.
    auto call = mr_client->call(ASK_TASK, 0);
    if (call.is_err()) {
      continue;
    }
    auto [type, index, file] = call.unwrap()->as<std::tuple<int, int, std::string>>();
    auto work_type = static_cast<mr_tasktype>(type);
    if (work_type == NONE) {
      continue;
    }
    if (work_type == MAP) {
      doMap(index, file);
    }
    if (work_type == REDUCE) {
      doReduce(index, std::stoi(file));
    }
    doSubmit(work_type, index);
  }
}
}  // namespace mapReduce