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

#include "map_reduce/protocol.h"

namespace mapReduce {

Worker::Worker(MR_CoordinatorConfig config) {
  mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
  outPutFile = config.resultFile;
  chfs_client = config.client;
  work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
  // Lab4: Your code goes here (Optional).
}

void Worker::doMap(int index, const std::string &filename) {
  // Lab4: Your code goes here.
}

void Worker::doReduce(int index, int nfiles) {
  // Lab4: Your code goes here.
}

void Worker::doSubmit(mr_tasktype taskType, int index) {
  // Lab4: Your code goes here.
}

void Worker::stop() {
  shouldStop = true;
  work_thread->join();
}

void Worker::doWork() {
  while (!shouldStop) {
    // Lab4: Your code goes here.
  }
}
}  // namespace mapReduce