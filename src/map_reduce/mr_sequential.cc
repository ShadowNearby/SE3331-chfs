#include <algorithm>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/logger.h"
#include "map_reduce/protocol.h"
namespace mapReduce {
SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                         const std::vector<std::string> &files_, std::string resultFile) {
  chfs_client = std::move(client);
  files = files_;
  outPutFile = resultFile;
  // Your code goes here (optional)
}

void SequentialMapReduce::doWork() {
  std::map<std::string, std::vector<std::string>> map_result;
  for (const auto &filename : files) {
    auto file_inode_id = chfs_client->lookup(1, filename).unwrap();
    auto [type, attr] = chfs_client->get_type_attr(file_inode_id).unwrap();
    auto content = chfs_client->read_file(file_inode_id, 0, attr.size).unwrap();
    auto str_content = std::string(content.begin(), content.end());
    auto map_it_result = Map(str_content);
    for (const auto &[key, value] : map_it_result) {
      map_result[key].emplace_back(value);
    }
  }
  auto result_ss = std::stringstream();
  for (const auto &[key, values] : map_result) {
    auto count = Reduce(key, values);
    result_ss << key << ' ' << count << '\n';
  }
  auto result_content = result_ss.str();
  auto result = std::vector<uint8_t>(result_content.begin(), result_content.end());
  auto output_file_inode_id = chfs_client->lookup(1, outPutFile).unwrap();
  chfs_client->write_file(output_file_inode_id, 0, result);
  // Your code goes here
}
}  // namespace mapReduce