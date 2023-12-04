#pragma once

#include "rpc/msgpack.hpp"
#include "rsm/raft/log.h"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
  /* Lab3: Your code here */
  int term;
  int candidate_id;
  int last_log_index;
  int last_log_term;
  MSGPACK_DEFINE(term, candidate_id, last_log_index, last_log_term)
};

struct RequestVoteReply {
  /* Lab3: Your code here */
  int term;
  bool vote_granted;
  MSGPACK_DEFINE(term, vote_granted)
};

template <typename Command>
struct AppendEntriesArgs {
  /* Lab3: Your code here */
  int term;
  int leader_id;
  int prev_log_index;
  int prev_log_term;
  int leader_commit;
  bool heart_beat;
  int last_include_index;
  std::vector<Entry<Command>> entries;
};

struct RpcAppendEntriesArgs {
  /* Lab3: Your code here */
  std::string data;

  MSGPACK_DEFINE(data)
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg) {
  /* Lab3: Your code here */
  std::stringstream ss;
  ss << arg.term << ' ';
  ss << arg.leader_id << ' ';
  ss << arg.prev_log_index << ' ';
  ss << arg.prev_log_term << ' ';
  ss << arg.leader_commit << ' ';
  ss << arg.heart_beat << ' ';
  ss << arg.last_include_index << ' ';
  ss << arg.entries.size() << ' ';
  for (const Entry<Command> &entry : arg.entries) {
    ss << entry.to_string() << ' ';
  }
  return RpcAppendEntriesArgs{ss.str()};
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg) {
  /* Lab3: Your code here */
  int term;
  int leader_id;
  int prev_log_index;
  int prev_log_term;
  int leader_commit;
  bool heart_beat;
  int last_include_index;
  int entry_size;
  int tmp_term;
  int value;
  std::vector<Entry<Command>> entries;
  auto str = rpc_arg.data;
  std::stringstream ss(str);
  ss >> term >> leader_id >> prev_log_index >> prev_log_term >> leader_commit >> heart_beat >> last_include_index >>
      entry_size;
  for (int i = 0; i < entry_size; ++i) {
    ss >> tmp_term >> value;
    entries.emplace_back(Entry<Command>{tmp_term, Command{value}});
  }
  return {term, leader_id, prev_log_index, prev_log_term, leader_commit, heart_beat, last_include_index, entries};
}

struct AppendEntriesReply {
  /* Lab3: Your code here */
  int term;
  bool success;
  MSGPACK_DEFINE(term, success)
};

struct InstallSnapshotArgs {
  /* Lab3: Your code here */
  int term;
  int leader_id;
  int last_included_index;
  int last_included_term;
  int offset;
  std::vector<uint8_t> data;
  bool done;
  MSGPACK_DEFINE(term, leader_id, last_included_index, last_included_term, offset, data, done)
};

struct InstallSnapshotReply {
  /* Lab3: Your code here */
  int term;
  MSGPACK_DEFINE(term)
};

} /* namespace chfs */