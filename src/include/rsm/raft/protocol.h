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
  std::vector<Entry<Command>> entries;
  int leader_commit;
};

struct RpcAppendEntriesArgs {
  /* Lab3: Your code here */
  int term;
  int leader_id;
  int prev_log_index;
  int prev_log_term;
  std::vector<uint8_t> entries_data;
  int leader_commit;
  MSGPACK_DEFINE(term, leader_id, prev_log_index, prev_log_term, entries_data, leader_commit)
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg) {
  /* Lab3: Your code here */
  std::vector<uint8_t> entries_data;
  const std::vector<Entry<Command>> &entries = arg.entries;
  entries_data.resize(sizeof(Entry<Command>) * entries.size());
  for (uint32_t i = 0; i < entries.size(); ++i) {
    *(Entry<Command> *)(entries_data.data() + i) = entries.at(i);
  }
  return RpcAppendEntriesArgs{arg.term,          arg.leader_id, arg.prev_log_index,
                              arg.prev_log_term, entries_data,  arg.leader_commit};
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg) {
  /* Lab3: Your code here */
  const std::vector<uint8_t> &entries_data = rpc_arg.entries_data;
  std::vector<Entry<Command>> entries;
  uint32_t num = entries_data.size() / sizeof(Entry<Command>);
  for (uint32_t i = 0; i < num; ++i) {
    entries.emplace_back(*(Entry<Command> *)(entries_data.data() + i));
  }
  return AppendEntriesArgs<Command>{rpc_arg.term,          rpc_arg.leader_id, rpc_arg.prev_log_index,
                                    rpc_arg.prev_log_term, entries,           rpc_arg.leader_commit};
}

struct AppendEntriesReply {
  /* Lab3: Your code here */
  int term;
  bool success;
  int conflict_index;
  int conflict_term;
  MSGPACK_DEFINE(term, success, conflict_index, conflict_term)
};

struct InstallSnapshotArgs {
  /* Lab3: Your code here */

  MSGPACK_DEFINE(

  )
};

struct InstallSnapshotReply {
  /* Lab3: Your code here */

  MSGPACK_DEFINE(

  )
};

} /* namespace chfs */