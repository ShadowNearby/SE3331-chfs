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
  std::vector<Entry<Command>> entries;
};

struct RpcAppendEntriesArgs {
  /* Lab3: Your code here */
  std::vector<uint8_t> data;
  MSGPACK_DEFINE(data)
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg) {
  /* Lab3: Your code here */
  std::vector<u8> buffer(sizeof(arg));
  *(AppendEntriesArgs<Command> *)buffer.data() = arg;
  return RpcAppendEntriesArgs{buffer};
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg) {
  /* Lab3: Your code here */
  AppendEntriesArgs<Command> args = *(AppendEntriesArgs<Command> *)(rpc_arg.data.data());
  return args;
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