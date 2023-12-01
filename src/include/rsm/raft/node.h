#pragma once

#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdarg>
#include <ctime>
#include <filesystem>
#include <memory>
#include <mutex>
#include <thread>

#include "block/manager.h"
#include "common/util.h"
#include "fmt/core.h"
#include "librpc/client.h"
#include "librpc/server.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "rsm/state_machine.h"
#include "utils/thread_pool.h"
namespace chfs {

enum class RaftRole { Follower, Candidate, Leader };
struct RaftNodeConfig {
  int node_id;
  uint16_t port;
  std::string ip_address;
};

template <typename StateMachine, typename Command>
class RaftNode {
#define RAFT_LOG(fmt, args...)                                                                                     \
  do {                                                                                                             \
    auto now =                                                                                                     \
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()) \
            .count();                                                                                              \
    char buf[512];                                                                                                 \
    snprintf(buf, 512, "[%lld][%s:%d][node %d term %d role %d] " fmt "\n", now % 100000, __FILE_NAME__, __LINE__,  \
             my_id, current_term, role, ##args);                                                                   \
    debug_thread_pool->enqueue([=]() { std::cerr << buf; });                                                       \
  } while (0);

 public:
  RaftNode(int node_id, std::vector<RaftNodeConfig> node_configs);
  ~RaftNode();

  /* interfaces for test */
  void set_network(std::map<int, bool> &network_availability);
  void set_reliable(bool flag);
  int get_list_state_log_num();
  int rpc_count();
  std::vector<u8> get_snapshot_direct();

 private:
  /*
   * Start the raft node.
   * Please make sure all of the rpc request handlers have been registered before this method.
   */
  auto start() -> int;

  /*
   * Stop the raft node.
   */
  auto stop() -> int;

  /* Returns whether this node is the leader, you should also return the current term. */
  auto is_leader() -> std::tuple<bool, int>;

  /* Checks whether the node is stopped */
  auto is_stopped() -> bool;

  /*
   * Send a new command to the raft nodes.
   * The returned tuple of the method contains three values:
   * 1. bool:  True if this raft node is the leader that successfully appends the log,
   *      false If this node is not the leader.
   * 2. int: Current term.
   * 3. int: Log index.
   */
  auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

  /* Save a snapshot of the state machine and compact the log. */
  auto save_snapshot() -> bool;

  /* Get a snapshot of the state machine */
  auto get_snapshot() -> std::vector<u8>;

  /* Internal RPC handlers */
  auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
  auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
  auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

  /* RPC helpers */
  void send_request_vote(int target, RequestVoteArgs arg);
  void handle_request_vote_reply(int target, RequestVoteArgs arg, RequestVoteReply reply);

  void send_append_entries(int target, AppendEntriesArgs<Command> arg);
  void handle_append_entries_reply(int target, AppendEntriesArgs<Command> arg, AppendEntriesReply reply);

  void send_install_snapshot(int target, InstallSnapshotArgs arg);
  void handle_install_snapshot_reply(int target, InstallSnapshotArgs arg, InstallSnapshotReply reply);

  /* background workers */
  void run_background_ping();
  void run_background_election();
  void run_background_commit();
  void run_background_apply();

  /* Data structures */
  bool network_stat; /* for test */

  std::mutex mtx;         /* A big lock to protect the whole data structure. */
  std::mutex clients_mtx; /* A lock to protect RpcClient pointers */
  std::unique_ptr<ThreadPool> thread_pool;
  std::unique_ptr<RaftLog<Command>> log_storage; /* To persist the raft log. */
  std::unique_ptr<StateMachine> state;           /*  The state machine that applies the raft log, e.g. a kv store. */

  std::unique_ptr<RpcServer> rpc_server;                     /* RPC server to recieve and handle the RPC requests. */
  std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map; /* RPC clients of all raft nodes including this node. */
  std::vector<RaftNodeConfig> node_configs;                  /* Configuration for all nodes */
  int my_id;                                                 /* The index of this node in rpc_clients, start from 0. */

  std::atomic_bool stopped;

  RaftRole role;
  int current_term;
  int leader_id;

  std::unique_ptr<std::thread> background_election;
  std::unique_ptr<std::thread> background_ping;
  std::unique_ptr<std::thread> background_commit;
  std::unique_ptr<std::thread> background_apply;

  /* Lab3: Your code here */
  int commit_index;
  int vote_for;
  std::atomic<int> granted_vote;
  std::map<int, int> next_index;
  std::map<int, int> match_index;
  RandomNumberGenerator generator;
  std::atomic<bool> receive_heartbeat;
  std::vector<int> peer;
  std::unique_ptr<ThreadPool> debug_thread_pool;

  void become_leader();
  void become_follower(int term, int id_leader);
  void become_candidate();
  auto alive_node() -> int;
  void mssleep(int min, int max, int base);
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs)
    : network_stat(true),
      node_configs(configs),
      my_id(node_id),
      stopped(true),
      role(RaftRole::Follower),
      current_term(0),
      leader_id(-1),
      commit_index(0),
      vote_for(-1),
      granted_vote(0),
      generator(),
      receive_heartbeat(false) {
  auto my_config = node_configs[my_id];

  /* launch RPC server */
  rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

  /* Register the RPCs. */
  rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
  rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
  rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
  rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
  rpc_server->bind(RAFT_RPC_NEW_COMMEND,
                   [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
  rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
  rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

  rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
  rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
  rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

  /* Lab3: Your code here */
  thread_pool = std::make_unique<ThreadPool>(16);
  debug_thread_pool = std::make_unique<ThreadPool>(16);
  auto log_filename = fmt::format("/tmp/raft_log/{}.log", my_id);
  auto bm = std::make_shared<BlockManager>(log_filename);
  log_storage = std::make_unique<RaftLog<Command>>(bm);
  for (const auto &node : node_configs) {
    if (node.node_id == my_id) {
      continue;
    }
    peer.emplace_back(node.node_id);
  }
  for (const auto &node : node_configs) {
    rpc_clients_map[node.node_id] = std::make_unique<RpcClient>(node.ip_address, node.port, true);
  }
  log_storage->Append({0, 0});
  for (const auto &node : peer) {
    next_index[node] = log_storage->Size();
    match_index[node] = 0;
  }
  rpc_server->run(true, configs.size());
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode() {
  stop();

  thread_pool.reset();
  rpc_server.reset();
  state.reset();
  log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int {
  /* Lab3: Your code here */

  stopped = false;
  become_follower(current_term, -1);
  background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
  background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
  background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
  background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);
  RAFT_LOG("start")
  return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int {
  /* Lab3: Your code here */
  stopped = true;
  background_election->join();
  background_ping->join();
  background_commit->join();
  background_apply->join();
  return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int> {
  /* Lab3: Your code here */
  return std::make_tuple(role == RaftRole::Leader, current_term);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool {
  return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size)
    -> std::tuple<bool, int, int> {
  /* Lab3: Your code here */
  return std::make_tuple(false, -1, -1);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool {
  /* Lab3: Your code here */
  return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8> {
  /* Lab3: Your code here */
  return std::vector<u8>{};
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply {
  /* Lab3: Your code here */

  auto term = args.term;
  if (term < current_term) {
    return {current_term, false};
  }
  if (term > current_term) {
    become_follower(term, -1);
  }
  auto last_log_index = log_storage->Size() - 1;
  auto last_log_term = log_storage->At(last_log_index).term;
  if (vote_for != -1) {
    return {current_term, false};
  }
  if (last_log_term > args.last_log_term ||
      (last_log_term == args.last_log_term && last_log_index > args.last_log_index)) {
    return {current_term, false};
  }
  vote_for = args.candidate_id;
  //  RAFT_LOG("vote for %d", vote_for)
  return {current_term, true};
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg,
                                                                const RequestVoteReply reply) {
  /* Lab3: Your code here */
  if (reply.term > current_term) {
    become_follower(reply.term, -1);
    return;
  }
  if (current_term != arg.term || role != RaftRole::Candidate) {
    return;
  }
  if (reply.vote_granted) {
    granted_vote++;
    auto alive = alive_node();
    auto half_node = alive / 2 + 1;
    RAFT_LOG("grant:%d half:%d all:%d", (int)granted_vote, half_node, alive)
    if (granted_vote >= half_node && alive >= 3) {
      become_leader();
    }
  }
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply {
  /* Lab3: Your code here */
  auto arg = transform_rpc_append_entries_args<Command>(rpc_arg);
  if (arg.term < current_term) {
    RAFT_LOG("append error arg.term:%d current_term:%d", arg.term, current_term);
    return {current_term, false, 0, 0};
  }
  become_follower(arg.term, arg.leader_id);
  RAFT_LOG("receive heartbeat from %d", arg.leader_id);
  if (arg.prev_log_index != 0 && !(arg.prev_log_index <= log_storage->Size() - 1 &&
                                   log_storage->At(arg.prev_log_index).term == arg.prev_log_term)) {
    return {current_term, false, 0, 0};
  }
  for (int i = 0; i < arg.entries.size() && i + arg.prev_log_index < log_storage->Size(); ++i) {
    if (arg.entries.at(i).term != log_storage->At(arg.prev_log_index + i).term) {
      log_storage->EraseAllAfterIndex(arg.prev_log_index + i);
      for (int j = i; j < arg.entries.size(); ++j) {
        log_storage->Append(arg.entries.at(j));
      }
      if (arg.leader_commit > commit_index) {
        commit_index = std::min({arg.leader_commit, (int)log_storage->Size()});
      }
      return {current_term, true, 0, 0};
    }
  }
  if (arg.prev_log_index + arg.entries.size() > log_storage->Size()) {
    for (int i = log_storage->Size() - arg.prev_log_index; i < arg.entries.size(); ++i) {
      log_storage->Append(arg.entries.at(i));
    }
  }
  if (arg.leader_commit > commit_index) {
    commit_index = std::min(
        {arg.leader_commit, std::max({arg.prev_log_index + (int)arg.entries.size(), (int)log_storage->Size()})});
  }
  return {current_term, true, 0, 0};
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg,
                                                                  const AppendEntriesReply reply) {
  /* Lab3: Your code here */
  if (role != RaftRole::Leader && arg.term != current_term) {
    return;
  }
  if (reply.success) {
    match_index[target] = arg.prev_log_index + arg.entries.size();
    next_index[target] = match_index[target] + 1;
    return;
  }
  if (current_term < arg.term) {
    become_follower(arg.term, arg.leader_id);
  }
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply {
  /* Lab3: Your code here */
  return InstallSnapshotReply{};
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg,
                                                                    const InstallSnapshotReply reply) {
  /* Lab3: Your code here */
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
    return;
  }
  //  RAFT_LOG("begin send request")
  auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_request_vote_reply(target_id, arg, res.unwrap()->template as<RequestVoteReply>());
  } else {
    // RPC fails
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
    return;
  }

  RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
  auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_append_entries_reply(target_id, arg, res.unwrap()->template as<AppendEntriesReply>());
  } else {
    // RPC fails
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  if (rpc_clients_map[target_id] == nullptr ||
      rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
    return;
  }

  auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
  clients_lock.unlock();
  if (res.is_ok()) {
    handle_install_snapshot_reply(target_id, arg, res.unwrap()->template as<InstallSnapshotReply>());
  } else {
    // RPC fails
  }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
  // Periodly check the liveness of the leader.

  // Work for followers and candidates.

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      std::this_thread::sleep_for(std::chrono::milliseconds(generator.rand(0, 200) + 300));
      {
        std::scoped_lock<std::mutex> lock(mtx);
        if (role == RaftRole::Leader) {
          continue;
        }
        if (receive_heartbeat) {
          receive_heartbeat = false;
          continue;
        }
        become_candidate();
        //        if (role == RaftRole::Follower) {
        //
        //        }
      }
      for (const auto &node_id : peer) {
        if (!rpc_clients_map[my_id]) {
          break;
        }
        send_request_vote(node_id, {current_term, my_id, log_storage->Size() - 1, log_storage->Back().term});
      }
    }
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
  // Periodly send logs to the follower.

  // Only work for the leader.

  /* Uncomment following code when you finish */
  // while (true) {
  //     {
  //         if (is_stopped()) {
  //             return;
  //         }
  //         /* Lab3: Your code here */
  //     }
  // }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
  // Periodly apply committed logs the state machine

  // Work for all the nodes.

  /* Uncomment following code when you finish */
  // while (true) {
  //     {
  //         if (is_stopped()) {
  //             return;
  //         }
  //         /* Lab3: Your code here */
  //     }
  // }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
  // Periodly send empty append_entries RPC to the followers.

  // Only work for the leader.

  /* Uncomment following code when you finish */
  while (true) {
    {
      if (is_stopped()) {
        return;
      }
      /* Lab3: Your code here */
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      if (role != RaftRole::Leader) {
        continue;
      }
      for (const auto &node_id : peer) {
        auto next = next_index[node_id];
        if (!rpc_clients_map[my_id]) {
          break;
        }
        RAFT_LOG("send heartbeat to %d", node_id);
        send_append_entries(node_id, {current_term, my_id, next - 1, log_storage->At(next - 1).term, {}, commit_index});
      }
    }
  }
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);

  /* turn off network */
  if (!network_availability[my_id]) {
    for (auto &&client : rpc_clients_map) {
      if (client.second != nullptr) client.second.reset();
    }

    return;
  }

  for (auto node_network : network_availability) {
    int node_id = node_network.first;
    bool node_status = node_network.second;

    if (node_status && rpc_clients_map[node_id] == nullptr) {
      RaftNodeConfig target_config;
      for (auto config : node_configs) {
        if (config.node_id == node_id) target_config = config;
      }

      rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
    }

    if (!node_status && rpc_clients_map[node_id] != nullptr) {
      RAFT_LOG("disable %d", node_id)
      rpc_clients_map[node_id].reset();
    }
  }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag) {
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  for (auto &&client : rpc_clients_map) {
    if (client.second) {
      client.second->set_reliable(flag);
    }
  }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num() {
  /* only applied to ListStateMachine*/
  std::unique_lock<std::mutex> lock(mtx);

  return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count() {
  int sum = 0;
  std::unique_lock<std::mutex> clients_lock(clients_mtx);

  for (auto &&client : rpc_clients_map) {
    if (client.second) {
      sum += client.second->count();
    }
  }

  return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct() {
  if (is_stopped()) {
    return std::vector<u8>{};
  }

  std::unique_lock<std::mutex> lock(mtx);

  return state->snapshot();
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::become_leader() {
  role = RaftRole::Leader;
  leader_id = my_id;
  RAFT_LOG("become leader")
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::become_follower(int term, int id_leader) {
  //  RAFT_LOG("become follower")
  if (role != RaftRole::Leader) {
    receive_heartbeat = true;
  }
  role = RaftRole::Follower;
  current_term = term;
  leader_id = id_leader;
  vote_for = -1;
  granted_vote = 0;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::become_candidate() {
  receive_heartbeat = true;
  role = RaftRole::Candidate;
  current_term++;
  vote_for = my_id;
  granted_vote = 1;
  RAFT_LOG("become candidate")
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::mssleep(int min, int max, int base) {
  auto sleep = (generator.rand(min, max) + base);
  usleep(1000 * sleep);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::alive_node() -> int {
  int sum = 0;
  std::vector<int> alive;
  std::unique_lock<std::mutex> clients_lock(clients_mtx);
  for (auto &&client : rpc_clients_map) {
    if (client.second) {
      alive.emplace_back(client.first);
      sum++;
    }
  }
  std::string s;
  for (const auto &item : alive) {
    s += " " + std::to_string(item);
  }
  RAFT_LOG("sum alive node:%d [%s]", sum, s.c_str())
  return sum;
}

}  // namespace chfs