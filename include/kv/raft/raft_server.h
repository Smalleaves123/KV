#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "kv/common/status.h"
#include "kv/common/socket_compat.h"
#include "kv/concurrency/thread_pool.h"
#include "kv/engine/write_applier.h"
#include "kv/raft/raft_node.h"

namespace kv {

// Raft cluster configuration for a single node.
struct RaftConfig {
  uint64_t node_id = 1;
  std::string host = "0.0.0.0";
  uint16_t client_port = 9527;   // client-facing port
  uint16_t raft_port = 9528;     // Raft RPC port (client_port + 1)

  // Peer info: node_id -> (host, raft_port)
  struct Peer {
    std::string host;
    uint16_t raft_port;
  };
  std::unordered_map<uint64_t, Peer> peers;

  // Data directory for Raft persistent state
  std::string data_dir = "data/raft";
};

// RaftServer wraps RaftNode with network RPC, Tick loop, and DB integration.
//
// Flow:
//   1. Client writes → net::Server → RaftServer::Propose(cmd) → RaftNode
//   2. RaftNode replicates to followers via AppendEntries RPC
//   3. On commit, RaftServer applies the command to the local DB
//   4. Client reads go directly to DB (no consensus needed)
class RaftServer {
 public:
  RaftServer(const RaftConfig& config, WriteApplier* applier);
  ~RaftServer();

  RaftServer(const RaftServer&) = delete;
  RaftServer& operator=(const RaftServer&) = delete;

  Status Start();
  Status Stop();

  // Propose a write command through Raft. Only valid on the leader.
  // cmd format: "S" + key (or "D" + key for delete)
  Status Propose(const std::string& cmd);
  Status LinearizableReadBarrier();

  bool IsLeader() const noexcept;
  uint64_t LeaderId() const noexcept;
  uint64_t NodeId() const noexcept;

 private:
  // Tick loop: drives RaftNode::Tick() periodically
  void TickLoop();

  // RPC listener: accepts connections from other Raft nodes
  void RpcListenLoop();
  void HandleRpcConnection(platform::SocketHandle fd);

  // Apply committed entries to the DB
  void ApplyCommitted();
  void PersistHardState();

  // Send RPCs to peers (called by RaftNode callbacks)
  void SendRequestVote(uint64_t to, const raft::RequestVoteArgs& args);
  void SendAppendEntries(uint64_t to, const raft::AppendEntriesArgs& args);

  // Serialization helpers for RPC messages
  static std::string EncodeMessage(uint8_t type, const std::string& body);
  static std::string EncodeRequestVote(const raft::RequestVoteArgs& args);
  static std::string EncodeRequestVoteReply(const raft::RequestVoteReply& reply);
  static std::string EncodeAppendEntries(const raft::AppendEntriesArgs& args);
  static std::string EncodeAppendEntriesReply(const raft::AppendEntriesReply& reply);

  // Connect to a peer and send a message
  void SendToPeer(const std::string& host, uint16_t port,
                  const std::string& msg);

  RaftConfig config_;
  WriteApplier* applier_;

  // Raft consensus
  std::shared_ptr<raft::RaftStorage> storage_;
  std::unique_ptr<raft::RaftNode> raft_node_;
  mutable std::mutex raft_mu_;

  // Threads
  std::thread tick_thread_;
  std::thread rpc_thread_;
  std::atomic<bool> running_;
  std::unique_ptr<ThreadPool> rpc_pool_;

  // RPC listener
  platform::SocketHandle rpc_fd_;

  // Peer connections cache
  mutable std::mutex conn_mu_;
  std::unordered_map<uint64_t, platform::SocketHandle> peer_fds_;

  mutable std::mutex hard_state_mu_;
  mutable std::mutex apply_mu_;
  std::condition_variable apply_cv_;
  std::unordered_map<uint64_t, Status> applied_results_;

  platform::SocketRuntime socket_runtime_;

  // Last applied index
  uint64_t last_applied_;
};

}  // namespace kv
