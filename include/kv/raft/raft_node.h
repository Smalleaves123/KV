#pragma once

#include <memory>
#include <utility>
#include <unordered_map>
#include <vector>
#include <functional>
#include <cstdint>

#include "kv/raft/raft_state.h"
#include "kv/raft/raft_rpc.h"
#include "kv/raft/raft_log.h"

namespace kv {
namespace raft {

// Follower在发送AppenEntries RPC时的辅助状态信息 (Leader维护)
struct Progress {
  uint64_t match{0}; // 已知该follower最高匹配的日志索引
  uint64_t next{1};  // 下一个要发送给该follower的日志索引
};

// 配置选项
struct RaftOptions {
  uint64_t node_id;         // 本机节点ID
  std::vector<uint64_t> peers; // 集群中所有节点（可以包含自己）

  int election_tick{10};    // 选举超时时的tick次数 (基础值)
  int heartbeat_tick{1};    // 心跳间隔的tick次数

  std::shared_ptr<RaftStorage> storage;
};

// 各种Raft RPC调用的发送回调，隔离网络层
using SendRequestVoteMsgFn = std::function<void(uint64_t to, const RequestVoteArgs&)>;
using SendAppendEntriesMsgFn = std::function<void(uint64_t to, const AppendEntriesArgs&)>;
using SendInstallSnapshotMsgFn =
    std::function<void(uint64_t to, const RaftSnapshotMeta&)>;

class RaftNode {
 public:
  explicit RaftNode(const RaftOptions& options);
  ~RaftNode() = default;

  // 定时器Tick驱动，外部时钟调用
  void Tick();

  // 作为客户端处理RPC请求，生成Reply
  RequestVoteReply HandleRequestVote(const RequestVoteArgs& args);
  AppendEntriesReply HandleAppendEntries(const AppendEntriesArgs& args);
  bool PrepareInstallSnapshot(const InstallSnapshotArgs& args);
  bool RestoreSnapshot(const RaftSnapshotMeta& meta);
  bool CompactSnapshot(const RaftSnapshotMeta& meta) {
    return raft_log_->CompactTo(meta);
  }
  // Apply a committed, single-step membership configuration.
  bool UpdateMembership(const std::vector<uint64_t>& members);
  const std::vector<uint64_t>& Members() const { return peers_; }
  
  // 处理接收到的Reply
  void HandleRequestVoteReply(uint64_t from, const RequestVoteReply& reply);
  void HandleAppendEntriesReply(uint64_t from, const AppendEntriesReply& reply);
  void HandleInstallSnapshotReply(uint64_t from,
                                  const InstallSnapshotReply& reply);

  // 状态查看
  uint64_t node_id() const { return id_; }
  uint64_t current_term() const { return term_; }
  uint64_t voted_for() const { return voted_for_; }
  RaftRole role() const { return role_; }
  uint64_t leader_id() const { return leader_id_; }
  uint64_t commit_index() const { return raft_log_->commit_index(); }
  uint64_t last_log_index() const { return raft_log_->LastIndex(); }
  RaftSnapshotMeta SnapshotMeta() const { return raft_log_->SnapshotMeta(); }
  std::vector<std::pair<uint64_t, Progress>> Progresses() const;
  void AdvanceApplied(uint64_t index) { raft_log_->AppliedTo(index); }

  // 接收上层应用的日志写入请求，返回新日志索引；非 leader 时返回 0。
  uint64_t Propose(const std::string& data);

  // 网络层发包回调设置
  void set_send_request_vote_fn(SendRequestVoteMsgFn fn) { send_rv_ = fn; }
  void set_send_append_entries_fn(SendAppendEntriesMsgFn fn) { send_ae_ = fn; }
  void set_send_install_snapshot_fn(SendInstallSnapshotMsgFn fn) {
    send_snapshot_ = fn;
  }

 private:
  void BecomeFollower(uint64_t term, uint64_t leader_id);
  void BecomeCandidate();
  void BecomeLeader();

  void BcastRequestVote();
  void BcastAppendEntries();
  void SendAppendEntries(uint64_t to);

  void ResetRandomizedElectionTimeout();
  bool IsMember(uint64_t id) const;

  uint64_t id_;
  uint64_t term_;
  uint64_t voted_for_;
  uint64_t leader_id_;
  RaftRole role_;

  std::vector<uint64_t> peers_;
  std::unique_ptr<RaftLog> raft_log_;

  // Leader state
  std::unordered_map<uint64_t, Progress> progresses_;

  // Election tracking
  std::unordered_map<uint64_t, bool> votes_received_;

  // Timers (Tick counts)
  int election_elapsed_;
  int heartbeat_elapsed_;
  int randomized_election_timeout_;
  
  int election_timeout_;
  int heartbeat_timeout_;

  // Callbacks for networking
  SendRequestVoteMsgFn send_rv_;
  SendAppendEntriesMsgFn send_ae_;
  SendInstallSnapshotMsgFn send_snapshot_;
};

} // namespace raft
} // namespace kv
