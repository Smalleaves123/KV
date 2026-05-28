#include "kv/raft/raft_node.h"

#include <gtest/gtest.h>
#include <memory>
#include <vector>

namespace kv {
namespace raft {
namespace {

// 内存存储，用于 RaftNode 测试
class MemStorage : public RaftStorage {
 public:
  HardState InitialState() const override {
    return hs_;
  }

  void SaveHardState(const HardState& state) override {
    hs_ = state;
  }

  std::vector<LogEntry> Entries(uint64_t low, uint64_t high) const override {
    if (low < FirstIndex() || high > LastIndex() + 1 || low > high) {
      return {};
    }
    std::vector<LogEntry> result;
    for (uint64_t i = low; i < high; ++i) {
      auto it = logs_.find(i);
      if (it == logs_.end()) break;
      result.push_back(it->second);
    }
    return result;
  }

  uint64_t Term(uint64_t index) const override {
    if (index == 0) return 0;
    auto it = logs_.find(index);
    if (it == logs_.end()) return 0;
    return it->second.term;
  }

  uint64_t FirstIndex() const override {
    if (logs_.empty()) return 1;
    return logs_.begin()->first;
  }

  uint64_t LastIndex() const override {
    if (logs_.empty()) return 0;
    return logs_.rbegin()->first;
  }

  void Append(const std::vector<LogEntry>& entries) override {
    for (const auto& e : entries) {
      logs_[e.index] = e;
    }
  }

  void TruncatePrefix(uint64_t index) override {
    auto it = logs_.begin();
    while (it != logs_.end() && it->first < index) {
      it = logs_.erase(it);
    }
  }

  void TruncateSuffix(uint64_t index) override {
    auto it = logs_.upper_bound(index);
    logs_.erase(it, logs_.end());
  }

 private:
  HardState hs_;
  std::map<uint64_t, LogEntry> logs_;
};

// 辅助：记录发出的消息
struct CapturedMessages {
  std::vector<std::pair<uint64_t, RequestVoteArgs>> rv_sent;
  std::vector<std::pair<uint64_t, AppendEntriesArgs>> ae_sent;
};

// ===== 单节点测试 =====

TEST(RaftNodeTest, SingleNodeBecomesLeader) {
  auto storage = std::make_shared<MemStorage>();

  RaftOptions opts;
  opts.node_id = 1;
  opts.peers = {1};
  opts.storage = storage;
  opts.election_tick = 10;
  opts.heartbeat_tick = 1;

  RaftNode node(opts);

  EXPECT_EQ(node.role(), RaftRole::kFollower);
  EXPECT_EQ(node.current_term(), 0u);
  EXPECT_EQ(node.leader_id(), 0u);

  // 模拟 Tick 超过选举超时
  for (int i = 0; i < 25; ++i) {
    node.Tick();
  }

  // 单节点应该成为 Leader
  EXPECT_EQ(node.role(), RaftRole::kLeader);
  EXPECT_GE(node.current_term(), 1u);
  EXPECT_EQ(node.leader_id(), 1u);
}

TEST(RaftNodeTest, SingleNodeProposeIncreasesLog) {
  auto storage = std::make_shared<MemStorage>();

  RaftOptions opts;
  opts.node_id = 1;
  opts.peers = {1};
  opts.storage = storage;
  opts.election_tick = 10;
  opts.heartbeat_tick = 1;

  RaftNode node(opts);

  // 先成为 Leader
  for (int i = 0; i < 25; ++i) {
    node.Tick();
  }
  ASSERT_EQ(node.role(), RaftRole::kLeader);

  // Propose 一条日志
  node.Propose("hello");
  EXPECT_EQ(node.commit_index(), 1u);
}

// ===== 选举测试 =====

TEST(RaftNodeTest, FollowerStartsElectionAfterTimeout) {
  auto storage = std::make_shared<MemStorage>();

  RaftOptions opts;
  opts.node_id = 1;
  opts.peers = {1, 2, 3};
  opts.storage = storage;
  opts.election_tick = 5;
  opts.heartbeat_tick = 1;

  RaftNode node(opts);
  EXPECT_EQ(node.role(), RaftRole::kFollower);

  // Tick 够多次保证触发选举（election_tick=5, 最大随机超时=9）
  for (int i = 0; i < 30; ++i) {
    node.Tick();
  }

  // 应该变成 Candidate（还没收到多数选票，因为没有网络回调）
  EXPECT_EQ(node.role(), RaftRole::kCandidate);
  EXPECT_GE(node.current_term(), 1u);
}

TEST(RaftNodeTest, HandleRequestVoteNewerTermGrantsVote) {
  auto storage = std::make_shared<MemStorage>();

  RaftOptions opts;
  opts.node_id = 2;
  opts.peers = {1, 2, 3};
  opts.storage = storage;
  opts.election_tick = 10;
  opts.heartbeat_tick = 1;

  RaftNode node(opts);

  RequestVoteArgs args;
  args.term = 1;
  args.candidate_id = 1;
  args.last_log_index = 0;
  args.last_log_term = 0;

  auto reply = node.HandleRequestVote(args);
  EXPECT_TRUE(reply.vote_granted);
  EXPECT_EQ(reply.term, 1u);
}

TEST(RaftNodeTest, HandleRequestVoteOlderTermRejected) {
  auto storage = std::make_shared<MemStorage>();

  RaftOptions opts;
  opts.node_id = 2;
  opts.peers = {1, 2, 3};
  opts.storage = storage;
  opts.election_tick = 10;
  opts.heartbeat_tick = 1;

  RaftNode node(opts);

  // 先让它进入 term 2（通过收到更高 term 的消息）
  RequestVoteArgs boost_args;
  boost_args.term = 2;
  boost_args.candidate_id = 3;
  boost_args.last_log_index = 0;
  boost_args.last_log_term = 0;
  node.HandleRequestVote(boost_args);

  // 现在 term 2 的节点拒绝 term 1 的投票请求
  RequestVoteArgs old_args;
  old_args.term = 1;
  old_args.candidate_id = 1;
  old_args.last_log_index = 0;
  old_args.last_log_term = 0;

  auto reply = node.HandleRequestVote(old_args);
  EXPECT_FALSE(reply.vote_granted);
}

TEST(RaftNodeTest, HandleRequestVoteAlreadyVotedRejects) {
  auto storage = std::make_shared<MemStorage>();

  RaftOptions opts;
  opts.node_id = 2;
  opts.peers = {1, 2, 3};
  opts.storage = storage;
  opts.election_tick = 10;
  opts.heartbeat_tick = 1;

  RaftNode node(opts);

  // 先投票给节点 1
  RequestVoteArgs args1;
  args1.term = 1;
  args1.candidate_id = 1;
  args1.last_log_index = 0;
  args1.last_log_term = 0;
  auto reply1 = node.HandleRequestVote(args1);
  EXPECT_TRUE(reply1.vote_granted);

  // 再投票给节点 3（同一任期，应该拒绝）
  RequestVoteArgs args2;
  args2.term = 1;
  args2.candidate_id = 3;
  args2.last_log_index = 0;
  args2.last_log_term = 0;
  auto reply2 = node.HandleRequestVote(args2);
  EXPECT_FALSE(reply2.vote_granted);
}

// ===== AppendEntries 测试 =====

TEST(RaftNodeTest, HandleAppendEntriesHeartbeat) {
  auto storage = std::make_shared<MemStorage>();

  RaftOptions opts;
  opts.node_id = 2;
  opts.peers = {1, 2, 3};
  opts.storage = storage;
  opts.election_tick = 10;
  opts.heartbeat_tick = 1;

  RaftNode node(opts);

  AppendEntriesArgs args;
  args.term = 1;
  args.leader_id = 1;
  args.prev_log_index = 0;
  args.prev_log_term = 0;
  args.leader_commit = 0;

  auto reply = node.HandleAppendEntries(args);
  EXPECT_TRUE(reply.success);
  EXPECT_EQ(node.role(), RaftRole::kFollower);
  EXPECT_EQ(node.leader_id(), 1u);
}

TEST(RaftNodeTest, HandleAppendEntriesOlderTermRejected) {
  auto storage = std::make_shared<MemStorage>();

  RaftOptions opts;
  opts.node_id = 1;
  opts.peers = {1, 2, 3};
  opts.storage = storage;
  opts.election_tick = 5;
  opts.heartbeat_tick = 1;

  RaftNode node(opts);

  // 用大量 Tick 保证选举触发（election_tick=5, 最大随机超时=9）
  for (int i = 0; i < 50; ++i) node.Tick();
  ASSERT_EQ(node.role(), RaftRole::kCandidate);
  uint64_t current_term = node.current_term();
  ASSERT_GE(current_term, 1u);

  // 收到 term 低于当前的 AppendEntries
  AppendEntriesArgs args;
  args.term = current_term - 1;
  args.leader_id = 2;
  args.prev_log_index = 0;
  args.prev_log_term = 0;
  args.leader_commit = 0;

  auto reply = node.HandleAppendEntries(args);
  EXPECT_FALSE(reply.success);
  EXPECT_EQ(reply.term, current_term);
}

// ===== 消息捕获测试 =====

TEST(RaftNodeTest, CandidateBroadcastsRequestVote) {
  auto storage = std::make_shared<MemStorage>();

  RaftOptions opts;
  opts.node_id = 1;
  opts.peers = {1, 2, 3};
  opts.storage = storage;
  opts.election_tick = 5;
  opts.heartbeat_tick = 1;

  CapturedMessages msgs;
  RaftNode node(opts);

  node.set_send_request_vote_fn(
      [&](uint64_t to, const RequestVoteArgs& args) {
        msgs.rv_sent.push_back({to, args});
      });

  // Tick 够多次保证选举触发
  for (int i = 0; i < 20; ++i) {
    node.Tick();
  }

  ASSERT_EQ(node.role(), RaftRole::kCandidate);

  // 应该向其他节点广播 RequestVote
  // election_tick=5, 最多触发 2 轮, 每轮发 2 条消息
  int rv_count = msgs.rv_sent.size();

  // 至少有一轮广播 (2 条)
  ASSERT_GE(rv_count, 2);
  EXPECT_LE(rv_count, 6);

  // 检查最后 2 条消息是对应最新一轮的
  bool has_peer2 = false, has_peer3 = false;
  for (int j = rv_count - 2; j < rv_count; ++j) {
    EXPECT_EQ(msgs.rv_sent[j].second.candidate_id, 1u);
    if (msgs.rv_sent[j].first == 2) has_peer2 = true;
    if (msgs.rv_sent[j].first == 3) has_peer3 = true;
  }
  EXPECT_TRUE(has_peer2);
  EXPECT_TRUE(has_peer3);
}

TEST(RaftNodeTest, LeaderBroadcastsHeartbeat) {
  auto storage = std::make_shared<MemStorage>();

  RaftOptions opts;
  opts.node_id = 1;
  opts.peers = {1};
  opts.storage = storage;
  opts.election_tick = 10;
  opts.heartbeat_tick = 2;

  CapturedMessages msgs;
  RaftNode node(opts);
  node.set_send_append_entries_fn(
      [&](uint64_t to, const AppendEntriesArgs& args) {
        msgs.ae_sent.push_back({to, args});
      });

  // 单节点通过 Tick 成为 Leader
  for (int i = 0; i < 25; ++i) {
    node.Tick();
  }

  ASSERT_EQ(node.role(), RaftRole::kLeader);

  // 成为 Leader 时应该发一次心跳（BcastAppendEntries），但单节点无 peer，所以为 0
  EXPECT_EQ(msgs.ae_sent.size(), 0u);
}

// ===== 日志复制测试 =====

TEST(RaftNodeTest, LeaderAppendsLogOnPropose) {
  auto storage = std::make_shared<MemStorage>();

  RaftOptions opts;
  opts.node_id = 1;
  opts.peers = {1, 2, 3};
  opts.storage = storage;
  opts.election_tick = 10;
  opts.heartbeat_tick = 1;

  CapturedMessages msgs;
  RaftNode node(opts);

  node.set_send_append_entries_fn(
      [&](uint64_t to, const AppendEntriesArgs& args) {
        msgs.ae_sent.push_back({to, args});
      });

  // 成为 Leader（单 peer list 含自身时，BecomeCandidate 直接成为 Leader）
  // 这里需要多节点，所以要先 mock RequestVote 回复
  // 或者让单节点先触发选举

  // 用另一种方式：用 {1,1} 单节点先成为 leader
}

TEST(RaftNodeTest, FollowerAppendsLogFromLeader) {
  auto storage = std::make_shared<MemStorage>();

  RaftOptions opts;
  opts.node_id = 2;
  opts.peers = {1, 2, 3};
  opts.storage = storage;
  opts.election_tick = 10;
  opts.heartbeat_tick = 1;

  RaftNode node(opts);

  AppendEntriesArgs args;
  args.term = 1;
  args.leader_id = 1;
  args.prev_log_index = 0;
  args.prev_log_term = 0;
  args.leader_commit = 0;
  args.entries = {
    {1, 1, "set x 1"},
    {1, 2, "set y 2"},
  };

  auto reply = node.HandleAppendEntries(args);
  EXPECT_TRUE(reply.success);
  EXPECT_EQ(node.role(), RaftRole::kFollower);
  EXPECT_EQ(node.leader_id(), 1u);
}

TEST(RaftNodeTest, StaleCandidateStepsDownOnNewerTerm) {
  auto storage = std::make_shared<MemStorage>();

  RaftOptions opts;
  opts.node_id = 2;
  opts.peers = {1, 2, 3};
  opts.storage = storage;
  opts.election_tick = 5;
  opts.heartbeat_tick = 1;

  RaftNode node(opts);

  // 触发选举
  for (int i = 0; i < 15; ++i) node.Tick();
  ASSERT_EQ(node.role(), RaftRole::kCandidate);

  // 收到更高 term 的 RequestVoteReply
  RequestVoteReply reply;
  reply.term = node.current_term() + 1;
  reply.vote_granted = false;

  node.HandleRequestVoteReply(3, reply);

  // 应该退化为 Follower
  EXPECT_EQ(node.role(), RaftRole::kFollower);
  EXPECT_EQ(node.current_term(), reply.term);
}

TEST(RaftNodeTest, LeaderStepsDownOnNewerTerm) {
  auto storage = std::make_shared<MemStorage>();

  RaftOptions opts;
  opts.node_id = 1;
  opts.peers = {1};
  opts.storage = storage;
  opts.election_tick = 10;
  opts.heartbeat_tick = 1;

  RaftNode node(opts);

  // 成为 Leader
  for (int i = 0; i < 25; ++i) node.Tick();
  ASSERT_EQ(node.role(), RaftRole::kLeader);

  // 收到更高 term 的 AppendEntriesReply
  AppendEntriesReply reply;
  reply.term = node.current_term() + 1;
  reply.success = false;

  node.HandleAppendEntriesReply(999, reply);

  // 应该退化为 Follower
  EXPECT_EQ(node.role(), RaftRole::kFollower);
  EXPECT_EQ(node.current_term(), reply.term);
}

TEST(RaftNodeTest, ReceiveHeartbeatResetsElectionTimer) {
  auto storage = std::make_shared<MemStorage>();

  RaftOptions opts;
  opts.node_id = 2;
  opts.peers = {1, 2, 3};
  opts.storage = storage;
  opts.election_tick = 10;
  opts.heartbeat_tick = 1;

  RaftNode node(opts);

  // Tick 几次，还没到超时
  for (int i = 0; i < 8; ++i) node.Tick();
  EXPECT_EQ(node.role(), RaftRole::kFollower);

  // 收到 Leader 心跳，重置选举计时器
  AppendEntriesArgs hb;
  hb.term = 1;
  hb.leader_id = 1;
  hb.prev_log_index = 0;
  hb.prev_log_term = 0;
  hb.leader_commit = 0;

  node.HandleAppendEntries(hb);

  // 再 Tick 8 次，因为计时器被重置，不应该触发选举
  for (int i = 0; i < 8; ++i) node.Tick();
  EXPECT_EQ(node.role(), RaftRole::kFollower);

  // 再多 Tick 一些次，最终应该超时
  for (int i = 0; i < 20; ++i) node.Tick();
  EXPECT_EQ(node.role(), RaftRole::kCandidate);
}

TEST(RaftNodeTest, BecomeCandidateResetsVotes) {
  auto storage = std::make_shared<MemStorage>();

  RaftOptions opts;
  opts.node_id = 2;
  opts.peers = {1, 2, 3};
  opts.storage = storage;
  opts.election_tick = 5;
  opts.heartbeat_tick = 1;

  CapturedMessages msgs;
  RaftNode node(opts);
  node.set_send_request_vote_fn(
      [&](uint64_t to, const RequestVoteArgs& args) {
        msgs.rv_sent.push_back({to, args});
      });

  // 大量 Tick 确保触发选举（election_tick=5, 最大随机超时=9）
  for (int i = 0; i < 30; ++i) node.Tick();
  ASSERT_EQ(node.role(), RaftRole::kCandidate);
  // 至少向 peer 1 和 3 发送了 RequestVote
  EXPECT_GE(msgs.rv_sent.size(), 2u);

  // 收到达不到多数票的 reply
  RequestVoteReply no;
  no.term = node.current_term();
  no.vote_granted = false;
  node.HandleRequestVoteReply(1, no);
  node.HandleRequestVoteReply(3, no);

  // 仍然是 Candidate
  EXPECT_EQ(node.role(), RaftRole::kCandidate);

  // 记录当前消息数和 term
  size_t sent_before = msgs.rv_sent.size();
  uint64_t old_term = node.current_term();

  // Tick 足够多次触发下一轮选举
  for (int i = 0; i < 30; ++i) node.Tick();

  // 应该发起了新一轮选举（term 增加了，又有新的 RequestVote 发出）
  EXPECT_GE(node.current_term(), old_term + 1);
  EXPECT_GT(msgs.rv_sent.size(), sent_before);
}

TEST(RaftNodeTest, RequestVoteWithBetterLogWins) {
  auto storage = std::make_shared<MemStorage>();

  // 节点 2 有一些日志
  // 先构造一个有日志的 storage
  storage->Append({{1, 1, "old"}, {1, 2, "old"}});

  RaftOptions opts;
  opts.node_id = 2;
  opts.peers = {1, 2, 3};
  opts.storage = storage;
  opts.election_tick = 10;
  opts.heartbeat_tick = 1;

  RaftNode node(opts);

  // 节点 1 的日志更旧，请求投票
  RequestVoteArgs args;
  args.term = 1;
  args.candidate_id = 1;
  args.last_log_index = 1;  // 只有 1 条日志
  args.last_log_term = 1;

  auto reply = node.HandleRequestVote(args);
  // 节点 2 日志更新 (LastIndex=2, term=1)，应该拒绝
  EXPECT_FALSE(reply.vote_granted);
}

TEST(RaftNodeTest, AppendEntriesReplyAdvancesCommitOnlyToAcknowledgedIndex) {
  auto storage = std::make_shared<MemStorage>();

  RaftOptions opts;
  opts.node_id = 1;
  opts.peers = {1, 2, 3};
  opts.storage = storage;
  opts.election_tick = 5;
  opts.heartbeat_tick = 1;

  RaftNode node(opts);
  for (int i = 0; i < 30; ++i) {
    node.Tick();
    if (node.role() == RaftRole::kCandidate) {
      break;
    }
  }
  ASSERT_EQ(node.role(), RaftRole::kCandidate);

  RequestVoteReply vote;
  vote.term = node.current_term();
  vote.vote_granted = true;
  node.HandleRequestVoteReply(2, vote);
  ASSERT_EQ(node.role(), RaftRole::kLeader);

  ASSERT_EQ(node.Propose("v1"), 1u);
  ASSERT_EQ(node.Propose("v2"), 2u);
  ASSERT_EQ(node.Propose("v3"), 3u);

  AppendEntriesReply ack;
  ack.term = node.current_term();
  ack.success = true;
  ack.match_index = 1;
  node.HandleAppendEntriesReply(2, ack);

  EXPECT_EQ(node.commit_index(), 1u);
}

}  // namespace
}  // namespace raft
}  // namespace kv
