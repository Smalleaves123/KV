#include "kv/raft/raft_node.h"
#include <random>
#include <iostream>

namespace kv {
namespace raft {

RaftNode::RaftNode(const RaftOptions& options)
    : id_(options.node_id),
      term_(0),
      voted_for_(0),
      leader_id_(0),
      role_(RaftRole::kFollower),
      peers_(options.peers),
      raft_log_(std::make_unique<RaftLog>(options.storage)),
      election_elapsed_(0),
      heartbeat_elapsed_(0),
      randomized_election_timeout_(0),
      election_timeout_(options.election_tick),
      heartbeat_timeout_(options.heartbeat_tick) {

  // 初始化从存储加载硬状态
  if (options.storage) {
    auto hs = options.storage->InitialState();
    term_ = hs.term;
    voted_for_ = hs.vote_for;
  }
  
  ResetRandomizedElectionTimeout();
}

void RaftNode::ResetRandomizedElectionTimeout() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<int> dis(election_timeout_, 2 * election_timeout_ - 1);
  randomized_election_timeout_ = dis(gen);
  election_elapsed_ = 0;
}

void RaftNode::Tick() {
  if (role_ == RaftRole::kLeader) {
    heartbeat_elapsed_++;
    if (heartbeat_elapsed_ >= heartbeat_timeout_) {
      heartbeat_elapsed_ = 0;
      BcastAppendEntries();
    }
  } else {
    election_elapsed_++;
    if (election_elapsed_ >= randomized_election_timeout_) {
      election_elapsed_ = 0;
      BecomeCandidate();
      BcastRequestVote();
    }
  }
}

void RaftNode::BecomeFollower(uint64_t term, uint64_t leader_id) {
  term_ = term;
  leader_id_ = leader_id;
  role_ = RaftRole::kFollower;
  voted_for_ = 0;
  election_elapsed_ = 0;
  ResetRandomizedElectionTimeout();
}

void RaftNode::BecomeCandidate() {
  role_ = RaftRole::kCandidate;
  term_++;
  voted_for_ = id_; // 投自己一票
  leader_id_ = 0;
  votes_received_.clear();
  votes_received_[id_] = true;
  
  ResetRandomizedElectionTimeout();

  if (peers_.size() == 1) {
    BecomeLeader(); // 单节点直接成为Leader
  }
}

void RaftNode::BecomeLeader() {
  role_ = RaftRole::kLeader;
  leader_id_ = id_;
  heartbeat_elapsed_ = 0;

  // 初始化Progress
  progresses_.clear();
  for (auto peer : peers_) {
    if (peer != id_) {
      Progress p;
      p.next = raft_log_->LastIndex() + 1;
      p.match = 0;
      progresses_[peer] = p;
    }
  }

  // 成为leader后立刻发送一次空AppendEntries (心跳)
  BcastAppendEntries();
}

void RaftNode::BcastRequestVote() {
  RequestVoteArgs args;
  args.term = term_;
  args.candidate_id = id_;
  args.last_log_index = raft_log_->LastIndex();
  args.last_log_term = raft_log_->Term(args.last_log_index);

  for (auto peer : peers_) {
    if (peer != id_) {
      if (send_rv_) {
        send_rv_(peer, args);
      }
    }
  }
}

void RaftNode::BcastAppendEntries() {
  for (auto peer : peers_) {
    if (peer != id_) {
      SendAppendEntries(peer);
    }
  }
}

void RaftNode::SendAppendEntries(uint64_t to) {
  AppendEntriesArgs args;
  args.term = term_;
  args.leader_id = id_;
  
  if (progresses_.find(to) == progresses_.end()) return;
  uint64_t next_index = progresses_[to].next;
  
  args.prev_log_index = next_index - 1;
  args.prev_log_term = raft_log_->Term(args.prev_log_index);
  args.leader_commit = raft_log_->commit_index();

  // 如果有新日志需要发，带上entries
  if (raft_log_->LastIndex() >= next_index) {
    args.entries = raft_log_->Entries(next_index, raft_log_->LastIndex());
  }

  if (send_ae_) {
    send_ae_(to, args);
  }
}

uint64_t RaftNode::Propose(const std::string& data) {
  if (role_ != RaftRole::kLeader) {
    // 应该将请求重定向给Leader
    return 0;
  }
  
  LogEntry entry;
  entry.term = term_;
  entry.index = raft_log_->LastIndex() + 1;
  entry.data = data;
  
  std::vector<LogEntry> entries{entry};
  raft_log_->Append(entries);

  if (peers_.size() == 1) {
    raft_log_->CommitTo(entry.index);
  }
  
  BcastAppendEntries();
  return entry.index;
}

RequestVoteReply RaftNode::HandleRequestVote(const RequestVoteArgs& args) {
  RequestVoteReply reply;
  
  if (args.term < term_) {
    reply.term = term_;
    reply.vote_granted = false;
    return reply;
  }

  if (args.term > term_) {
    BecomeFollower(args.term, 0); 
  }

  reply.term = term_;
  
  // 检查日志是不是够新
  uint64_t my_last_index = raft_log_->LastIndex();
  uint64_t my_last_term = raft_log_->Term(my_last_index);

  bool log_ok = (args.last_log_term > my_last_term) ||
                (args.last_log_term == my_last_term && args.last_log_index >= my_last_index);

  if ((voted_for_ == 0 || voted_for_ == args.candidate_id) && log_ok) {
    voted_for_ = args.candidate_id;
    election_elapsed_ = 0; // 重置自身选举超时
    reply.vote_granted = true;
  } else {
    reply.vote_granted = false;
  }

  return reply;
}

AppendEntriesReply RaftNode::HandleAppendEntries(const AppendEntriesArgs& args) {
  AppendEntriesReply reply;
  
  if (args.term < term_) {
    reply.term = term_;
    reply.success = false;
    return reply;
  }

  // 如果收到>=当前任期的AppenEntries，自己肯定是Follower
  BecomeFollower(args.term, args.leader_id);
  reply.term = term_;
  
  // 匹配前一个日志条目
  if (!raft_log_->MatchLog(args.prev_log_index, args.prev_log_term)) {
    reply.success = false;
    reply.match_index = 0;
    return reply;
  }

  // 冲突的后半部分直接截断后追加
  if (!args.entries.empty()) {
     raft_log_->Append(args.entries);
  }

  // 更新commit_index
  if (args.leader_commit > raft_log_->commit_index()) {
    uint64_t last_new_index = args.prev_log_index + args.entries.size();
    raft_log_->CommitTo(std::min(args.leader_commit, last_new_index));
  }

  reply.success = true;
  reply.match_index = args.prev_log_index + args.entries.size();
  return reply;
}

void RaftNode::HandleRequestVoteReply(uint64_t from, const RequestVoteReply& reply) {
  if (role_ != RaftRole::kCandidate) {
    return;
  }

  if (reply.term > term_) {
    BecomeFollower(reply.term, 0);
    return;
  }

  if (reply.vote_granted) {
    votes_received_[from] = true;
    
    size_t votes = 0;
    for (auto const& kv : votes_received_) {
      if (kv.second) votes++;
    }

    if (votes > peers_.size() / 2) {
      BecomeLeader();
    }
  }
}

void RaftNode::HandleAppendEntriesReply(uint64_t from, const AppendEntriesReply& reply) {
  if (role_ != RaftRole::kLeader) {
    return;
  }

  if (reply.term > term_) {
    BecomeFollower(reply.term, 0);
    return;
  }

  if (progresses_.find(from) == progresses_.end()) return;

  if (reply.success) {
    progresses_[from].match =
        std::max(progresses_[from].match, reply.match_index);
    progresses_[from].next = progresses_[from].match + 1;

    // 检查是否可以向前推进commit_index
    // leader 只能提交当前任期的记录
    for (uint64_t n = raft_log_->LastIndex(); n > raft_log_->commit_index(); n--) {
      if (raft_log_->Term(n) == term_) {
        size_t match_count = 1; // 算上自己
        for (auto const& p : progresses_) {
          if (p.second.match >= n) {
            match_count++;
          }
        }
        if (match_count > peers_.size() / 2) {
          raft_log_->CommitTo(n);
          break; // 从最大可能的N跳出
        }
      }
    }
  } else {
    // 失败了，next_index 后退并重发
    if (progresses_[from].next > 1) {
      progresses_[from].next--;
      SendAppendEntries(from);
    }
  }
}

} // namespace raft
} // namespace kv
