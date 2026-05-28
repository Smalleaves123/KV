#pragma once

#include <cstdint>
#include <vector>
#include "kv/raft/raft_state.h"

namespace kv {
namespace raft {

struct RequestVoteArgs {
  uint64_t term{0};          // 候选人的任期号
  uint64_t candidate_id{0};  // 请求选票的候选人ID
  uint64_t last_log_index{0}; // 候选人最后日志条目的索引值
  uint64_t last_log_term{0};  // 候选人最后日志条目的任期号
};

struct RequestVoteReply {
  uint64_t term{0};          // 当前任期号，以便候选人去更新自己的任期号
  bool vote_granted{false};  // 候选人赢得了此张选票时为真
};

struct AppendEntriesArgs {
  uint64_t term{0};          // 领导人的任期号
  uint64_t leader_id{0};     // 领导人ID，因此跟随者可以对客户端进行重定向
  uint64_t prev_log_index{0}; // 紧邻新日志条目之前的那个日志条目的索引
  uint64_t prev_log_term{0};  // 紧邻新日志条目之前的那个日志条目的任期
  std::vector<LogEntry> entries; // 准备存储的日志条目（表示心跳时为空）
  uint64_t leader_commit{0}; // 领导人已经提交的日志的索引
};

struct AppendEntriesReply {
  uint64_t term{0};          // 当前任期号，对于领导人去更新自己的任期号
  bool success{false};       // 如果跟随者包含匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
  uint64_t match_index{0};   // follower 已确认复制到的最大日志索引
  // 快速恢复用的额外字段 (可选)
  uint64_t conflict_index{0};
  uint64_t conflict_term{0};
};

} // namespace raft
} // namespace kv
