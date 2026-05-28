#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace kv {
namespace raft {

// 节点角色
enum class RaftRole {
  kFollower,
  kCandidate,
  kLeader
};

// 日志条目
struct LogEntry {
  uint64_t term{0};
  uint64_t index{0};
  std::string data; // 实际的命令或数据
};

// 持久化的Raft状态
struct HardState {
  uint64_t term{0};
  uint64_t vote_for{0};
  uint64_t commit_index{0};
  uint64_t applied_index{0};
};

} // namespace raft
} // namespace kv
