#pragma once

#include <memory>
#include <vector>
#include "kv/raft/raft_state.h"
#include "kv/raft/raft_storage.h"

namespace kv {
namespace raft {

// RaftLog 用于管理Raft节点的日志，包括内存日志和持久化日志的交互
class RaftLog {
 public:
  explicit RaftLog(std::shared_ptr<RaftStorage> storage);
  ~RaftLog() = default;

  // 日志访问
  uint64_t FirstIndex() const;
  uint64_t LastIndex() const;
  uint64_t Term(uint64_t index) const;
  RaftSnapshotMeta SnapshotMeta() const;
  std::vector<LogEntry> Entries(uint64_t low, uint64_t high) const;

  // 日志匹配
  bool MatchLog(uint64_t index, uint64_t term) const;
  
  // 变更状态
  Status Append(const std::vector<LogEntry>& entries);
  void CommitTo(uint64_t index);
  void AppliedTo(uint64_t index);
  Status CompactTo(const RaftSnapshotMeta& meta);
  Status RestoreSnapshot(const RaftSnapshotMeta& meta);

  uint64_t commit_index() const { return commit_index_; }
  uint64_t applied() const { return applied_; }
  std::shared_ptr<RaftStorage> storage() const { return storage_; }

 private:
  std::shared_ptr<RaftStorage> storage_;
  uint64_t commit_index_;
  uint64_t applied_;
  
  // 可以增加一层内存中的unstable日志缓冲，目前为了简化，直接依赖storage
};

} // namespace raft
} // namespace kv
