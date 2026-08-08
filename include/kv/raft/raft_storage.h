#pragma once

#include <cstdint>
#include <vector>

#include "kv/common/status.h"
#include "kv/raft/raft_state.h"

namespace kv {
namespace raft {

// RaftStorage 接口：定义了Raft需要的持久化存储接口
class RaftStorage {
 public:
  virtual ~RaftStorage() = default;

  // 状态持久化
  virtual Status InitialStatus() const { return Status::OK(); }
  virtual HardState InitialState() const = 0;
  virtual Status SaveHardState(const HardState& state) = 0;
  virtual RaftSnapshotMeta SnapshotMeta() const { return {}; }
  virtual Status SaveSnapshotMeta(const RaftSnapshotMeta&) {
    return Status::OK();
  }

  // The committed membership is persisted separately from the log so a
  // restarted node uses the same quorum as before the restart.  Older/custom
  // storage implementations can opt in later; an empty result means that the
  // caller should use the configured bootstrap membership.
  virtual std::vector<uint64_t> InitialMembers() const { return {}; }
  virtual Status SaveMembers(const std::vector<uint64_t>&) {
    return Status::OK();
  }

  // 日志持久化
  virtual std::vector<LogEntry> Entries(uint64_t low, uint64_t high) const = 0;
  virtual uint64_t Term(uint64_t index) const = 0;
  virtual uint64_t FirstIndex() const = 0;
  virtual uint64_t LastIndex() const = 0;
  
  // 增加新日志
  virtual Status Append(const std::vector<LogEntry>& entries) = 0;
  
  // 用于日志压缩
  virtual Status TruncatePrefix(uint64_t index) = 0;
  virtual Status TruncateSuffix(uint64_t index) = 0;
};

} // namespace raft
} // namespace kv
