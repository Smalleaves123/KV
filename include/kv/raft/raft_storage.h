#pragma once

#include <cstdint>
#include <vector>
#include "kv/raft/raft_state.h"

namespace kv {
namespace raft {

// RaftStorage 接口：定义了Raft需要的持久化存储接口
class RaftStorage {
 public:
  virtual ~RaftStorage() = default;

  // 状态持久化
  virtual HardState InitialState() const = 0;
  virtual void SaveHardState(const HardState& state) = 0;
  virtual RaftSnapshotMeta SnapshotMeta() const { return {}; }
  virtual void SaveSnapshotMeta(const RaftSnapshotMeta&) {}

  // 日志持久化
  virtual std::vector<LogEntry> Entries(uint64_t low, uint64_t high) const = 0;
  virtual uint64_t Term(uint64_t index) const = 0;
  virtual uint64_t FirstIndex() const = 0;
  virtual uint64_t LastIndex() const = 0;
  
  // 增加新日志
  virtual void Append(const std::vector<LogEntry>& entries) = 0;
  
  // 用于日志压缩
  virtual void TruncatePrefix(uint64_t index) = 0;
  virtual void TruncateSuffix(uint64_t index) = 0;
};

} // namespace raft
} // namespace kv
