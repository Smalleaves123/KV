#pragma once

#include <cstdint>
#include <fstream>
#include <map>
#include <string>
#include <vector>

#include "kv/raft/raft_storage.h"

namespace kv {
namespace raft {

// File-based RaftStorage implementation.
// Uses two files: <dir>/state (hard state) and <dir>/log (log entries).
class FileRaftStorage : public RaftStorage {
 public:
  // dir_path: directory to store raft state and log files
  explicit FileRaftStorage(const std::string& dir_path);
  ~FileRaftStorage() override = default;

  HardState InitialState() const override;
  void SaveHardState(const HardState& state) override;
  RaftSnapshotMeta SnapshotMeta() const override;
  void SaveSnapshotMeta(const RaftSnapshotMeta& meta) override;
  std::vector<uint64_t> InitialMembers() const override;
  void SaveMembers(const std::vector<uint64_t>& members) override;

  std::vector<LogEntry> Entries(uint64_t low, uint64_t high) const override;
  uint64_t Term(uint64_t index) const override;
  uint64_t FirstIndex() const override;
  uint64_t LastIndex() const override;

  void Append(const std::vector<LogEntry>& entries) override;
  void TruncatePrefix(uint64_t index) override;
  void TruncateSuffix(uint64_t index) override;

 private:
  void LoadIndex();
  void WriteEntry(const LogEntry& entry);
  LogEntry ReadEntryAt(uint64_t offset) const;

  std::string dir_path_;
  std::string state_path_;
  std::string snapshot_meta_path_;
  std::string members_path_;
  std::string log_path_;

  HardState hard_state_;
  RaftSnapshotMeta snapshot_meta_;
  std::vector<uint64_t> members_;
  uint64_t first_index_;
  uint64_t last_index_;
  std::map<uint64_t, uint64_t> index_offset_;  // index -> file offset
};

}  // namespace raft
}  // namespace kv
