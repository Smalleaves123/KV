#include "kv/raft/raft_log.h"
#include <algorithm>

namespace kv {
namespace raft {

RaftLog::RaftLog(std::shared_ptr<RaftStorage> storage)
    : storage_(std::move(storage)), commit_index_(0), applied_(0) {
  if (storage_) {
    HardState hs = storage_->InitialState();
    const uint64_t snapshot_index = storage_->SnapshotMeta().last_included_index;
    commit_index_ = std::max(hs.commit_index, snapshot_index);
    applied_ = std::max(
        hs.applied_index,
        storage_->FirstIndex() > 0 ? storage_->FirstIndex() - 1 : 0);
    applied_ = std::max(applied_, snapshot_index);
  }
}

uint64_t RaftLog::FirstIndex() const {
  return storage_->FirstIndex();
}

uint64_t RaftLog::LastIndex() const {
  return std::max(storage_->LastIndex(), storage_->SnapshotMeta().last_included_index);
}

uint64_t RaftLog::Term(uint64_t index) const {
  const RaftSnapshotMeta snapshot = storage_->SnapshotMeta();
  if (index == snapshot.last_included_index) {
    return snapshot.last_included_term;
  }
  if (index > LastIndex() ||
      (index < FirstIndex() && index != snapshot.last_included_index)) {
    return 0;
  }
  return storage_->Term(index);
}

RaftSnapshotMeta RaftLog::SnapshotMeta() const {
  return storage_->SnapshotMeta();
}

std::vector<LogEntry> RaftLog::Entries(uint64_t low, uint64_t high) const {
  if (low > high || low < FirstIndex() || high > LastIndex() + 1) {
    return {};
  }
  return storage_->Entries(low, high);
}

bool RaftLog::MatchLog(uint64_t index, uint64_t term) const {
  if (index > LastIndex()) {
    return false;
  }
  uint64_t log_term = Term(index);
  return log_term == term;
}

void RaftLog::Append(const std::vector<LogEntry>& entries) {
  if (entries.empty()) {
    return;
  }

  const RaftSnapshotMeta snapshot = SnapshotMeta();
  uint64_t first_append_index = entries[0].index;
  if (first_append_index <= snapshot.last_included_index) {
    return;
  }
  if (first_append_index <= LastIndex()) {
    // 存在冲突，截断后缀
    storage_->TruncateSuffix(first_append_index - 1);
  }
  storage_->Append(entries);
}

void RaftLog::CommitTo(uint64_t index) {
  if (commit_index_ < index) {
    if (index > LastIndex()) {
      // 不合法的commit_index
      return;
    }
    commit_index_ = index;
  }
}

void RaftLog::AppliedTo(uint64_t index) {
  if (index == 0) {
    return;
  }
  if (applied_ < index && index <= commit_index_) {
    applied_ = index;
  }
}

bool RaftLog::CompactTo(const RaftSnapshotMeta& meta) {
  if (meta.last_included_index == 0 || meta.last_included_term == 0) {
    return false;
  }
  const RaftSnapshotMeta current = SnapshotMeta();
  if (meta.last_included_index <= current.last_included_index) {
    return meta.last_included_index == current.last_included_index &&
           meta.last_included_term == current.last_included_term;
  }
  if (meta.last_included_index > LastIndex() ||
      Term(meta.last_included_index) != meta.last_included_term) {
    return false;
  }

  storage_->SaveSnapshotMeta(meta);
  storage_->TruncatePrefix(meta.last_included_index + 1);
  commit_index_ = std::max(commit_index_, meta.last_included_index);
  applied_ = std::max(applied_, meta.last_included_index);
  return true;
}

bool RaftLog::RestoreSnapshot(const RaftSnapshotMeta& meta) {
  if (meta.last_included_index == 0 || meta.last_included_term == 0) {
    return false;
  }
  const RaftSnapshotMeta current = SnapshotMeta();
  if (meta.last_included_index < current.last_included_index) {
    return false;
  }
  if (meta.last_included_index == current.last_included_index) {
    return meta.last_included_term == current.last_included_term;
  }

  storage_->SaveSnapshotMeta(meta);
  storage_->TruncatePrefix(meta.last_included_index + 1);
  commit_index_ = std::max(commit_index_, meta.last_included_index);
  applied_ = std::max(applied_, meta.last_included_index);
  return true;
}

} // namespace raft
} // namespace kv
