#include "kv/raft/raft_log.h"
#include <algorithm>

namespace kv {
namespace raft {

RaftLog::RaftLog(std::shared_ptr<RaftStorage> storage)
    : storage_(std::move(storage)), commit_index_(0), applied_(0) {
  if (storage_) {
    HardState hs = storage_->InitialState();
    commit_index_ = hs.commit_index;
    applied_ = storage_->FirstIndex() > 0 ? storage_->FirstIndex() - 1 : 0;
  }
}

uint64_t RaftLog::FirstIndex() const {
  return storage_->FirstIndex();
}

uint64_t RaftLog::LastIndex() const {
  return storage_->LastIndex();
}

uint64_t RaftLog::Term(uint64_t index) const {
  if (index > LastIndex() || index < FirstIndex() - 1) {
    return 0;
  }
  return storage_->Term(index);
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
  
  uint64_t first_append_index = entries[0].index;
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

} // namespace raft
} // namespace kv
