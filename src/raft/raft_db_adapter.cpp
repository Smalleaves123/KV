#include "kv/raft/raft_db_adapter.h"

#include "kv/raft/raft_rpc_codec.h"
#include "kv/raft/raft_server.h"
#include "kv/common/time.h"

#include <limits>
#include <string>

namespace kv {

RaftDBAdapter::RaftDBAdapter(DB* local_db, RaftServer* raft)
    : local_db_(local_db), raft_(raft) {}

bool RaftDBAdapter::IsOpen() const noexcept {
  return local_db_ != nullptr && local_db_->IsOpen();
}

Status RaftDBAdapter::NotLeader() const {
  if (raft_ == nullptr) {
    return Status::IOError("raft server is null");
  }
  return Status::AlreadyExists("not the leader, current leader is " +
                               std::to_string(raft_->LeaderId()));
}

Status RaftDBAdapter::NotSupportedInRaft(const char* operation) const {
  return Status::InvalidArgument(std::string(operation) +
                                 " is not supported when raft is enabled");
}

Status RaftDBAdapter::Put(const WriteOptions&, const Slice& key, const Slice& value) {
  if (!raft_->IsLeader()) {
    return NotLeader();
  }
  return raft_->Propose(raft::EncodePutCmd(key.ToString(), value.ToString()));
}

Status RaftDBAdapter::Get(const ReadOptions& options, const Slice& key,
                          std::string* value) {
  if (!raft_->IsLeader()) {
    return NotLeader();
  }
  Status s = raft_->LinearizableReadBarrier();
  if (!s.ok()) {
    return s;
  }
  return local_db_->Get(options, key, value);
}

Status RaftDBAdapter::Delete(const WriteOptions&, const Slice& key) {
  if (!raft_->IsLeader()) {
    return NotLeader();
  }
  return raft_->Propose(raft::EncodeDelCmd(key.ToString()));
}

Status RaftDBAdapter::Expire(const WriteOptions&, const Slice& key,
                             int64_t ttl_seconds) {
  if (!raft_->IsLeader()) return NotLeader();
  Status s = raft_->LinearizableReadBarrier();
  if (!s.ok()) return s;

  std::string value;
  s = local_db_->Get(ReadOptions{}, key, &value);
  if (!s.ok()) return s;
  if (ttl_seconds <= 0) {
    return raft_->Propose(raft::EncodeDelCmd(key.ToString()));
  }

  const uint64_t now_ms = NowUnixMillis();
  if (static_cast<uint64_t>(ttl_seconds) >
      std::numeric_limits<uint64_t>::max() / 1000ULL) {
    return Status::InvalidArgument("ttl is too large");
  }
  const uint64_t ttl_ms = static_cast<uint64_t>(ttl_seconds) * 1000ULL;
  if (ttl_ms > std::numeric_limits<uint64_t>::max() - now_ms) {
    return Status::InvalidArgument("ttl is too large");
  }
  return raft_->Propose(
      raft::EncodeExpireCmd(key.ToString(), now_ms + ttl_ms));
}

Status RaftDBAdapter::TTL(const ReadOptions& options, const Slice& key,
                          int64_t* ttl_seconds) {
  if (!raft_->IsLeader()) return NotLeader();
  Status s = raft_->LinearizableReadBarrier();
  if (!s.ok()) return s;
  return local_db_->TTL(options, key, ttl_seconds);
}

Status RaftDBAdapter::Persist(const WriteOptions&, const Slice& key) {
  if (!raft_->IsLeader()) return NotLeader();
  Status s = raft_->LinearizableReadBarrier();
  if (!s.ok()) return s;

  std::string value;
  s = local_db_->Get(ReadOptions{}, key, &value);
  if (!s.ok()) return s;
  int64_t ttl_seconds = -2;
  s = local_db_->TTL(ReadOptions{}, key, &ttl_seconds);
  if (!s.ok()) return s;
  if (ttl_seconds == -1) return Status::OK();
  return raft_->Propose(raft::EncodeExpireCmd(key.ToString(), 0));
}

Status RaftDBAdapter::Write(const WriteOptions&, const WriteBatch&) {
  return NotSupportedInRaft("write batch");
}

Status RaftDBAdapter::BeginTransaction(const TxnOptions&,
                                       std::unique_ptr<Transaction>* txn) {
  if (txn != nullptr) {
    txn->reset();
  }
  return NotSupportedInRaft("transactions");
}

Status RaftDBAdapter::Compact() { return local_db_->Compact(); }

Status RaftDBAdapter::GetCacheStats(CacheStats* stats) const {
  return local_db_->GetCacheStats(stats);
}

Status RaftDBAdapter::GetReadPathStats(ReadPathStats* stats) const {
  return local_db_->GetReadPathStats(stats);
}

Status RaftDBAdapter::GetCompactionStats(CompactionStats* stats) const {
  return local_db_->GetCompactionStats(stats);
}

const Snapshot* RaftDBAdapter::GetSnapshot() { return local_db_->GetSnapshot(); }

Status RaftDBAdapter::ReleaseSnapshot(const Snapshot* snapshot) {
  return local_db_->ReleaseSnapshot(snapshot);
}

Status RaftDBAdapter::Close() { return local_db_->Close(); }

std::unique_ptr<Iterator> RaftDBAdapter::NewIterator(const ReadOptions& options) {
  return local_db_->NewIterator(options);
}

Status RaftDBAdapter::Scan(
    const ReadOptions& options,
    const Slice& start_key,
    const Slice& end_key,
    size_t limit,
    std::vector<std::pair<std::string, std::string>>* out) {
  return local_db_->Scan(options, start_key, end_key, limit, out);
}

} // namespace kv
