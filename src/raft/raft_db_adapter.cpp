#include "kv/raft/raft_db_adapter.h"

#include "kv/raft/raft_rpc_codec.h"
#include "kv/raft/raft_server.h"

#include <string>

namespace kv {

RaftDBAdapter::RaftDBAdapter(DB* local_db, RaftServer* raft)
    : local_db_(local_db), raft_(raft) {}

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

} // namespace kv
