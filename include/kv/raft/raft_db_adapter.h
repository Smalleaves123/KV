#pragma once

#include "kv/engine/db.h"

#include <memory>

namespace kv {

class RaftServer;

// DB adapter used by the network server when Raft is enabled. Reads are
// served from the local DB after a linearizable barrier; writes are proposed
// to Raft and applied by RaftServer after commit.
class RaftDBAdapter final : public DB {
public:
  RaftDBAdapter(DB* local_db, RaftServer* raft);

  bool IsOpen() const noexcept override;

  Status Put(const WriteOptions& options, const Slice& key,
             const Slice& value) override;
  Status Get(const ReadOptions& options, const Slice& key, std::string* value) override;
  Status Delete(const WriteOptions& options, const Slice& key) override;
  Status Expire(const WriteOptions& options, const Slice& key,
                int64_t ttl_seconds) override;
  Status TTL(const ReadOptions& options, const Slice& key,
             int64_t* ttl_seconds) override;
  Status Persist(const WriteOptions& options, const Slice& key) override;
  Status Write(const WriteOptions& options, const WriteBatch& batch) override;
  Status BeginTransaction(const TxnOptions& options,
                          std::unique_ptr<Transaction>* txn) override;
  Status Compact() override;
  Status CreateCheckpoint(const std::string& checkpoint_dir) override;
  Status InstallCheckpoint(const std::string& checkpoint_dir);
  Status GetCacheStats(CacheStats* stats) const override;
  Status GetReadPathStats(ReadPathStats* stats) const override;
  Status GetCompactionStats(CompactionStats* stats) const override;
  Status GetFlushStats(FlushStats* stats) const override;
  const Snapshot* GetSnapshot() override;
  Status ReleaseSnapshot(const Snapshot* snapshot) override;
  Status Close() override;
  std::unique_ptr<Iterator> NewIterator(const ReadOptions& options) override;
  Status Scan(const ReadOptions& options,
              const Slice& start_key,
              const Slice& end_key,
              size_t limit,
              std::vector<std::pair<std::string, std::string>>* out) override;

private:
  Status NotSupportedInRaft(const char* operation) const;
  Status NotLeader() const;

  DB* local_db_;
  RaftServer* raft_;
};

} // namespace kv
