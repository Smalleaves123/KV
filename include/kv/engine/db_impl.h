#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "kv/cache/cache.h"
#include "kv/engine/db.h"
#include "kv/engine/snapshot.h"
#include "kv/engine/write_applier.h"
#include "kv/engine/write_batch.h"
#include "kv/memtable/memtable.h"
#include "kv/sstable/table_cache.h"
#include "kv/version/manifest.h"
#include "kv/wal/wal_manager.h"
#include "kv/wal/wal_writer.h"

namespace kv {

class DBImpl final : public DB, public WriteApplier {
 public:
  explicit DBImpl(DBOptions options);
  ~DBImpl() override;

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  Status Init();

  Status Put(const WriteOptions& options,
             const Slice& key,
             const Slice& value) override;

  Status Get(const ReadOptions& options,
             const Slice& key,
             std::string* value) override;

  Status Delete(const WriteOptions& options,
                const Slice& key) override;
  Status Expire(const WriteOptions& options, const Slice& key,
                int64_t ttl_seconds) override;
  Status TTL(const ReadOptions& options, const Slice& key,
             int64_t* ttl_seconds) override;
  Status Persist(const WriteOptions& options, const Slice& key) override;

  Status Write(const WriteOptions& options, const WriteBatch& batch) override;
  Status BeginTransaction(const TxnOptions& options,
                          std::unique_ptr<Transaction>* txn) override;
  Status Compact() override;
  Status GetCacheStats(CacheStats* stats) const override;
  Status GetReadPathStats(ReadPathStats* stats) const override;
  Status GetCompactionStats(CompactionStats* stats) const override;
  Status GetFlushStats(FlushStats* stats) const override;
  const Snapshot* GetSnapshot() override;
  Status ReleaseSnapshot(const Snapshot* snapshot) override;
  Status TxnGetAtSequence(const Slice& key,
                          uint64_t read_seq,
                          std::string* value) const;
  Status TxnValidateKey(const Slice& key) const;
  Status TxnCommitOCC(const TxnOptions& options,
                      uint64_t start_seq,
                      const std::unordered_set<std::string>& read_set,
                      const std::vector<WriteBatch::Operation>& writes);
  void TxnUnregister(Transaction* txn);

  Status Close() override;
  bool IsOpen() const noexcept override;

  std::unique_ptr<Iterator> NewIterator(const ReadOptions& options) override;
  Status Scan(const ReadOptions& options,
              const Slice& start_key,
              const Slice& end_key,
              size_t limit,
              std::vector<std::pair<std::string, std::string>>* out) override;

  Status ApplyPut(const std::string& key,
                  const std::string& value) override;
  Status ApplyDelete(const std::string& key) override;
  Status ApplyPutWithExpiry(const std::string& key, const std::string& value,
                            uint64_t expires_at_ms) override;
  Status ApplyExpireAt(const std::string& key,
                       uint64_t expires_at_ms) override;

  bool is_open() const noexcept;
  uint64_t LatestSequence() const noexcept;
  const std::string& wal_path() const noexcept;

 private:
  Status ApplyPut(uint64_t seq,
                  const WriteOptions& options,
                  const Slice& key,
                  const Slice& value);
  Status ApplyPutWithExpiry(uint64_t seq, const WriteOptions& options,
                            const Slice& key, const Slice& value,
                            uint64_t expires_at_ms);

  Status ApplyDelete(uint64_t seq,
                     const WriteOptions& options,
                     const Slice& key);
  // Applies one already-validated operation without syncing the WAL. Callers
  // that process a batch can sync once after all operations are appended.
  Status ApplyOperationLocked(uint64_t seq,
                              const WriteBatch::Operation& operation);
  Status AppendWALPut(uint64_t seq, const Slice& key, const Slice& value);
  Status AppendWALPutWithTTL(uint64_t seq,
                             const Slice& key,
                             const Slice& value,
                             uint64_t expires_at_ms);
  Status AppendWALDelete(uint64_t seq, const Slice& key);
  Status SyncWAL();
  Status CloseWAL();
  Status RemoveFlushedWAL(uint64_t max_flushed_seq);
  Status MaybeFlushMemTable(std::unique_lock<std::mutex>& lock);
  Status WaitForImmutableMemTables(std::unique_lock<std::mutex>& lock);
  Status WaitForImmutableMemTableSpace(std::unique_lock<std::mutex>& lock);
  void FlushWorker();
  void RotateMemTable();
  Status FlushMemTableToSST(const MemTable& memtable,
                            std::string* out_file);
  void MaybeCompactAfterFlushLocked();
  Status CheckWritable() const;
  Status GetFromMemTableAt(const Slice& key,
                           uint64_t read_seq,
                           std::string* value,
                           uint64_t* expires_at_ms = nullptr,
                           bool* has_visible_version = nullptr) const;
  Status GetFromSSTFilesAt(const Slice& key,
                           uint64_t read_seq,
                           std::string* value,
                           uint64_t* expires_at_ms = nullptr) const;
  Status GetAtSequence(const Slice& key,
                       uint64_t read_seq,
                       std::string* value,
                       uint64_t* expires_at_ms = nullptr) const;
  void InvalidateCacheEntry(const std::string& key) const;
  Status LoadSSTFilesFromManifest(uint64_t* max_seq);
  Status LoadSSTFilesFromDir(uint64_t* max_seq);
  Status RebuildLatestKeySeqIndex();
  Status ValidateSnapshot(const Snapshot* snapshot) const;
  uint64_t ResolveReadSequence(const ReadOptions& options) const;
  Status CommitOCCTransaction(const TxnOptions& options,
                              uint64_t start_seq,
                              const std::unordered_set<std::string>& read_set,
                              const std::vector<WriteBatch::Operation>& writes);
  void RegisterTransaction(Transaction* txn);
  void UnregisterTransaction(Transaction* txn);

  bool ShouldSync(const WriteOptions& options) const noexcept;
  bool ShouldSync(const TxnOptions& options) const noexcept;
  Status ValidateKey(const Slice& key) const;

  static std::string BuildWalPath(const DBOptions& options);
  static std::string BuildWalDirPath(const DBOptions& options);
  static std::string BuildSSTDirPath(const DBOptions& options);
  static std::string BuildManifestPath(const DBOptions& options);
  static std::string BuildSSTFileName(uint64_t file_number);
  static std::string BuildSSTCacheKey(const std::string& sst_file,
                                      const std::string& user_key);
  Status CompactSSTFilesLocked();

  DBOptions options_;
  std::string wal_path_;
  std::string wal_dir_;
  std::string sst_dir_;
  std::string manifest_path_;
  std::vector<std::string> sst_files_;
  Manifest manifest_;
  std::unique_ptr<MemTable> memtable_;
  struct ImmutableMemTable {
    std::unique_ptr<MemTable> table;
    uint64_t min_sequence = 0;
    uint64_t max_sequence = 0;
    size_t approximate_bytes = 0;
    bool flushing = false;
  };
  std::deque<ImmutableMemTable> immutable_mems_;
  WALWriter wal_writer_;
  WALManager wal_manager_;
  bool using_segmented_wal_;
  uint64_t next_seq_;
  uint64_t next_file_number_;
  bool open_;
  mutable std::mutex mu_;
  mutable std::mutex wal_mu_;
  std::condition_variable flush_cv_;
  std::condition_variable flush_done_cv_;
  std::thread flush_thread_;
  bool shutting_down_ = false;
  Status background_error_;
  std::unordered_set<Transaction*> active_transactions_;
  std::unordered_map<std::string, uint64_t> latest_key_seq_;
  std::unordered_set<const Snapshot*> active_snapshots_;
  std::vector<std::unique_ptr<Snapshot>> owned_snapshots_;
  mutable std::unique_ptr<Cache> cache_;
  mutable std::unique_ptr<TableCache> table_cache_;
  mutable ReadPathStats read_path_stats_;
  mutable CompactionStats compaction_stats_;
  FlushStats flush_stats_;

};

}  // namespace kv
