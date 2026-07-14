#include "kv/engine/db_impl.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

#include "kv/engine/recovery.h"
#include "kv/common/time.h"
#include "kv/sstable/block.h"
#include "kv/sstable/block_iterator.h"
#include "kv/sstable/table_builder.h"
#include "kv/sstable/table_cache.h"
#include "kv/sstable/table_reader.h"
#include "kv/sstable/value_codec.h"
#include "kv/table/bloom_filter.h"
#include "kv/table/table_index.h"

namespace kv {

// Forward declaration of the merging iterator factory (defined in iterator.cpp).
std::unique_ptr<Iterator> NewMergingIterator(
    std::unique_ptr<MemTable::Iterator> mem_iter,
    std::vector<std::unique_ptr<TableIterator>> sst_iters,
    uint64_t read_seq);

std::unique_ptr<Snapshot> NewSnapshot(uint64_t seq);
std::unique_ptr<Transaction> NewOCCTransaction(DBImpl* db,
                                                TxnOptions options,
                                                uint64_t start_seq);

namespace {

Status RequireOpen(bool is_open) {
  if (!is_open) {
    return Status::IOError("db is not open");
  }
  return Status::OK();
}

struct CompactionEntry {
  uint64_t seq = 0;
  uint8_t type = 0;  // 0 = value, 1 = deletion
  uint64_t expires_at_ms = 0;
  std::string value;
};

}  // namespace

DBImpl::DBImpl(DBOptions options)
    : options_(std::move(options)),
      wal_path_(),
      sst_dir_(),
      manifest_path_(),
      sst_files_(),
      memtable_(),
      wal_writer_(),
      next_seq_(1),
      next_file_number_(1),
      open_(false),
      active_transactions_(),
      latest_key_seq_(),
      active_snapshots_(),
      owned_snapshots_(),
      cache_(),
      table_cache_(std::make_unique<TableCache>(64)) {}

DBImpl::~DBImpl() {
  (void)Close();
}

Status DBImpl::Init() {
  std::lock_guard<std::mutex> lk(mu_);

  if (open_) {
    return Status::AlreadyExists("db is already open");
  }

  wal_path_ = BuildWalPath(options_);
  sst_dir_ = BuildSSTDirPath(options_);
  manifest_path_ = BuildManifestPath(options_);
  if (options_.cache_enabled) {
    cache_ = CreateCache(
        options_.cache_policy, options_.cache_capacity,
        options_.cache_default_ttl_ms);
  } else {
    cache_.reset();
  }

  if (wal_path_.empty()) {
    return Status::InvalidArgument("wal path is empty");
  }
  if (sst_dir_.empty()) {
    return Status::InvalidArgument("sst dir path is empty");
  }
  if (manifest_path_.empty()) {
    return Status::InvalidArgument("manifest path is empty");
  }

  uint64_t max_sst_seq = 0;
  Status s = manifest_.Open(manifest_path_, options_.create_if_missing);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  if (s.ok()) {
    s = LoadSSTFilesFromManifest(&max_sst_seq);
    if (!s.ok()) {
      return s;
    }
  } else {
    s = LoadSSTFilesFromDir(&max_sst_seq);
    if (!s.ok()) {
      return s;
    }
  }

  const std::filesystem::path wal_fs_path(wal_path_);
  std::error_code ec;
  const bool wal_exists = std::filesystem::exists(wal_fs_path, ec);
  if (ec) {
    return Status::IOError("failed to query wal path: " + wal_path_);
  }

  if (!wal_exists && !options_.create_if_missing && sst_files_.empty()) {
    return Status::NotFound("db does not exist: missing wal and sst files");
  }

  uint64_t max_seq = 0;
  if (wal_exists) {
    s = Recovery::ReplayWAL(wal_path_, &memtable_, &max_seq);
    if (!s.ok()) {
      return s;
    }
  }

  s = wal_writer_.Open(wal_path_, true);
  if (!s.ok()) {
    return s;
  }

  const uint64_t global_max_seq = std::max(max_seq, max_sst_seq);
  next_seq_ = global_max_seq + 1;
  s = RebuildLatestKeySeqIndex();
  if (!s.ok()) {
    return s;
  }
  open_ = true;
  return Status::OK();
}

Status DBImpl::Put(const WriteOptions& options,
                   const Slice& key,
                   const Slice& value) {
  std::lock_guard<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) {
    return open_status;
  }

  Status validate_status = ValidateKey(key);
  if (!validate_status.ok()) {
    return validate_status;
  }

  const uint64_t seq = next_seq_;
  Status s = ApplyPut(seq, options, key, value);
  if (!s.ok()) {
    return s;
  }

  ++next_seq_;

  s = MaybeFlushMemTable();
  if (!s.ok()) {
    return s;
  }

  return Status::OK();
}

Status DBImpl::ApplyPut(const std::string& key, const std::string& value) {
  std::lock_guard<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) return open_status;

  const uint64_t seq = next_seq_;
  Status s = ValidateKey(Slice(key));
  if (!s.ok()) return s;
  s = ApplyPut(seq, WriteOptions{}, Slice(key), Slice(value));
  if (!s.ok()) return s;
  ++next_seq_;
  return MaybeFlushMemTable();
}

Status DBImpl::ApplyDelete(const std::string& key) {
  std::lock_guard<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) return open_status;

  const uint64_t seq = next_seq_;
  Status s = ValidateKey(Slice(key));
  if (!s.ok()) return s;
  s = ApplyDelete(seq, WriteOptions{}, Slice(key));
  if (!s.ok()) return s;
  ++next_seq_;
  return MaybeFlushMemTable();
}

Status DBImpl::ApplyPutWithExpiry(const std::string& key,
                                  const std::string& value,
                                  uint64_t expires_at_ms) {
  std::lock_guard<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) return open_status;
  Status s = ValidateKey(Slice(key));
  if (!s.ok()) return s;
  s = ApplyPutWithExpiry(next_seq_, WriteOptions{}, Slice(key), Slice(value),
                         expires_at_ms);
  if (!s.ok()) return s;
  ++next_seq_;
  return MaybeFlushMemTable();
}

Status DBImpl::ApplyExpireAt(const std::string& key, uint64_t expires_at_ms) {
  std::lock_guard<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) return open_status;
  Status s = ValidateKey(Slice(key));
  if (!s.ok()) return s;

  std::string value;
  s = GetAtSequence(Slice(key), std::numeric_limits<uint64_t>::max(), &value);
  if (!s.ok()) return s;
  s = ApplyPutWithExpiry(next_seq_, WriteOptions{}, Slice(key), Slice(value),
                         expires_at_ms);
  if (!s.ok()) return s;
  ++next_seq_;
  return MaybeFlushMemTable();
}

Status DBImpl::Get(const ReadOptions& options,
                   const Slice& key,
                   std::string* value) {
  std::lock_guard<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) {
    return open_status;
  }

  Status validate_status = ValidateKey(key);
  if (!validate_status.ok()) {
    return validate_status;
  }

  if (value == nullptr) {
    return Status::InvalidArgument("value output pointer is null");
  }

  Status snapshot_status = ValidateSnapshot(options.snapshot);
  if (!snapshot_status.ok()) {
    return snapshot_status;
  }

  const uint64_t read_seq = ResolveReadSequence(options);

  uint64_t mem_expires_at_ms = 0;
  bool mem_has_visible_version = false;
  Status s = GetFromMemTableAt(key, read_seq, value, &mem_expires_at_ms,
                               &mem_has_visible_version);
  if (s.ok()) {
    return s;
  }
  if (!s.IsNotFound()) {
    return s;
  }
  if (mem_has_visible_version) {
    return s;
  }

  return GetFromSSTFilesAt(key, read_seq, value);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  std::lock_guard<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) {
    return open_status;
  }

  Status validate_status = ValidateKey(key);
  if (!validate_status.ok()) {
    return validate_status;
  }

  const uint64_t seq = next_seq_;
  Status s = ApplyDelete(seq, options, key);
  if (!s.ok()) {
    return s;
  }

  ++next_seq_;

  s = MaybeFlushMemTable();
  if (!s.ok()) {
    return s;
  }

  return Status::OK();
}

Status DBImpl::Expire(const WriteOptions& options, const Slice& key,
                      int64_t ttl_seconds) {
  std::lock_guard<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) return open_status;
  Status validate_status = ValidateKey(key);
  if (!validate_status.ok()) return validate_status;

  std::string value;
  Status s = GetAtSequence(key, std::numeric_limits<uint64_t>::max(), &value);
  if (!s.ok()) return s;

  const uint64_t seq = next_seq_;
  if (ttl_seconds <= 0) {
    s = ApplyDelete(seq, options, key);
  } else {
    if (static_cast<uint64_t>(ttl_seconds) >
        std::numeric_limits<uint64_t>::max() / 1000ULL) {
      return Status::InvalidArgument("ttl is too large");
    }
    const uint64_t ttl_ms = static_cast<uint64_t>(ttl_seconds) * 1000ULL;
    const uint64_t now_ms = NowUnixMillis();
    if (ttl_ms > std::numeric_limits<uint64_t>::max() - now_ms) {
      return Status::InvalidArgument("ttl is too large");
    }
    s = ApplyPutWithExpiry(seq, options, key, Slice(value), now_ms + ttl_ms);
  }
  if (!s.ok()) return s;

  ++next_seq_;
  return MaybeFlushMemTable();
}

Status DBImpl::TTL(const ReadOptions& options, const Slice& key,
                   int64_t* ttl_seconds) {
  std::lock_guard<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) return open_status;
  if (ttl_seconds == nullptr) {
    return Status::InvalidArgument("ttl output pointer is null");
  }
  Status validate_status = ValidateKey(key);
  if (!validate_status.ok()) return validate_status;
  Status snapshot_status = ValidateSnapshot(options.snapshot);
  if (!snapshot_status.ok()) return snapshot_status;

  std::string value;
  uint64_t expires_at_ms = 0;
  const uint64_t read_seq = ResolveReadSequence(options);
  Status s = GetAtSequence(key, read_seq, &value, &expires_at_ms);
  if (!s.ok()) {
    if (s.IsNotFound()) *ttl_seconds = -2;
    return s.IsNotFound() ? Status::OK() : s;
  }

  if (expires_at_ms == 0) {
    *ttl_seconds = -1;
    return Status::OK();
  }

  const uint64_t now_ms = NowUnixMillis();
  *ttl_seconds = IsExpired(expires_at_ms, now_ms)
                     ? -2
                     : static_cast<int64_t>((expires_at_ms - now_ms) / 1000ULL);
  return Status::OK();
}

Status DBImpl::Persist(const WriteOptions& options, const Slice& key) {
  std::lock_guard<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) return open_status;
  Status validate_status = ValidateKey(key);
  if (!validate_status.ok()) return validate_status;

  std::string value;
  uint64_t expires_at_ms = 0;
  Status s = GetAtSequence(key, std::numeric_limits<uint64_t>::max(), &value,
                           &expires_at_ms);
  if (!s.ok()) return s;
  if (expires_at_ms == 0) return Status::OK();

  s = ApplyPutWithExpiry(next_seq_, options, key, Slice(value), 0);
  if (!s.ok()) return s;
  ++next_seq_;
  return MaybeFlushMemTable();
}

Status DBImpl::Write(const WriteOptions& options, const WriteBatch& batch) {
  std::lock_guard<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) {
    return open_status;
  }

  if (batch.Empty()) {
    return Status::OK();
  }

  for (const auto& op : batch.operations()) {
    Status s = ValidateKey(op.key);
    if (!s.ok()) {
      return s;
    }
  }

  for (const auto& op : batch.operations()) {
    const uint64_t seq = next_seq_;
    Status s = ApplyOperationLocked(seq, op);
    if (!s.ok()) {
      return s;
    }
    ++next_seq_;
  }

  if (ShouldSync(options)) {
    Status s = wal_writer_.Sync();
    if (!s.ok()) {
      return s;
    }
  }

  return MaybeFlushMemTable();
}

Status DBImpl::BeginTransaction(const TxnOptions& options,
                                std::unique_ptr<Transaction>* txn) {
  std::lock_guard<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) {
    return open_status;
  }

  if (txn == nullptr) {
    return Status::InvalidArgument("transaction output pointer is null");
  }

  const uint64_t start_seq = next_seq_ == 0 ? 0 : next_seq_ - 1;
  auto occ = NewOCCTransaction(this, options, start_seq);
  RegisterTransaction(occ.get());
  *txn = std::move(occ);
  return Status::OK();
}

Status DBImpl::Compact() {
  std::lock_guard<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) {
    return open_status;
  }
  if (!active_snapshots_.empty()) {
    return Status::AlreadyExists("cannot compact with active snapshots");
  }

  if (!memtable_.Empty()) {
    std::string sst_file;
    Status s = FlushMemTableToSST(&sst_file);
    if (!s.ok()) {
      return s;
    }
    sst_files_.push_back(std::move(sst_file));
    memtable_.Clear();
  }

  if (sst_files_.size() < options_.compaction_min_input_files) {
    return Status::NotFound("not enough sst files for compaction");
  }

  ++compaction_stats_.trigger_attempts;
  Status s = CompactSSTFilesLocked();
  if (s.ok()) {
    ++compaction_stats_.succeeded;
  } else {
    ++compaction_stats_.failed;
  }
  return s;
}

Status DBImpl::GetCacheStats(CacheStats* stats) const {
  std::lock_guard<std::mutex> lk(mu_);
  if (stats == nullptr) {
    return Status::InvalidArgument("cache stats output is null");
  }
  if (cache_ == nullptr) {
    *stats = CacheStats{};
    return Status::OK();
  }
  *stats = cache_->Stats();
  return Status::OK();
}

Status DBImpl::GetReadPathStats(ReadPathStats* stats) const {
  std::lock_guard<std::mutex> lk(mu_);
  if (stats == nullptr) {
    return Status::InvalidArgument("read path stats output is null");
  }
  *stats = read_path_stats_;
  if (table_cache_ != nullptr) {
    const TableCacheStats table_stats = table_cache_->Stats();
    stats->table_cache_hits = table_stats.hit;
    stats->table_cache_misses = table_stats.miss;
    stats->table_cache_evictions = table_stats.evict;
    stats->table_cache_entries = table_stats.entries;
  }
  return Status::OK();
}

Status DBImpl::GetCompactionStats(CompactionStats* stats) const {
  std::lock_guard<std::mutex> lk(mu_);
  if (stats == nullptr) {
    return Status::InvalidArgument("compaction stats output is null");
  }
  *stats = compaction_stats_;
  return Status::OK();
}

Status DBImpl::TxnGetAtSequence(const Slice& key,
                                uint64_t read_seq,
                                std::string* value) const {
  std::lock_guard<std::mutex> lk(mu_);
  return GetAtSequence(key, read_seq, value);
}

Status DBImpl::TxnValidateKey(const Slice& key) const {
  return ValidateKey(key);
}

Status DBImpl::TxnCommitOCC(const TxnOptions& options,
                            uint64_t start_seq,
                            const std::unordered_set<std::string>& read_set,
                            const std::vector<WriteBatch::Operation>& writes) {
  return CommitOCCTransaction(options, start_seq, read_set, writes);
}

void DBImpl::TxnUnregister(Transaction* txn) {
  UnregisterTransaction(txn);
}

const Snapshot* DBImpl::GetSnapshot() {
  std::lock_guard<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) {
    return nullptr;
  }

  const uint64_t seq = next_seq_ == 0 ? 0 : next_seq_ - 1;
  auto snapshot = NewSnapshot(seq);
  const Snapshot* raw = snapshot.get();
  active_snapshots_.insert(raw);
  owned_snapshots_.push_back(std::move(snapshot));
  return raw;
}

Status DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  std::lock_guard<std::mutex> lk(mu_);

  if (snapshot == nullptr) {
    return Status::InvalidArgument("snapshot is null");
  }

  auto active_it = active_snapshots_.find(snapshot);
  if (active_it == active_snapshots_.end()) {
    return Status::InvalidArgument("snapshot is not active");
  }
  active_snapshots_.erase(active_it);

  for (auto it = owned_snapshots_.begin(); it != owned_snapshots_.end(); ++it) {
    if (it->get() == snapshot) {
      owned_snapshots_.erase(it);
      return Status::OK();
    }
  }

  return Status::InvalidArgument("snapshot ownership mismatch");
}

std::unique_ptr<Iterator> DBImpl::NewIterator(const ReadOptions& options) {
  std::lock_guard<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) {
    return nullptr;
  }

  Status snapshot_status = ValidateSnapshot(options.snapshot);
  if (!snapshot_status.ok()) {
    return nullptr;
  }

  const uint64_t read_seq = ResolveReadSequence(options);

  // MemTable iterator.
  auto mem_iter = std::make_unique<MemTable::Iterator>(memtable_.NewIterator());

  // SST iterators.
  std::vector<std::unique_ptr<TableIterator>> sst_iters;
  sst_iters.reserve(sst_files_.size());
  for (const auto& file : sst_files_) {
    std::shared_ptr<const TableReader> reader;
    Status s = table_cache_->Get(file, &reader);
    if (!s.ok()) {
      return nullptr;
    }
    sst_iters.push_back(std::make_unique<TableIterator>(*reader));
  }

  return NewMergingIterator(std::move(mem_iter), std::move(sst_iters), read_seq);
}

Status DBImpl::Close() {
  std::lock_guard<std::mutex> lk(mu_);

  if (!open_) {
    return Status::OK();
  }

  Status s = wal_writer_.Close();
  if (!s.ok()) {
    return s;
  }

  s = manifest_.Close();
  if (!s.ok()) {
    return s;
  }

  table_cache_->Clear();
  active_transactions_.clear();
  active_snapshots_.clear();
  owned_snapshots_.clear();
  open_ = false;
  return Status::OK();
}

bool DBImpl::is_open() const noexcept {
  std::lock_guard<std::mutex> lk(mu_);
  return open_;
}

bool DBImpl::IsOpen() const noexcept {
  return is_open();
}

uint64_t DBImpl::LatestSequence() const noexcept {
  std::lock_guard<std::mutex> lk(mu_);
  return next_seq_ == 0 ? 0 : next_seq_ - 1;
}

const std::string& DBImpl::wal_path() const noexcept {
  std::lock_guard<std::mutex> lk(mu_);
  return wal_path_;
}

Status DBImpl::ApplyPut(uint64_t seq,
                        const WriteOptions& options,
                        const Slice& key,
                        const Slice& value) {
  WriteBatch::Operation operation;
  operation.type = WriteBatch::ValueType::kPut;
  operation.key = key.ToString();
  operation.value = value.ToString();

  Status s = ApplyOperationLocked(seq, operation);
  if (!s.ok()) {
    return s;
  }
  if (ShouldSync(options)) {
    s = wal_writer_.Sync();
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

Status DBImpl::ApplyPutWithExpiry(uint64_t seq, const WriteOptions& options,
                                  const Slice& key, const Slice& value,
                                  uint64_t expires_at_ms) {
  if (expires_at_ms == 0) {
    return ApplyPut(seq, options, key, value);
  }

  Status s = wal_writer_.AppendPutWithTTL(seq, key, value, expires_at_ms);
  if (!s.ok()) return s;
  s = memtable_.Put(seq, key, value, expires_at_ms);
  if (!s.ok()) return s;
  InvalidateCacheEntry(key.ToString());
  latest_key_seq_[key.ToString()] = seq;
  if (ShouldSync(options)) {
    return wal_writer_.Sync();
  }
  return Status::OK();
}

Status DBImpl::ApplyDelete(uint64_t seq,
                           const WriteOptions& options,
                           const Slice& key) {
  WriteBatch::Operation operation;
  operation.type = WriteBatch::ValueType::kDelete;
  operation.key = key.ToString();

  Status s = ApplyOperationLocked(seq, operation);
  if (!s.ok()) {
    return s;
  }
  if (ShouldSync(options)) {
    s = wal_writer_.Sync();
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

Status DBImpl::ApplyOperationLocked(
    uint64_t seq, const WriteBatch::Operation& operation) {
  Status s;
  if (operation.type == WriteBatch::ValueType::kPut) {
    s = wal_writer_.AppendPut(seq, operation.key, operation.value);
    if (s.ok()) {
      s = memtable_.Put(seq, operation.key, operation.value);
    }
  } else {
    s = wal_writer_.AppendDelete(seq, operation.key);
    if (s.ok()) {
      s = memtable_.Delete(seq, operation.key);
    }
  }
  if (!s.ok()) {
    return s;
  }

  InvalidateCacheEntry(operation.key);
  latest_key_seq_[operation.key] = seq;
  return Status::OK();
}

Status DBImpl::MaybeFlushMemTable() {
  if (options_.memtable_write_buffer_size == 0 || memtable_.Empty()) {
    return Status::OK();
  }

  if (memtable_.ApproximateMemoryUsage() < options_.memtable_write_buffer_size) {
    return Status::OK();
  }

  std::string sst_file;
  Status s = FlushMemTableToSST(&sst_file);
  if (!s.ok()) {
    return s;
  }

  sst_files_.push_back(std::move(sst_file));
  memtable_.Clear();

  if (!options_.auto_compaction_enabled) {
    return Status::OK();
  }

  if (sst_files_.size() < options_.compaction_min_input_files) {
    ++compaction_stats_.skipped_due_threshold;
    return Status::OK();
  }
  if (!active_snapshots_.empty()) {
    ++compaction_stats_.skipped_due_snapshot;
    return Status::OK();
  }

  ++compaction_stats_.trigger_attempts;
  Status compact_status = CompactSSTFilesLocked();
  if (compact_status.ok()) {
    ++compaction_stats_.succeeded;
    return Status::OK();
  }

  ++compaction_stats_.failed;
  return Status::OK();
}

Status DBImpl::FlushMemTableToSST(std::string* out_file) {
  if (out_file == nullptr) {
    return Status::InvalidArgument("out_file is null");
  }

  if (memtable_.Empty()) {
    return Status::NotFound("memtable is empty");
  }

  std::error_code ec;
  std::filesystem::create_directories(sst_dir_, ec);
  if (ec) {
    return Status::IOError("failed to create sst dir: " + sst_dir_);
  }

  const std::string sst_path =
      sst_dir_ + "/" + BuildSSTFileName(next_file_number_);
  const uint64_t file_number = next_file_number_;

  TableBuilder builder(sst_path, options_.memtable_write_buffer_size);

  auto it = memtable_.NewIterator();
  uint64_t max_flushed_seq = 0;

  for (it.SeekToFirst(); it.Valid(); it.Next()) {
    const auto& entry = it.entry();
    max_flushed_seq = std::max(max_flushed_seq, entry.seq);

    uint8_t type = (entry.type == RecordType::kDeletion) ? 1 : 0;
    Status s = builder.Add(entry.key, entry.seq, type, entry.expires_at_ms,
                           entry.value);
    if (!s.ok()) {
      return s;
    }
  }

  Status s = builder.Finish();
  if (!s.ok()) {
    return s;
  }

  if (builder.FileSize() == 0) {
    std::filesystem::remove(sst_path, ec);
    return Status::NotFound("no visible entries to flush");
  }

  if (manifest_.IsOpen()) {
    ManifestFileMeta meta;
    meta.file_number = file_number;
    meta.file_path = sst_path;
    meta.max_seq = max_flushed_seq;
    s = manifest_.AddFile(meta);
    if (!s.ok()) {
      return s;
    }
  }

  *out_file = sst_path;
  ++next_file_number_;
  return Status::OK();
}

Status DBImpl::GetFromMemTableAt(const Slice& key,
                                 uint64_t read_seq,
                                 std::string* value,
                                 uint64_t* expires_at_ms,
                                 bool* has_visible_version) const {
  const std::string target = key.ToString();
  if (has_visible_version != nullptr) *has_visible_version = false;
  auto it = memtable_.NewIterator();
  it.Seek(key);

  while (it.Valid()) {
    const auto& entry = it.entry();
    if (entry.key != target) {
      break;
    }

    if (entry.seq <= read_seq) {
      if (has_visible_version != nullptr) *has_visible_version = true;
      if (entry.type == RecordType::kDeletion) {
        return Status::NotFound("key deleted");
      }
      if (expires_at_ms != nullptr) {
        *expires_at_ms = entry.expires_at_ms;
      }
      if (IsExpired(entry.expires_at_ms, NowUnixMillis())) {
        return Status::NotFound("key expired");
      }
      *value = entry.value;
      return Status::OK();
    }

    it.Next();
  }

  return Status::NotFound("key not found");
}

Status DBImpl::GetFromSSTFilesAt(const Slice& key,
                                 uint64_t read_seq,
                                 std::string* value,
                                 uint64_t* expires_at_ms) const {
  const std::string target = key.ToString();
  const bool allow_cache =
      cache_ != nullptr && read_seq == std::numeric_limits<uint64_t>::max();

  // Check in-memory key-value cache first
  if (allow_cache) {
    if (cache_->Get(target, value)) {
      if (expires_at_ms != nullptr) *expires_at_ms = 0;
      return Status::OK();
    }
  }

  // Walk SST files from newest to oldest so duplicate keys split across blocks
  // still resolve to the newest visible version.
  for (size_t i = sst_files_.size(); i-- > 0;) {
    const std::string& file = sst_files_[i];

    std::shared_ptr<const TableReader> reader;
    Status s = table_cache_->Get(file, &reader);
    if (!s.ok()) {
      return s;
    }

    uint8_t entry_type = 0;
    uint64_t entry_expires_at_ms = 0;
    TableReadStatsDelta stats_delta;
    s = reader->Get(target, read_seq, &entry_type, value,
                    &entry_expires_at_ms, &stats_delta);
    read_path_stats_.bloom_queries += stats_delta.bloom_queries;
    read_path_stats_.bloom_negatives += stats_delta.bloom_negatives;
    if (s.ok()) {
      if (entry_type == 1) {
        // deletion tombstone
        return Status::NotFound("key deleted");
      }
      if (expires_at_ms != nullptr) *expires_at_ms = entry_expires_at_ms;
      if (IsExpired(entry_expires_at_ms, NowUnixMillis())) {
        return Status::NotFound("key expired");
      }
      if (allow_cache) {
        if (entry_expires_at_ms == 0) {
          cache_->Put(target, *value);
        }
      }
      return Status::OK();
    }
    if (s.IsNotFound()) {
      if (entry_type == 1) {
        return s;
      }
      continue;
    }
    return s;
  }

  return Status::NotFound("key not found");
}

Status DBImpl::GetAtSequence(const Slice& key,
                             uint64_t read_seq,
                             std::string* value,
                             uint64_t* expires_at_ms) const {
  Status validate_status = ValidateKey(key);
  if (!validate_status.ok()) {
    return validate_status;
  }
  if (value == nullptr) {
    return Status::InvalidArgument("value output pointer is null");
  }

  uint64_t local_expires_at_ms = 0;
  uint64_t* resolved_expires_at_ms =
      expires_at_ms == nullptr ? &local_expires_at_ms : expires_at_ms;
  *resolved_expires_at_ms = 0;
  bool mem_has_visible_version = false;
  Status s = GetFromMemTableAt(key, read_seq, value, resolved_expires_at_ms,
                               &mem_has_visible_version);
  if (s.ok()) {
    return s;
  }
  if (!s.IsNotFound()) {
    return s;
  }
  if (mem_has_visible_version) {
    return s;
  }
  return GetFromSSTFilesAt(key, read_seq, value, expires_at_ms);
}

Status DBImpl::RebuildLatestKeySeqIndex() {
  latest_key_seq_.clear();

  for (const auto& file : sst_files_) {
    std::shared_ptr<const TableReader> reader;
    Status s = table_cache_->Get(file, &reader);
    if (!s.ok()) {
      // Try to open directly if cache fails
      std::unique_ptr<TableReader> direct_reader;
      s = TableReader::Open(file, &direct_reader);
      if (!s.ok()) {
        return s;
      }
      // Scan the file to build sequence index
      TableIterator iter(*direct_reader);
      for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        const std::string& k = iter.key();
        const uint64_t seq = iter.seq();
        auto it = latest_key_seq_.find(k);
        if (it == latest_key_seq_.end() || seq > it->second) {
          latest_key_seq_[k] = seq;
        }
      }
    } else {
      TableIterator iter(*reader);
      for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        const std::string& k = iter.key();
        const uint64_t seq = iter.seq();
        auto it = latest_key_seq_.find(k);
        if (it == latest_key_seq_.end() || seq > it->second) {
          latest_key_seq_[k] = seq;
        }
      }
    }
  }

  auto it = memtable_.NewIterator();
  for (it.SeekToFirst(); it.Valid(); it.Next()) {
    const auto& entry = it.entry();
    auto found = latest_key_seq_.find(entry.key);
    if (found == latest_key_seq_.end() || entry.seq > found->second) {
      latest_key_seq_[entry.key] = entry.seq;
    }
  }

  return Status::OK();
}

Status DBImpl::CommitOCCTransaction(
    const TxnOptions& options,
    uint64_t start_seq,
    const std::unordered_set<std::string>& read_set,
    const std::vector<WriteBatch::Operation>& writes) {
  std::lock_guard<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) {
    return open_status;
  }

  std::unordered_set<std::string> keys_to_validate = read_set;
  for (const auto& op : writes) {
    keys_to_validate.insert(op.key);
  }

  for (const auto& key : keys_to_validate) {
    auto it = latest_key_seq_.find(key);
    if (it != latest_key_seq_.end() && it->second > start_seq) {
      return Status::AlreadyExists("transaction conflict");
    }
  }

  for (const auto& op : writes) {
    Status s = ValidateKey(op.key);
    if (!s.ok()) {
      return s;
    }
  }

  for (const auto& op : writes) {
    const uint64_t seq = next_seq_;
    Status s = ApplyOperationLocked(seq, op);
    if (!s.ok()) {
      return s;
    }
    ++next_seq_;
  }

  if (ShouldSync(options)) {
    Status s = wal_writer_.Sync();
    if (!s.ok()) {
      return s;
    }
  }

  return MaybeFlushMemTable();
}

void DBImpl::RegisterTransaction(Transaction* txn) {
  if (txn != nullptr) {
    active_transactions_.insert(txn);
  }
}

void DBImpl::UnregisterTransaction(Transaction* txn) {
  if (txn != nullptr) {
    std::lock_guard<std::mutex> lk(mu_);
    active_transactions_.erase(txn);
  }
}

Status DBImpl::LoadSSTFilesFromManifest(uint64_t* max_seq) {
  if (max_seq == nullptr) {
    return Status::InvalidArgument("max_seq output is null");
  }

  *max_seq = 0;
  sst_files_.clear();
  next_file_number_ = 1;

  std::vector<ManifestFileMeta> files;
  Status s = manifest_.Recover(&files);
  if (!s.ok()) {
    return s;
  }

  uint64_t max_file_number = 0;
  for (const auto& f : files) {
    if (f.file_number == 0 || f.file_path.empty()) {
      return Status::Corruption("invalid manifest add-file record");
    }
    sst_files_.push_back(f.file_path);
    max_file_number = std::max(max_file_number, f.file_number);
    *max_seq = std::max(*max_seq, f.max_seq);
  }

  if (max_file_number > 0) {
    next_file_number_ = max_file_number + 1;
  }

  return Status::OK();
}

Status DBImpl::LoadSSTFilesFromDir(uint64_t* max_seq) {
  if (max_seq == nullptr) {
    return Status::InvalidArgument("max_seq output is null");
  }

  *max_seq = 0;
  sst_files_.clear();
  next_file_number_ = 1;

  const std::filesystem::path dir(sst_dir_);
  std::error_code ec;
  if (!std::filesystem::exists(dir, ec)) {
    if (ec) {
      return Status::IOError("failed to query sst dir: " + sst_dir_);
    }
    return Status::OK();
  }

  if (!std::filesystem::is_directory(dir, ec)) {
    if (ec) {
      return Status::IOError("failed to query sst dir type: " + sst_dir_);
    }
    return Status::InvalidArgument("sst_dir is not a directory: " + sst_dir_);
  }

  std::vector<std::pair<uint64_t, std::string>> files;
  for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
    if (ec) {
      return Status::IOError("failed to iterate sst dir: " + sst_dir_);
    }

    if (!entry.is_regular_file()) {
      continue;
    }

    const std::filesystem::path& path = entry.path();
    if (path.extension() != ".sst") {
      continue;
    }

    const std::string stem = path.stem().string();
    if (stem.empty()) {
      continue;
    }

    uint64_t number = 0;
    bool valid = true;
    for (char c : stem) {
      if (c < '0' || c > '9') {
        valid = false;
        break;
      }
      number = number * 10 + static_cast<uint64_t>(c - '0');
    }
    if (!valid) {
      continue;
    }

    files.push_back({number, path.string()});
  }

  std::sort(files.begin(), files.end(), [](const auto& a, const auto& b) {
    return a.first < b.first;
  });

  for (const auto& [number, file] : files) {
    (void)number;
    sst_files_.push_back(file);

    // Get max_seq from the file via TableReader
    std::unique_ptr<TableReader> reader;
    Status s = TableReader::Open(file, &reader);
    if (!s.ok()) {
      return s;
    }
    *max_seq = std::max(*max_seq, reader->MaxSequence());
  }

  if (!files.empty()) {
    next_file_number_ = files.back().first + 1;
  }

  return Status::OK();
}

Status DBImpl::ValidateSnapshot(const Snapshot* snapshot) const {
  if (snapshot == nullptr) {
    return Status::OK();
  }
  if (active_snapshots_.find(snapshot) == active_snapshots_.end()) {
    return Status::InvalidArgument("snapshot is not active");
  }
  return Status::OK();
}

uint64_t DBImpl::ResolveReadSequence(const ReadOptions& options) const {
  if (options.snapshot != nullptr) {
    return options.snapshot->sequence();
  }
  return std::numeric_limits<uint64_t>::max();
}

bool DBImpl::ShouldSync(const WriteOptions& options) const noexcept {
  return options.sync || options_.sync_on_write;
}

bool DBImpl::ShouldSync(const TxnOptions& options) const noexcept {
  return options.sync_on_commit || options_.sync_on_write;
}

Status DBImpl::ValidateKey(const Slice& key) const {
  if (key.empty()) {
    return Status::InvalidArgument("key is empty");
  }
  return Status::OK();
}

void DBImpl::InvalidateCacheEntry(const std::string& key) const {
  if (cache_ != nullptr) {
    cache_->Erase(key);
  }
}

std::string DBImpl::BuildWalPath(const DBOptions& options) {
  if (!options.wal_path.empty()) {
    return options.wal_path;
  }
  if (options.db_path.empty()) {
    return {};
  }
  return options.db_path + "/wal.log";
}

std::string DBImpl::BuildSSTDirPath(const DBOptions& options) {
  if (!options.sst_dir.empty()) {
    return options.sst_dir;
  }
  if (options.db_path.empty()) {
    return {};
  }
  return options.db_path + "/sst";
}

std::string DBImpl::BuildManifestPath(const DBOptions& options) {
  if (!options.manifest_path.empty()) {
    return options.manifest_path;
  }
  if (options.db_path.empty()) {
    return {};
  }
  return options.db_path + "/MANIFEST";
}

std::string DBImpl::BuildSSTFileName(uint64_t file_number) {
  std::ostringstream oss;
  oss << std::setw(20) << std::setfill('0') << file_number << ".sst";
  return oss.str();
}

std::string DBImpl::BuildSSTCacheKey(const std::string& sst_file,
                                     const std::string& user_key) {
  std::string out;
  out.reserve(sst_file.size() + 1 + user_key.size());
  out.append(sst_file);
  out.push_back('\n');
  out.append(user_key);
  return out;
}

Status DBImpl::CompactSSTFilesLocked() {
  if (sst_files_.size() <= 1) {
    return Status::OK();
  }
  const std::vector<std::string> old_files = sst_files_;

  std::map<std::string, CompactionEntry> latest;
  uint64_t compact_max_seq = 0;

  for (const auto& file : sst_files_) {
    std::shared_ptr<const TableReader> reader;
    Status s = table_cache_->Get(file, &reader);
    if (!s.ok()) {
      return s;
    }

    for (const auto& index_entry : reader->index()) {
      std::string block_data;
      s = reader->ReadBlock(index_entry.block_offset,
                            index_entry.block_size,
                            &block_data);
      if (!s.ok()) {
        return s;
      }

      Block block(block_data.data(), block_data.size());
      BlockIterator iter(block);
      for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        const std::string k = std::string(iter.key());
        const uint64_t seq = iter.seq();
        const uint8_t type = iter.type();

        compact_max_seq = std::max(compact_max_seq, seq);
        auto it = latest.find(k);
        if (it == latest.end() || seq > it->second.seq) {
          CompactionEntry entry;
          entry.seq = seq;
          entry.type = type;
          DecodeSSTValue(std::string(iter.value()), &entry.value,
                         &entry.expires_at_ms);
          latest[k] = std::move(entry);
        }
      }
    }
  }

  if (latest.empty()) {
    return Status::OK();
  }

  std::error_code ec;
  std::filesystem::create_directories(sst_dir_, ec);
  if (ec) {
    return Status::IOError("failed to create sst dir: " + sst_dir_);
  }

  const std::string sst_path =
      sst_dir_ + "/" + BuildSSTFileName(next_file_number_);
  const uint64_t file_number = next_file_number_;

  TableBuilder builder(sst_path);
  for (const auto& [key, entry] : latest) {
    Status s = builder.Add(key, entry.seq, entry.type, entry.expires_at_ms,
                           entry.value);
    if (!s.ok()) {
      return s;
    }
  }

  Status s = builder.Finish();
  if (!s.ok()) {
    return s;
  }

  if (manifest_.IsOpen()) {
    ManifestFileMeta meta;
    meta.file_number = file_number;
    meta.file_path = sst_path;
    meta.max_seq = compact_max_seq;
    s = manifest_.AddFile(meta);
    if (!s.ok()) {
      return s;
    }

    for (const auto& old_file : old_files) {
      // Parse file number from path
      const std::filesystem::path path(old_file);
      if (path.extension() != ".sst") {
        return Status::Corruption("unexpected sst extension: " + old_file);
      }
      const std::string stem = path.stem().string();
      uint64_t old_number = 0;
      for (char c : stem) {
        if (c < '0' || c > '9') {
          return Status::Corruption(
              "failed to parse old sst file number: " + old_file);
        }
        old_number = old_number * 10 + static_cast<uint64_t>(c - '0');
      }
      s = manifest_.RemoveFile(old_number);
      if (!s.ok()) {
        return s;
      }
    }
  }

  // Evict old files from cache
  for (const auto& old_file : old_files) {
    table_cache_->Evict(old_file);
  }

  sst_files_.clear();
  sst_files_.push_back(sst_path);
  ++next_file_number_;

  // Delete old files
  for (const auto& old_file : old_files) {
    std::error_code remove_ec;
    (void)std::filesystem::remove(old_file, remove_ec);
  }

  return RebuildLatestKeySeqIndex();
}

}  // namespace kv
