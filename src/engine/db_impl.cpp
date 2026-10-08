#include "kv/engine/db_impl.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

#include "kv/engine/recovery.h"
#include "kv/common/file_compat.h"
#include "kv/common/time.h"
#include "kv/sstable/block.h"
#include "kv/sstable/block_iterator.h"
#include "kv/sstable/table_builder.h"
#include "kv/sstable/table_cache.h"
#include "kv/sstable/table_reader.h"
#include "kv/sstable/value_codec.h"
#include "kv/table/bloom_filter.h"
#include "kv/table/table_index.h"
#include "kv/testing/failure_injection.h"
#include "kv/wal/log_recovery.h"

namespace kv {

// Forward declaration of the merging iterator factory (defined in iterator.cpp).
std::unique_ptr<Iterator> NewMergingIterator(
    std::vector<std::unique_ptr<MemTable>> memtables,
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

Status CopyMemTable(const MemTable& source,
                    std::unique_ptr<MemTable>* destination) {
  if (destination == nullptr) {
    return Status::InvalidArgument("memtable copy destination is null");
  }

  auto copy = std::make_unique<MemTable>();
  auto it = source.NewIterator();
  for (it.SeekToFirst(); it.Valid(); it.Next()) {
    const auto& entry = it.entry();
    Status s;
    if (entry.type == RecordType::kDeletion) {
      s = copy->Delete(entry.seq, entry.key);
    } else {
      s = copy->Put(entry.seq, entry.key, entry.value, entry.expires_at_ms);
    }
    if (!s.ok()) {
      return s;
    }
  }

  *destination = std::move(copy);
  return Status::OK();
}

Status SyncFilePath(const std::filesystem::path& path) {
  const int fd = platform::OpenSyncFile(path.string(), true);
  if (fd < 0) {
    return Status::IOError("failed to open file for sync: " + path.string() +
                           ": " + platform::FileErrorString());
  }
  const int sync_result = platform::SyncFile(fd);
  const std::string sync_error =
      sync_result == 0 ? "" : platform::FileErrorString();
  const int close_result = platform::CloseFile(fd);
  if (sync_result != 0) {
    return Status::IOError("failed to sync file: " + path.string() + ": " +
                           sync_error);
  }
  if (close_result != 0) {
    return Status::IOError("failed to close synced file: " + path.string() +
                           ": " + platform::FileErrorString());
  }
  return Status::OK();
}

Status SyncParentDirectory(const std::filesystem::path& path) {
  const std::filesystem::path parent = path.parent_path();
  if (parent.empty()) {
    return Status::OK();
  }
  if (platform::SyncDirectory(parent.string()) != 0) {
    return Status::IOError("failed to sync directory: " + parent.string() +
                           ": " + platform::FileErrorString());
  }
  return Status::OK();
}

Status SyncDirectoryPath(const std::filesystem::path& path) {
  if (platform::SyncDirectory(path.string()) != 0) {
    return Status::IOError("failed to sync directory: " + path.string() +
                           ": " + platform::FileErrorString());
  }
  return Status::OK();
}

Status RemovePath(const std::filesystem::path& path, const std::string& label) {
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    return Status::IOError("failed to remove " + label + ": " + path.string());
  }
  return Status::OK();
}

Status MovePath(const std::filesystem::path& source,
                const std::filesystem::path& destination,
                const std::string& label, bool* moved) {
  if (moved == nullptr) {
    return Status::InvalidArgument("move result is null");
  }
  *moved = false;
  std::error_code ec;
  const bool exists = std::filesystem::exists(source, ec);
  if (ec) {
    return Status::IOError("failed to query " + label + ": " + source.string());
  }
  if (!exists) {
    return Status::OK();
  }
  std::filesystem::rename(source, destination, ec);
  if (ec) {
    return Status::IOError("failed to move " + label + ": " + source.string());
  }
  *moved = true;
  return SyncParentDirectory(source);
}

std::string NextCheckpointInstallSuffix() {
  static std::atomic<uint64_t> counter{0};
  const uint64_t id = counter.fetch_add(1, std::memory_order_relaxed);
  const uint64_t now = static_cast<uint64_t>(
      std::chrono::steady_clock::now().time_since_epoch().count());
  return ".checkpoint-install-" + std::to_string(now) + "-" +
         std::to_string(id);
}

}  // namespace

DBImpl::DBImpl(DBOptions options)
    : options_(std::move(options)),
      wal_path_(),
      wal_dir_(),
      sst_dir_(),
      manifest_path_(),
      sst_files_(),
      memtable_(std::make_unique<MemTable>()),
      immutable_mems_(),
      wal_writer_(),
      wal_manager_(),
      using_segmented_wal_(false),
      next_seq_(1),
      next_file_number_(1),
      open_(false),
      active_transactions_(),
      latest_key_seq_(),
      active_snapshots_(),
      owned_snapshots_(),
      cache_(),
      table_cache_(std::make_unique<TableCache>(options_.table_cache_capacity)) {}

DBImpl::~DBImpl() {
  (void)Close();
}

Status DBImpl::Init() {
  std::lock_guard<std::mutex> lk(mu_);

  if (open_) {
    return Status::AlreadyExists("db is already open");
  }

  shutting_down_ = false;
  background_error_ = Status::OK();
  wal_append_generation_.store(0, std::memory_order_release);
  wal_synced_generation_ = 0;
  wal_sync_in_progress_ = false;
  wal_sync_error_ = Status::OK();
  immutable_mems_.clear();
  memtable_ = std::make_unique<MemTable>();
  latest_key_seq_.clear();
  active_transactions_.clear();
  active_snapshots_.clear();
  owned_snapshots_.clear();

  using_segmented_wal_ = options_.wal_path.empty() && options_.wal_segmented;
  wal_path_ = using_segmented_wal_ ? std::string{} : BuildWalPath(options_);
  wal_dir_ = using_segmented_wal_ ? BuildWalDirPath(options_) : std::string{};
  sst_dir_ = BuildSSTDirPath(options_);
  manifest_path_ = BuildManifestPath(options_);
  if (options_.cache_enabled) {
    cache_ = CreateCache(
        options_.cache_policy, options_.cache_capacity,
        options_.cache_default_ttl_ms);
  } else {
    cache_.reset();
  }

  if (using_segmented_wal_ && wal_dir_.empty()) {
    return Status::InvalidArgument("wal dir is empty");
  }
  if (!using_segmented_wal_ && wal_path_.empty()) {
    return Status::InvalidArgument("wal path is empty");
  }
  if (sst_dir_.empty()) {
    return Status::InvalidArgument("sst dir path is empty");
  }
  if (manifest_path_.empty()) {
    return Status::InvalidArgument("manifest path is empty");
  }
  if (options_.max_immutable_memtables == 0) {
    return Status::InvalidArgument("max immutable memtables must be positive");
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

  bool wal_exists = false;
  std::vector<std::string> wal_segments;
  if (using_segmented_wal_) {
    s = WALManager::ListLogs(wal_dir_, &wal_segments);
    if (!s.ok()) {
      return s;
    }
    wal_exists = !wal_segments.empty();
  } else {
    const std::filesystem::path wal_fs_path(wal_path_);
    std::error_code ec;
    wal_exists = std::filesystem::exists(wal_fs_path, ec);
    if (ec) {
      return Status::IOError("failed to query wal path: " + wal_path_);
    }
  }

  if (!wal_exists && !options_.create_if_missing && sst_files_.empty()) {
    return Status::NotFound("db does not exist: missing wal and sst files");
  }

  uint64_t max_seq = 0;
  if (using_segmented_wal_) {
    if (!wal_segments.empty()) {
      s = LogRecovery::ReplayLogs(wal_segments, memtable_.get(), &max_seq);
      if (!s.ok()) {
        return s;
      }
    }
  } else if (wal_exists) {
    s = Recovery::ReplayWAL(wal_path_, memtable_.get(), &max_seq);
    if (!s.ok()) {
      return s;
    }
  }

  if (using_segmented_wal_) {
    WALOptions wal_options;
    wal_options.sync_on_write = false;
    wal_options.max_log_file_size = options_.wal_segment_size_bytes;
    wal_options.wal_dir = wal_dir_;
    s = wal_manager_.Open(wal_options);
  } else {
    s = wal_writer_.Open(wal_path_, true);
  }
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
  flush_thread_ = std::thread(&DBImpl::FlushWorker, this);
  return Status::OK();
}

Status DBImpl::Put(const WriteOptions& options,
                   const Slice& key,
                   const Slice& value) {
  std::unique_lock<std::mutex> lk(mu_);

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

  if (ShouldSync(options)) {
    s = SyncWALGroup(lk);
    if (!s.ok()) {
      return s;
    }
  }

  s = MaybeFlushMemTable(lk);
  if (!s.ok()) {
    return s;
  }

  return Status::OK();
}

Status DBImpl::ApplyPut(const std::string& key, const std::string& value) {
  std::unique_lock<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) return open_status;

  const uint64_t seq = next_seq_;
  Status s = ValidateKey(Slice(key));
  if (!s.ok()) return s;
  WriteOptions durable_options;
  durable_options.sync = true;
  s = ApplyPut(seq, durable_options, Slice(key), Slice(value));
  if (!s.ok()) return s;
  ++next_seq_;
  s = SyncWALGroup(lk);
  if (!s.ok()) return s;
  return MaybeFlushMemTable(lk);
}

Status DBImpl::ApplyDelete(const std::string& key) {
  std::unique_lock<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) return open_status;

  const uint64_t seq = next_seq_;
  Status s = ValidateKey(Slice(key));
  if (!s.ok()) return s;
  WriteOptions durable_options;
  durable_options.sync = true;
  s = ApplyDelete(seq, durable_options, Slice(key));
  if (!s.ok()) return s;
  ++next_seq_;
  s = SyncWALGroup(lk);
  if (!s.ok()) return s;
  return MaybeFlushMemTable(lk);
}

Status DBImpl::ApplyPutWithExpiry(const std::string& key,
                                  const std::string& value,
                                  uint64_t expires_at_ms) {
  std::unique_lock<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) return open_status;
  Status s = ValidateKey(Slice(key));
  if (!s.ok()) return s;
  WriteOptions durable_options;
  durable_options.sync = true;
  s = ApplyPutWithExpiry(next_seq_, durable_options, Slice(key), Slice(value),
                         expires_at_ms);
  if (!s.ok()) return s;
  ++next_seq_;
  s = SyncWALGroup(lk);
  if (!s.ok()) return s;
  return MaybeFlushMemTable(lk);
}

Status DBImpl::ApplyExpireAt(const std::string& key, uint64_t expires_at_ms) {
  std::unique_lock<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) return open_status;
  Status s = ValidateKey(Slice(key));
  if (!s.ok()) return s;

  std::string value;
  s = GetAtSequence(Slice(key), std::numeric_limits<uint64_t>::max(), &value);
  if (!s.ok()) return s;
  WriteOptions durable_options;
  durable_options.sync = true;
  s = ApplyPutWithExpiry(next_seq_, durable_options, Slice(key), Slice(value),
                         expires_at_ms);
  if (!s.ok()) return s;
  ++next_seq_;
  s = SyncWALGroup(lk);
  if (!s.ok()) return s;
  return MaybeFlushMemTable(lk);
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
  std::unique_lock<std::mutex> lk(mu_);

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

  if (ShouldSync(options)) {
    s = SyncWALGroup(lk);
    if (!s.ok()) {
      return s;
    }
  }

  s = MaybeFlushMemTable(lk);
  if (!s.ok()) {
    return s;
  }

  return Status::OK();
}

Status DBImpl::Expire(const WriteOptions& options, const Slice& key,
                      int64_t ttl_seconds) {
  std::unique_lock<std::mutex> lk(mu_);

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
  if (ShouldSync(options)) {
    s = SyncWALGroup(lk);
    if (!s.ok()) return s;
  }
  return MaybeFlushMemTable(lk);
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
  std::unique_lock<std::mutex> lk(mu_);

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
  if (ShouldSync(options)) {
    s = SyncWALGroup(lk);
    if (!s.ok()) return s;
  }
  return MaybeFlushMemTable(lk);
}

Status DBImpl::Write(const WriteOptions& options, const WriteBatch& batch) {
  std::unique_lock<std::mutex> lk(mu_);

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
    Status s = SyncWALGroup(lk);
    if (!s.ok()) {
      return s;
    }
  }

  return MaybeFlushMemTable(lk);
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
  std::unique_lock<std::mutex> lk(mu_);

  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) {
    return open_status;
  }
  if (!active_snapshots_.empty()) {
    return Status::AlreadyExists("cannot compact with active snapshots");
  }

  if (compaction_in_progress_) {
    return Status::AlreadyExists("compaction already in progress");
  }

  if (!memtable_->Empty()) {
    RotateMemTable();
    flush_cv_.notify_one();
  }
  Status flush_status = WaitForImmutableMemTables(lk);
  if (!flush_status.ok()) {
    return flush_status;
  }

  size_t l0_file_count = 0;
  size_t l1_file_count = 0;
  for (const auto& state : sst_files_) {
    if (state.metadata.level == 0) {
      ++l0_file_count;
    } else if (state.metadata.level == 1) {
      ++l1_file_count;
    }
  }
  if (l0_file_count < options_.compaction_min_input_files &&
      l1_file_count < options_.compaction_min_input_files) {
    return Status::NotFound("not enough sst files for compaction");
  }

  ++compaction_stats_.trigger_attempts;
  Status s = CompactSSTFiles(lk);
  if (s.ok()) {
    ++compaction_stats_.succeeded;
  } else {
    ++compaction_stats_.failed;
  }
  return s;
}

Status DBImpl::CreateCheckpoint(const std::string& checkpoint_dir) {
  if (checkpoint_dir.empty()) {
    return Status::InvalidArgument("checkpoint directory is empty");
  }

  std::unique_lock<std::mutex> lk(mu_);
  Status open_status = RequireOpen(open_);
  if (!open_status.ok()) {
    return open_status;
  }

  if (!memtable_->Empty()) {
    RotateMemTable();
    flush_cv_.notify_one();
  }
  Status flush_status = WaitForImmutableMemTables(lk);
  if (!flush_status.ok()) {
    return flush_status;
  }

  const std::filesystem::path checkpoint_path(checkpoint_dir);
  const std::filesystem::path tmp_path = checkpoint_dir + ".tmp";
  std::error_code ec;
  if (std::filesystem::exists(checkpoint_path, ec)) {
    return Status::AlreadyExists("checkpoint already exists: " + checkpoint_dir);
  }
  if (ec) {
    return Status::IOError("failed to query checkpoint directory: " +
                           checkpoint_dir);
  }

  std::filesystem::remove_all(tmp_path, ec);
  if (ec) {
    return Status::IOError("failed to clear checkpoint temporary directory: " +
                           tmp_path.string());
  }
  std::filesystem::create_directories(tmp_path / "sst", ec);
  if (ec) {
    return Status::IOError("failed to create checkpoint directory: " +
                           tmp_path.string());
  }

  std::vector<ManifestFileMeta> files;
  Status s = manifest_.Recover(&files);
  if (!s.ok()) {
    std::filesystem::remove_all(tmp_path, ec);
    return s;
  }

  for (const auto& file : files) {
    const std::filesystem::path destination =
        tmp_path / "sst" / BuildSSTFileName(file.file_number);
    std::filesystem::copy_file(file.file_path, destination, ec);
    if (ec) {
      std::filesystem::remove_all(tmp_path, ec);
      return Status::IOError("failed to copy checkpoint sst: " +
                             file.file_path);
    }
  }

  Manifest checkpoint_manifest;
  const std::string checkpoint_manifest_path =
      (tmp_path / "MANIFEST").string();
  s = checkpoint_manifest.Open(checkpoint_manifest_path, true);
  if (s.ok()) {
    for (const auto& file : files) {
      ManifestFileMeta checkpoint_file = file;
      checkpoint_file.file_path =
          (checkpoint_path / "sst" / BuildSSTFileName(file.file_number))
              .string();
      s = checkpoint_manifest.AddFile(checkpoint_file);
      if (!s.ok()) {
        break;
      }
    }
  }
  if (s.ok()) {
    s = checkpoint_manifest.Sync();
  }
  Status close_status = checkpoint_manifest.Close();
  if (s.ok() && !close_status.ok()) {
    s = close_status;
  }
  if (!s.ok()) {
    std::filesystem::remove_all(tmp_path, ec);
    return s;
  }

  std::filesystem::rename(tmp_path, checkpoint_path, ec);
  if (ec) {
    std::filesystem::remove_all(tmp_path, ec);
    return Status::IOError("failed to publish checkpoint: " + checkpoint_dir);
  }
  return Status::OK();
}

Status DBImpl::InstallCheckpoint(const std::string& checkpoint_dir) {
  if (checkpoint_dir.empty()) {
    return Status::InvalidArgument("checkpoint directory is empty");
  }

  const std::filesystem::path source(checkpoint_dir);
  const std::filesystem::path source_manifest = source / "MANIFEST";
  const std::filesystem::path source_sst = source / "sst";
  std::error_code ec;
  if (!std::filesystem::is_regular_file(source_manifest, ec) || ec ||
      !std::filesystem::is_directory(source_sst, ec) || ec) {
    return Status::Corruption("checkpoint is incomplete: " + checkpoint_dir);
  }

  Manifest source_manifest_reader;
  Status s = source_manifest_reader.Open(source_manifest.string(), false);
  if (!s.ok()) {
    return s;
  }
  std::vector<ManifestFileMeta> files;
  s = source_manifest_reader.Recover(&files);
  Status source_close_status = source_manifest_reader.Close();
  if (s.ok() && !source_close_status.ok()) {
    s = source_close_status;
  }
  if (!s.ok()) {
    return s;
  }

  std::filesystem::path target_sst;
  std::filesystem::path target_manifest;
  std::filesystem::path target_wal;
  bool segmented_wal = false;
  {
    std::lock_guard<std::mutex> lk(mu_);
    s = RequireOpen(open_);
    if (!s.ok()) {
      return s;
    }
    target_sst = sst_dir_;
    target_manifest = manifest_path_;
    segmented_wal = using_segmented_wal_;
    target_wal = segmented_wal ? std::filesystem::path(wal_dir_)
                               : std::filesystem::path(wal_path_);
  }
  if (target_sst.empty() || target_manifest.empty() || target_wal.empty()) {
    return Status::InvalidArgument("database paths are not initialized");
  }

  // Validate every manifest entry before touching the live database. Snapshot
  // archives can be extracted under a new directory, so rebuild paths from
  // the checkpoint's own immutable SST directory instead of trusting the
  // serialized source-machine paths in its manifest.
  for (const auto& file : files) {
    const std::filesystem::path expected =
        (source_sst / BuildSSTFileName(file.file_number)).lexically_normal();
    if (file.file_number == 0 ||
        std::filesystem::path(file.file_path).filename() != expected.filename()) {
      return Status::Corruption("checkpoint manifest references an invalid sst");
    }
    if (!std::filesystem::is_regular_file(expected, ec) || ec) {
      return Status::Corruption("checkpoint references missing sst: " +
                                expected.string());
    }
  }

  const std::string suffix = NextCheckpointInstallSuffix();
  const std::filesystem::path staged_sst = target_sst.string() + suffix + ".stage";
  const std::filesystem::path staged_manifest =
      target_manifest.string() + suffix + ".stage";
  const std::filesystem::path backup_sst = target_sst.string() + suffix + ".backup";
  const std::filesystem::path backup_manifest =
      target_manifest.string() + suffix + ".backup";
  const std::filesystem::path backup_wal = target_wal.string() + suffix + ".backup";

  for (const auto& path : {staged_sst, staged_manifest, backup_sst,
                           backup_manifest, backup_wal}) {
    if (std::filesystem::exists(path, ec) || ec) {
      if (ec) {
        return Status::IOError("failed to query checkpoint staging path: " +
                               path.string());
      }
      return Status::AlreadyExists("checkpoint staging path already exists: " +
                                   path.string());
    }
  }

  std::filesystem::create_directories(staged_sst, ec);
  if (ec) {
    return Status::IOError("failed to create checkpoint staging directory: " +
                           staged_sst.string());
  }
  auto remove_staged = [&] {
    (void)RemovePath(staged_sst, "checkpoint staging directory");
    (void)RemovePath(staged_manifest, "checkpoint staging manifest");
  };

  for (const auto& file : files) {
    const std::filesystem::path source_file =
        source_sst / BuildSSTFileName(file.file_number);
    const std::filesystem::path destination =
        staged_sst / BuildSSTFileName(file.file_number);
    std::filesystem::copy_file(source_file, destination, ec);
    if (ec) {
      remove_staged();
      return Status::IOError("failed to stage checkpoint sst: " +
                             source_file.string());
    }
    s = SyncFilePath(destination);
    if (!s.ok()) {
      remove_staged();
      return s;
    }
    s = testing::MaybeInjectFailure(testing::FailurePoint::kDuringCheckpointStage);
    if (!s.ok()) {
      remove_staged();
      return s;
    }
  }

  s = SyncDirectoryPath(staged_sst);
  if (!s.ok()) {
    remove_staged();
    return s;
  }

  Manifest staged_manifest_writer;
  s = staged_manifest_writer.Open(staged_manifest.string(), true);
  for (const auto& file : files) {
    if (!s.ok()) {
      break;
    }
    ManifestFileMeta staged_file = file;
    staged_file.file_path =
        (target_sst / BuildSSTFileName(file.file_number)).string();
    s = staged_manifest_writer.AddFile(staged_file);
  }
  if (s.ok()) {
    s = staged_manifest_writer.Sync();
  }
  Status manifest_close_status = staged_manifest_writer.Close();
  if (s.ok() && !manifest_close_status.ok()) {
    s = manifest_close_status;
  }
  if (!s.ok()) {
    remove_staged();
    return s;
  }

  s = SyncParentDirectory(staged_manifest);
  if (!s.ok()) {
    remove_staged();
    return s;
  }

  Status close_status = Close();
  if (!close_status.ok()) {
    remove_staged();
    return close_status;
  }

  bool backed_up_sst = false;
  bool backed_up_manifest = false;
  bool backed_up_wal = false;
  bool published_sst = false;
  bool published_manifest = false;
  bool init_attempted = false;

  auto rollback = [&](const Status& failure) {
    Status rollback_status = Status::OK();
    auto remember_failure = [&rollback_status](const Status& candidate) {
      if (rollback_status.ok() && !candidate.ok()) {
        rollback_status = candidate;
      }
    };
    if (init_attempted) {
      remember_failure(CloseWAL());
      remember_failure(manifest_.Close());
      if (table_cache_ != nullptr) {
        table_cache_->Clear();
      }
      remember_failure(RemovePath(target_wal, "partially initialized wal"));
    }
    if (published_manifest) {
      remember_failure(RemovePath(target_manifest, "published checkpoint manifest"));
    }
    if (published_sst) {
      remember_failure(RemovePath(target_sst, "published checkpoint sst"));
    }

    auto restore = [&remember_failure](const std::filesystem::path& backup,
                                        const std::filesystem::path& target,
                                        const std::string& label, bool moved) {
      if (!moved) {
        return;
      }
      bool restored = false;
      Status restore_status = MovePath(backup, target, label, &restored);
      if (restore_status.ok() && !restored) {
        restore_status = Status::IOError("missing checkpoint backup: " +
                                         backup.string());
      }
      remember_failure(restore_status);
    };
    restore(backup_wal, target_wal, "checkpoint wal backup", backed_up_wal);
    restore(backup_manifest, target_manifest, "checkpoint manifest backup",
            backed_up_manifest);
    restore(backup_sst, target_sst, "checkpoint sst backup", backed_up_sst);
    remove_staged();

    if (rollback_status.ok()) {
      Status reopen_status = Init();
      if (!reopen_status.ok()) {
        rollback_status = reopen_status;
      }
    }
    if (!rollback_status.ok()) {
      return Status::IOError("checkpoint install failed: " + failure.ToString() +
                             "; rollback failed: " + rollback_status.ToString());
    }
    return failure;
  };

  s = MovePath(target_sst, backup_sst, "local sst directory", &backed_up_sst);
  if (!s.ok()) {
    return rollback(s);
  }
  s = MovePath(target_manifest, backup_manifest, "local manifest",
               &backed_up_manifest);
  if (!s.ok()) {
    return rollback(s);
  }
  s = MovePath(target_wal, backup_wal,
               segmented_wal ? "local wal directory" : "local wal",
               &backed_up_wal);
  if (!s.ok()) {
    return rollback(s);
  }

  s = testing::MaybeInjectFailure(testing::FailurePoint::kDuringCheckpointPublish);
  if (!s.ok()) {
    return rollback(s);
  }
  s = MovePath(staged_sst, target_sst, "staged checkpoint sst", &published_sst);
  if (!s.ok() || !published_sst) {
    return rollback(s.ok() ? Status::IOError("missing staged checkpoint sst") : s);
  }
  s = MovePath(staged_manifest, target_manifest, "staged checkpoint manifest",
               &published_manifest);
  if (!s.ok() || !published_manifest) {
    return rollback(s.ok() ? Status::IOError("missing staged checkpoint manifest")
                           : s);
  }

  s = testing::MaybeInjectFailure(
      testing::FailurePoint::kAfterCheckpointPublishBeforeInit);
  if (!s.ok()) {
    return rollback(s);
  }
  init_attempted = true;
  s = Init();
  if (!s.ok()) {
    return rollback(s);
  }

  (void)RemovePath(backup_sst, "checkpoint sst backup");
  (void)RemovePath(backup_manifest, "checkpoint manifest backup");
  (void)RemovePath(backup_wal, "checkpoint wal backup");
  return Status::OK();
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

Status DBImpl::GetFlushStats(FlushStats* stats) const {
  std::lock_guard<std::mutex> lk(mu_);
  if (stats == nullptr) {
    return Status::InvalidArgument("flush stats output is null");
  }
  *stats = flush_stats_;
  stats->queue_length = immutable_mems_.size();
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
  std::unique_lock<std::mutex> lk(mu_);

  // Compaction may be reclaiming versions based on the absence of snapshots.
  // Delay snapshot creation until the commit point so it cannot race with
  // that decision.
  compaction_done_cv_.wait(lk, [this] { return !compaction_in_progress_; });

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

  // Copy in-memory tables so the returned iterator remains valid after a
  // later rotation or background flush.
  std::vector<std::unique_ptr<MemTable>> memtables;
  memtables.reserve(immutable_mems_.size() + 1);
  std::unique_ptr<MemTable> memtable_copy;
  Status s = CopyMemTable(*memtable_, &memtable_copy);
  if (!s.ok()) {
    return nullptr;
  }
  memtables.push_back(std::move(memtable_copy));
  for (const auto& immutable_mem : immutable_mems_) {
    std::unique_ptr<MemTable> immutable_copy;
    s = CopyMemTable(*immutable_mem.table, &immutable_copy);
    if (!s.ok()) {
      return nullptr;
    }
    memtables.push_back(std::move(immutable_copy));
  }

  // SST iterators.
  std::vector<std::unique_ptr<TableIterator>> sst_iters;
  sst_iters.reserve(sst_files_.size());
  for (const auto& state : sst_files_) {
    std::shared_ptr<const TableReader> reader;
    s = table_cache_->Get(state.file_path, &reader);
    if (!s.ok()) {
      return nullptr;
    }
    sst_iters.push_back(std::make_unique<TableIterator>(*reader));
  }

  return NewMergingIterator(std::move(memtables), std::move(sst_iters),
                            read_seq);
}

Status DBImpl::Scan(
    const ReadOptions& options,
    const Slice& start_key,
    const Slice& end_key,
    size_t limit,
    std::vector<std::pair<std::string, std::string>>* out) {
  if (out == nullptr) {
    return Status::InvalidArgument("scan output is null");
  }
  out->clear();

  auto it = NewIterator(options);
  if (it == nullptr) {
    return Status::IOError("failed to create scan iterator");
  }

  const std::string end = end_key.ToString();
  for (it->Seek(start_key); it->Valid(); it->Next()) {
    if (!end.empty() && it->key().ToString() >= end) {
      break;
    }
    out->emplace_back(it->key().ToString(), it->value().ToString());
    if (limit != 0 && out->size() == limit) {
      break;
    }
  }
  return Status::OK();
}

Status DBImpl::Close() {
  std::unique_lock<std::mutex> lk(mu_);

  if (!open_) {
    return Status::OK();
  }

  shutting_down_ = true;
  open_ = false;
  if (background_error_.ok() && !memtable_->Empty()) {
    RotateMemTable();
  }
  flush_cv_.notify_one();

  if (background_error_.ok()) {
    flush_done_cv_.wait(lk, [this] {
      return immutable_mems_.empty() || !background_error_.ok();
    });
  }
  compaction_done_cv_.wait(lk, [this] { return !compaction_in_progress_; });
  flush_cv_.notify_one();
  lk.unlock();
  if (flush_thread_.joinable()) {
    flush_thread_.join();
  }
  lk.lock();

  // A grouped WAL sync temporarily releases mu_. Wait for that sync to
  // publish its result before closing the WAL, otherwise CloseWAL() could
  // hold wal_mu_ while the sync thread waits to reacquire mu_.
  wal_sync_cv_.wait(lk, [this] { return !wal_sync_in_progress_; });

  Status result = background_error_;
  Status s = CloseWAL();
  if (result.ok() && !s.ok()) {
    result = s;
  }

  s = manifest_.Close();
  if (result.ok() && !s.ok()) {
    result = s;
  }

  table_cache_->Clear();
  active_transactions_.clear();
  active_snapshots_.clear();
  owned_snapshots_.clear();
  return result;
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
  if (using_segmented_wal_) {
    return wal_manager_.active_log_path();
  }
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
  (void)options;
  return Status::OK();
}

Status DBImpl::ApplyPutWithExpiry(uint64_t seq, const WriteOptions& options,
                                  const Slice& key, const Slice& value,
                                  uint64_t expires_at_ms) {
  Status writable_status = CheckWritable();
  if (!writable_status.ok()) {
    return writable_status;
  }
  if (expires_at_ms == 0) {
    return ApplyPut(seq, options, key, value);
  }

  Status s = AppendWALPutWithTTL(seq, key, value, expires_at_ms);
  if (!s.ok()) return s;
  s = memtable_->Put(seq, key, value, expires_at_ms);
  if (!s.ok()) return s;
  InvalidateCacheEntry(key.ToString());
  latest_key_seq_[key.ToString()] = seq;
  (void)options;
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
  (void)options;
  return Status::OK();
}

Status DBImpl::ApplyOperationLocked(
    uint64_t seq, const WriteBatch::Operation& operation) {
  Status writable_status = CheckWritable();
  if (!writable_status.ok()) {
    return writable_status;
  }

  Status s;
  if (operation.type == WriteBatch::ValueType::kPut) {
    s = AppendWALPut(seq, operation.key, operation.value);
    if (s.ok()) {
      s = memtable_->Put(seq, operation.key, operation.value);
    }
  } else {
    s = AppendWALDelete(seq, operation.key);
    if (s.ok()) {
      s = memtable_->Delete(seq, operation.key);
    }
  }
  if (!s.ok()) {
    return s;
  }

  InvalidateCacheEntry(operation.key);
  latest_key_seq_[operation.key] = seq;
  return Status::OK();
}

Status DBImpl::AppendWALPut(uint64_t seq,
                            const Slice& key,
                            const Slice& value) {
  std::lock_guard<std::mutex> lk(wal_mu_);
  Status s;
  if (using_segmented_wal_) {
    s = wal_manager_.AppendPut(seq, key.ToString(), value.ToString());
  } else {
    s = wal_writer_.AppendPut(seq, key, value);
  }
  if (s.ok()) {
    wal_append_generation_.fetch_add(1, std::memory_order_release);
  }
  return s;
}

Status DBImpl::AppendWALPutWithTTL(uint64_t seq,
                                   const Slice& key,
                                   const Slice& value,
                                   uint64_t expires_at_ms) {
  std::lock_guard<std::mutex> lk(wal_mu_);
  Status s;
  if (using_segmented_wal_) {
    s = wal_manager_.AppendPutWithTTL(seq, key.ToString(), value.ToString(),
                                      expires_at_ms);
  } else {
    s = wal_writer_.AppendPutWithTTL(seq, key, value, expires_at_ms);
  }
  if (s.ok()) {
    wal_append_generation_.fetch_add(1, std::memory_order_release);
  }
  return s;
}

Status DBImpl::AppendWALDelete(uint64_t seq, const Slice& key) {
  std::lock_guard<std::mutex> lk(wal_mu_);
  Status s;
  if (using_segmented_wal_) {
    s = wal_manager_.AppendDelete(seq, key.ToString());
  } else {
    s = wal_writer_.AppendDelete(seq, key);
  }
  if (s.ok()) {
    wal_append_generation_.fetch_add(1, std::memory_order_release);
  }
  return s;
}

Status DBImpl::SyncWAL() {
  std::lock_guard<std::mutex> lk(wal_mu_);
  return using_segmented_wal_ ? wal_manager_.Sync() : wal_writer_.Sync();
}

Status DBImpl::SyncWALGroup(std::unique_lock<std::mutex>& lock) {
  const uint64_t requested_generation =
      wal_append_generation_.load(std::memory_order_acquire);
  if (requested_generation <= wal_synced_generation_) {
    return Status::OK();
  }

  while (true) {
    if (!wal_sync_in_progress_) {
      wal_sync_in_progress_ = true;
      break;
    }
    wal_sync_cv_.wait(lock, [this, requested_generation] {
      return wal_synced_generation_ >= requested_generation ||
             !wal_sync_in_progress_;
    });
    if (wal_synced_generation_ >= requested_generation) {
      return Status::OK();
    }
    if (!wal_sync_error_.ok()) {
      return wal_sync_error_;
    }
    // The previous group completed before this append reached wal_mu_. Loop
    // and become the sole leader for a second sync covering this writer.
  }

  wal_sync_error_ = Status::OK();
  lock.unlock();

  // Holding wal_mu_ makes the generation snapshot precise: appends that
  // completed before this lock are included in the sync; later appends wait
  // and will be handled by the next group.
  std::unique_lock<std::mutex> wal_lock(wal_mu_);
  const uint64_t sync_generation =
      wal_append_generation_.load(std::memory_order_acquire);
  Status sync_status = using_segmented_wal_ ? wal_manager_.Sync()
                                            : wal_writer_.Sync();
  wal_lock.unlock();

  lock.lock();
  if (sync_status.ok()) {
    wal_synced_generation_ =
        std::max(wal_synced_generation_, sync_generation);
  } else {
    wal_sync_error_ = sync_status;
  }
  wal_sync_in_progress_ = false;
  wal_sync_cv_.notify_all();
  return sync_status;
}

Status DBImpl::CloseWAL() {
  std::lock_guard<std::mutex> lk(wal_mu_);
  return using_segmented_wal_ ? wal_manager_.Close() : wal_writer_.Close();
}

Status DBImpl::RemoveFlushedWAL(uint64_t max_flushed_seq) {
  if (!using_segmented_wal_) {
    return Status::OK();
  }
  std::lock_guard<std::mutex> lk(wal_mu_);
  return wal_manager_.RemoveLogsUpTo(max_flushed_seq);
}

Status DBImpl::MaybeFlushMemTable(std::unique_lock<std::mutex>& lock) {
  if (options_.memtable_write_buffer_size == 0 || memtable_->Empty() ||
      memtable_->ApproximateMemoryUsage() <
          options_.memtable_write_buffer_size) {
    return Status::OK();
  }

  Status s = WaitForImmutableMemTableSpace(lock);
  if (!s.ok()) {
    return s;
  }

  RotateMemTable();
  flush_cv_.notify_one();
  return Status::OK();
}

Status DBImpl::WaitForImmutableMemTables(
    std::unique_lock<std::mutex>& lock) {
  flush_done_cv_.wait(lock, [this] {
    return immutable_mems_.empty() || !background_error_.ok();
  });
  return background_error_;
}

Status DBImpl::WaitForImmutableMemTableSpace(
    std::unique_lock<std::mutex>& lock) {
  const bool will_stall =
      immutable_mems_.size() >= options_.max_immutable_memtables;
  const auto start = std::chrono::steady_clock::now();
  flush_done_cv_.wait(lock, [this] {
    return immutable_mems_.size() < options_.max_immutable_memtables ||
           !background_error_.ok() || shutting_down_;
  });
  if (will_stall) {
    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start);
    ++flush_stats_.write_stalls;
    flush_stats_.write_stall_duration_us +=
        static_cast<uint64_t>(elapsed.count());
  }
  return CheckWritable();
}

void DBImpl::MaybeCompactAfterFlush(std::unique_lock<std::mutex>& lock) {
  if (!options_.auto_compaction_enabled) {
    return;
  }

  if (compaction_in_progress_) {
    return;
  }

  const auto level1_over_threshold = [this]() {
    size_t files = 0;
    uint64_t bytes = 0;
    for (const auto& state : sst_files_) {
      if (state.metadata.level == 1) {
        ++files;
        bytes += state.metadata.file_size;
      }
    }
    return files >= options_.compaction_min_input_files &&
           (options_.compaction_level_max_bytes == 0 ||
            bytes >= options_.compaction_level_max_bytes);
  };

  size_t l0_file_count = 0;
  for (const auto& state : sst_files_) {
    if (state.metadata.level == 0) ++l0_file_count;
  }
  const bool should_compact_l0 =
      l0_file_count >= options_.compaction_min_input_files;
  const bool should_compact_l1 = l0_file_count == 0 && level1_over_threshold();
  if (!should_compact_l0 && !should_compact_l1) {
    ++compaction_stats_.skipped_due_threshold;
    return;
  }
  if (!active_snapshots_.empty()) {
    ++compaction_stats_.skipped_due_snapshot;
    return;
  }

  ++compaction_stats_.trigger_attempts;
  Status compact_status = CompactSSTFiles(lock);
  if (compact_status.ok()) {
    ++compaction_stats_.succeeded;

    // A successful L0→L1 pass can make L1 exceed its size budget. Run one
    // follow-up pass immediately while the flush worker already has control;
    // future flushes will retry if the budget remains exceeded.
    if (should_compact_l0 && level1_over_threshold() &&
        !compaction_in_progress_) {
      ++compaction_stats_.trigger_attempts;
      Status next_status = CompactSSTFiles(lock);
      if (next_status.ok()) {
        ++compaction_stats_.succeeded;
      } else {
        ++compaction_stats_.failed;
      }
    }
    return;
  }

  ++compaction_stats_.failed;
}

void DBImpl::RotateMemTable() {
  ImmutableMemTable immutable_mem;
  immutable_mem.min_sequence = memtable_->MinSequence();
  immutable_mem.max_sequence = memtable_->MaxSequence();
  immutable_mem.approximate_bytes = memtable_->ApproximateMemoryUsage();
  immutable_mem.table = std::move(memtable_);
  immutable_mems_.push_back(std::move(immutable_mem));
  memtable_ = std::make_unique<MemTable>();
}

void DBImpl::FlushWorker() {
  while (true) {
    std::unique_lock<std::mutex> lock(mu_);
    flush_cv_.wait(lock, [this] {
      return shutting_down_ ||
             (!immutable_mems_.empty() && background_error_.ok());
    });

    if (!background_error_.ok()) {
      return;
    }
    if (immutable_mems_.empty()) {
      if (shutting_down_) {
        return;
      }
      continue;
    }

    ImmutableMemTable& immutable_mem = immutable_mems_.front();
    immutable_mem.flushing = true;
    MemTable* memtable = immutable_mem.table.get();
    // Reserve the file number while holding the DB mutex. The actual SST
    // build runs without the mutex, so explicit compaction and concurrent
    // flushes cannot allocate the same output name.
    const uint64_t file_number = next_file_number_++;
    lock.unlock();

    const auto start = std::chrono::steady_clock::now();
    std::string sst_file;
    ManifestFileMeta sst_meta;
    Status s = FlushMemTableToSST(*memtable, file_number, &sst_file, &sst_meta);
    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start);

    lock.lock();
    immutable_mems_.front().flushing = false;
    flush_stats_.total_duration_us += static_cast<uint64_t>(elapsed.count());
    if (!s.ok()) {
      ++flush_stats_.failed;
      background_error_ = s;
      flush_done_cv_.notify_all();
      continue;
    }

    if (manifest_.IsOpen()) {
      s = manifest_.AddFile(sst_meta);
      if (s.ok()) {
        s = testing::MaybeInjectFailure(
            testing::FailurePoint::kAfterManifestAppendBeforeSync);
      }
      if (s.ok()) {
        s = manifest_.Sync();
      }
      if (s.ok()) {
        s = testing::MaybeInjectFailure(
            testing::FailurePoint::kAfterManifestSyncBeforeWALCleanup);
      }
      if (s.ok()) {
        s = RemoveFlushedWAL(sst_meta.max_seq);
      }
      if (!s.ok()) {
        ++flush_stats_.failed;
        background_error_ = s;
        flush_done_cv_.notify_all();
        continue;
      }
    }

    sst_files_.push_back({std::move(sst_file), std::move(sst_meta)});
    immutable_mems_.pop_front();
    ++flush_stats_.completed;
    MaybeCompactAfterFlush(lock);
    flush_done_cv_.notify_all();
  }
}

Status DBImpl::CheckWritable() const {
  if (!open_ || shutting_down_) {
    return Status::IOError("db is not accepting writes");
  }
  return background_error_;
}

Status DBImpl::FlushMemTableToSST(const MemTable& memtable,
                                  uint64_t file_number,
                                  std::string* out_file,
                                  ManifestFileMeta* out_meta) {
  if (out_file == nullptr) {
    return Status::InvalidArgument("out_file is null");
  }
  if (out_meta == nullptr) {
    return Status::InvalidArgument("out_meta is null");
  }

  if (memtable.Empty()) {
    return Status::NotFound("memtable is empty");
  }

  std::error_code ec;
  std::filesystem::create_directories(sst_dir_, ec);
  if (ec) {
    return Status::IOError("failed to create sst dir: " + sst_dir_);
  }

  const std::string sst_path =
      sst_dir_ + "/" + BuildSSTFileName(file_number);

  TableBuilder builder(sst_path, options_.sstable_block_size_bytes,
                       options_.bloom_bits_per_key);

  auto it = memtable.NewIterator();
  uint64_t max_flushed_seq = 0;
  ManifestFileMeta meta;
  meta.file_number = file_number;
  meta.file_path = sst_path;

  for (it.SeekToFirst(); it.Valid(); it.Next()) {
    const auto& entry = it.entry();
    max_flushed_seq = std::max(max_flushed_seq, entry.seq);
    if (meta.smallest_key.empty()) {
      meta.smallest_key = entry.key;
    }
    meta.largest_key = entry.key;

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

  s = testing::MaybeInjectFailure(
      testing::FailurePoint::kAfterSSTableWriteBeforeManifest);
  if (!s.ok()) {
    return s;
  }

  if (builder.FileSize() == 0) {
    std::filesystem::remove(sst_path, ec);
    return Status::NotFound("no visible entries to flush");
  }

  meta.max_seq = max_flushed_seq;
  meta.file_size = std::filesystem::file_size(sst_path, ec);
  if (ec) {
    return Status::IOError("failed to query flushed sst size: " + sst_path);
  }

  *out_file = sst_path;
  *out_meta = meta;
  return Status::OK();
}

Status DBImpl::GetFromMemTableAt(const Slice& key,
                                 uint64_t read_seq,
                                 std::string* value,
                                 uint64_t* expires_at_ms,
                                 bool* has_visible_version) const {
  const std::string target = key.ToString();
  if (has_visible_version != nullptr) *has_visible_version = false;

  const auto find_visible = [&](const MemTable& memtable) -> Status {
    if (memtable.Empty() || memtable.MinSequence() > read_seq) {
      return Status::NotFound("key not found");
    }

    auto it = memtable.NewIterator();
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
  };

  Status s = find_visible(*memtable_);
  if (s.ok() || (has_visible_version != nullptr && *has_visible_version)) {
    return s;
  }

  for (auto it = immutable_mems_.rbegin(); it != immutable_mems_.rend();
       ++it) {
    s = find_visible(*it->table);
    if (s.ok() || (has_visible_version != nullptr && *has_visible_version)) {
      return s;
    }
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

  // Publication order is not version order: a compaction can publish old
  // entries after a concurrent flush. Compare visible entry sequences across
  // files, including tombstones and expired values, before resolving the key.
  bool have_latest = false;
  uint64_t latest_seq = 0;
  uint8_t latest_type = 0;
  uint64_t latest_expires_at_ms = 0;
  std::string latest_value;
  for (size_t i = sst_files_.size(); i-- > 0;) {
    const SSTFileState& state = sst_files_[i];
    if (have_latest && state.metadata.max_seq <= latest_seq) continue;
    const std::string& file = state.file_path;
    if (state.metadata.HasKeyRange()) {
      const auto& range = state.metadata;
      if (target < range.smallest_key || target > range.largest_key) {
        ++read_path_stats_.range_prunes;
        continue;
      }
    }

    std::shared_ptr<const TableReader> reader;
    Status s = table_cache_->Get(file, &reader);
    if (!s.ok()) {
      return s;
    }

    uint8_t entry_type = 0;
    uint64_t entry_seq = 0;
    uint64_t entry_expires_at_ms = 0;
    std::string entry_value;
    TableReadStatsDelta stats_delta;
    s = reader->Get(target, read_seq, &entry_type, &entry_value,
                    &entry_expires_at_ms, &stats_delta, &entry_seq);
    read_path_stats_.bloom_queries += stats_delta.bloom_queries;
    read_path_stats_.bloom_negatives += stats_delta.bloom_negatives;
    if (s.ok() || (s.IsNotFound() && entry_type == 1)) {
      if (!have_latest || entry_seq > latest_seq) {
        have_latest = true;
        latest_seq = entry_seq;
        latest_type = entry_type;
        latest_expires_at_ms = entry_expires_at_ms;
        latest_value = std::move(entry_value);
      }
      continue;
    }
    if (!s.IsNotFound()) return s;
  }

  if (!have_latest) return Status::NotFound("key not found");
  if (latest_type == 1) return Status::NotFound("key deleted");
  if (expires_at_ms != nullptr) *expires_at_ms = latest_expires_at_ms;
  if (IsExpired(latest_expires_at_ms, NowUnixMillis())) {
    return Status::NotFound("key expired");
  }
  *value = std::move(latest_value);
  if (allow_cache && latest_expires_at_ms == 0) cache_->Put(target, *value);
  return Status::OK();
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

  for (const auto& state : sst_files_) {
    const auto& file = state.file_path;
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

  const auto add_memtable_sequences = [&](const MemTable& memtable) {
    auto it = memtable.NewIterator();
    for (it.SeekToFirst(); it.Valid(); it.Next()) {
      const auto& entry = it.entry();
      auto found = latest_key_seq_.find(entry.key);
      if (found == latest_key_seq_.end() || entry.seq > found->second) {
        latest_key_seq_[entry.key] = entry.seq;
      }
    }
  };

  for (const auto& immutable_mem : immutable_mems_) {
    add_memtable_sequences(*immutable_mem.table);
  }
  add_memtable_sequences(*memtable_);

  return Status::OK();
}

Status DBImpl::CommitOCCTransaction(
    const TxnOptions& options,
    uint64_t start_seq,
    const std::unordered_set<std::string>& read_set,
    const std::vector<WriteBatch::Operation>& writes) {
  std::unique_lock<std::mutex> lk(mu_);

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
    Status s = SyncWALGroup(lk);
    if (!s.ok()) {
      return s;
    }
  }

  return MaybeFlushMemTable(lk);
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
    std::error_code ec;
    if (!std::filesystem::exists(f.file_path, ec)) {
      if (ec) {
        return Status::IOError("failed to query sst file: " + f.file_path);
      }
      return Status::Corruption("manifest references missing sst file: " +
                                f.file_path);
    }
    sst_files_.push_back({f.file_path, f});
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
    // Get max_seq from the file via TableReader
    std::unique_ptr<TableReader> reader;
    Status s = TableReader::Open(file, &reader);
    if (!s.ok()) {
      return s;
    }
    *max_seq = std::max(*max_seq, reader->MaxSequence());

    ManifestFileMeta meta;
    meta.file_number = number;
    meta.file_path = file;
    meta.max_seq = reader->MaxSequence();
    meta.level = 0;
    std::error_code size_ec;
    meta.file_size = std::filesystem::file_size(file, size_ec);
    if (size_ec) {
      return Status::IOError("failed to query sst file size: " + file);
    }
    TableIterator iterator(*reader);
    iterator.SeekToFirst();
    if (!iterator.status().ok()) {
      return iterator.status();
    }
    if (iterator.Valid()) {
      meta.smallest_key = iterator.key();
      while (iterator.Valid()) {
        meta.largest_key = iterator.key();
        iterator.Next();
        if (!iterator.status().ok()) {
          return iterator.status();
        }
      }
    }
    sst_files_.push_back({file, std::move(meta)});
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

std::string DBImpl::BuildWalDirPath(const DBOptions& options) {
  if (!options.wal_dir.empty()) {
    return options.wal_dir;
  }
  if (options.db_path.empty()) {
    return {};
  }
  return options.db_path + "/wal";
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

Status DBImpl::CompactSSTFiles(std::unique_lock<std::mutex>& lock) {
  if (compaction_in_progress_) {
    return Status::AlreadyExists("compaction already in progress");
  }
  if (sst_files_.empty()) {
    return Status::OK();
  }

  // Select inputs while holding the DB mutex. The selected paths and metadata
  // form an immutable plan; flushes may append newer files while I/O runs.
  std::vector<size_t> selected_indices;
  uint32_t input_level = 0;
  if (std::none_of(sst_files_.begin(), sst_files_.end(),
                   [](const SSTFileState& state) {
                     return state.metadata.level == 0;
                   })) {
    input_level = 1;
  }
  bool have_range = false;
  bool unknown_input_range = false;
  std::string input_smallest;
  std::string input_largest;
  for (size_t i = 0; i < sst_files_.size(); ++i) {
    const auto& meta = sst_files_[i].metadata;
    if (meta.level != input_level) {
      continue;
    }
    selected_indices.push_back(i);
    if (!meta.HasKeyRange()) {
      unknown_input_range = true;
      continue;
    }
    if (!have_range) {
      input_smallest = meta.smallest_key;
      input_largest = meta.largest_key;
      have_range = true;
    } else {
      input_smallest = std::min(input_smallest, meta.smallest_key);
      input_largest = std::max(input_largest, meta.largest_key);
    }
  }
  if (selected_indices.empty()) {
    return Status::NotFound("no L0 files to compact");
  }
  for (size_t i = 0; i < sst_files_.size(); ++i) {
    const auto& meta = sst_files_[i].metadata;
    if (meta.level != input_level + 1) {
      continue;
    }
    const bool overlaps = unknown_input_range || !have_range ||
                          !meta.HasKeyRange() ||
                          !(meta.largest_key < input_smallest ||
                            meta.smallest_key > input_largest);
    if (overlaps) {
      selected_indices.push_back(i);
    }
  }
  std::sort(selected_indices.begin(), selected_indices.end());

  CompactionPlan plan;
  plan.old_files.reserve(selected_indices.size());
  plan.old_metadata.reserve(selected_indices.size());
  for (size_t index : selected_indices) {
    plan.old_files.push_back(sst_files_[index].file_path);
    plan.old_metadata.push_back(sst_files_[index].metadata);
  }
  // Reserve the output number before releasing the mutex. Gaps are harmless
  // after a failed compaction and prevent names from racing with flushes.
  plan.output_file_number = next_file_number_++;
  plan.output_level = input_level + 1;
  compaction_in_progress_ = true;

  std::vector<ManifestFileMeta> output_metadata;
  const auto compaction_start = std::chrono::steady_clock::now();
  lock.unlock();
  Status status = ExecuteCompaction(plan, &output_metadata);
  lock.lock();

  uint64_t output_bytes = 0;
  for (const auto& meta : output_metadata) output_bytes += meta.file_size;
  if (status.ok()) {
    status = CommitCompaction(plan, std::move(output_metadata));
  }
  if (status.ok()) {
    uint64_t input_bytes = 0;
    for (const auto& meta : plan.old_metadata) input_bytes += meta.file_size;
    compaction_stats_.input_bytes += input_bytes;
    compaction_stats_.output_bytes += output_bytes;
    compaction_stats_.total_duration_us += static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - compaction_start)
            .count());
  }
  compaction_in_progress_ = false;
  compaction_done_cv_.notify_all();
  return status;
}

Status DBImpl::ExecuteCompaction(
    const CompactionPlan& plan,
    std::vector<ManifestFileMeta>* output_metadata) {
  if (output_metadata == nullptr) {
    return Status::InvalidArgument("compaction output is null");
  }
  output_metadata->clear();

  std::error_code ec;
  std::filesystem::create_directories(sst_dir_, ec);
  if (ec) {
    return Status::IOError("failed to create sst dir: " + sst_dir_);
  }

  // Each input iterator owns at most one decoded data block. The heap holds
  // one entry per input, so compaction memory is O(number of input files).
  struct MergeSource {
    std::shared_ptr<const TableReader> reader;
    std::unique_ptr<TableIterator> iterator;
  };
  struct HeapEntry {
    std::string key;
    uint64_t seq = 0;
    uint8_t type = 0;
    uint64_t expires_at_ms = 0;
    std::string value;
    size_t source = 0;
  };
  struct HeapCompare {
    bool operator()(const HeapEntry& lhs, const HeapEntry& rhs) const {
      if (lhs.key != rhs.key) return lhs.key > rhs.key;
      if (lhs.seq != rhs.seq) return lhs.seq < rhs.seq;
      return lhs.source > rhs.source;
    }
  };

  std::vector<MergeSource> sources;
  sources.reserve(plan.old_files.size());
  std::priority_queue<HeapEntry, std::vector<HeapEntry>, HeapCompare> heap;
  for (const auto& file : plan.old_files) {
    MergeSource source;
    Status s = table_cache_->Get(file, &source.reader);
    if (!s.ok()) return s;
    source.iterator = std::make_unique<TableIterator>(*source.reader);
    source.iterator->SeekToFirst();
    if (!source.iterator->status().ok()) return source.iterator->status();
    sources.push_back(std::move(source));
  }

  const auto push_current = [&heap, &sources](size_t source_index) {
    const TableIterator& iterator = *sources[source_index].iterator;
    if (!iterator.Valid()) return;
    HeapEntry entry;
    entry.key = iterator.key();
    entry.seq = iterator.seq();
    entry.type = iterator.type();
    entry.expires_at_ms = iterator.expires_at_ms();
    entry.value = iterator.value();
    entry.source = source_index;
    heap.push(std::move(entry));
  };
  for (size_t i = 0; i < sources.size(); ++i) push_current(i);

  const uint64_t target_size =
      static_cast<uint64_t>(options_.compaction_output_file_size_bytes);
  std::unique_ptr<TableBuilder> builder;
  ManifestFileMeta current_meta;
  bool current_has_entries = false;
  size_t shard_index = 0;
  const auto start_builder = [&]() {
    const std::string path = sst_dir_ + "/.compaction-" +
                             std::to_string(plan.output_file_number) + "-" +
                             std::to_string(shard_index++) + ".sst";
    builder = std::make_unique<TableBuilder>(
        path, options_.sstable_block_size_bytes, options_.bloom_bits_per_key);
    current_meta = ManifestFileMeta();
    current_meta.file_path = path;
    current_meta.level = plan.output_level;
    current_has_entries = false;
  };
  const auto finish_builder = [&]() -> Status {
    if (!builder || !current_has_entries) return Status::OK();
    Status s = builder->Finish();
    if (!s.ok()) return s;
    current_meta.file_size = std::filesystem::file_size(current_meta.file_path, ec);
    if (ec) {
      return Status::IOError("failed to query compacted sst size: " +
                             current_meta.file_path);
    }
    output_metadata->push_back(current_meta);
    builder.reset();
    current_has_entries = false;
    return Status::OK();
  };

  uint64_t compact_max_seq = 0;
  while (!heap.empty()) {
    const std::string key = heap.top().key;
    CompactionEntry latest;
    bool have_latest = false;
    while (!heap.empty() && heap.top().key == key) {
      HeapEntry entry = heap.top();
      heap.pop();
      if (!have_latest || entry.seq > latest.seq) {
        latest.seq = entry.seq;
        latest.type = entry.type;
        latest.expires_at_ms = entry.expires_at_ms;
        latest.value = std::move(entry.value);
        have_latest = true;
      }
      MergeSource& source = sources[entry.source];
      source.iterator->Next();
      if (!source.iterator->status().ok()) return source.iterator->status();
      push_current(entry.source);
    }
    if (!have_latest) continue;

    // Once compaction advances into L2 or deeper, every older overlapping
    // level is part of the input set. With no active snapshots (checked by
    // the caller), a deletion marker or already-expired value can therefore
    // be discarded together with the shadowed history.
    if (plan.output_level >= 2 &&
        (latest.type == 1 || IsExpired(latest.expires_at_ms, NowUnixMillis()))) {
      continue;
    }

    if (!builder) start_builder();
    if (target_size != 0 && current_has_entries &&
        builder->FileSize() >= target_size) {
      Status s = finish_builder();
      if (!s.ok()) return s;
      start_builder();
    }

    Status s = builder->Add(key, latest.seq, latest.type,
                            latest.expires_at_ms, latest.value);
    if (!s.ok()) return s;
    if (!current_has_entries) current_meta.smallest_key = key;
    current_meta.largest_key = key;
    current_meta.max_seq = std::max(current_meta.max_seq, latest.seq);
    compact_max_seq = std::max(compact_max_seq, latest.seq);
    current_has_entries = true;
  }

  Status s = finish_builder();
  if (!s.ok()) return s;
  s = testing::MaybeInjectFailure(testing::FailurePoint::kDuringCompactionOutput);
  if (!s.ok()) return s;
  (void)compact_max_seq;
  return Status::OK();
}

Status DBImpl::CommitCompaction(
    const CompactionPlan& plan,
    std::vector<ManifestFileMeta> output_metadata) {
  if (plan.old_files.size() != plan.old_metadata.size()) {
    return Status::Corruption("compaction plan metadata is out of sync");
  }

  std::vector<size_t> selected_indices;
  selected_indices.reserve(plan.old_files.size());
  for (size_t old = 0; old < plan.old_files.size(); ++old) {
    size_t found = sst_files_.size();
    for (size_t i = 0; i < sst_files_.size(); ++i) {
      if (sst_files_[i].file_path == plan.old_files[old] &&
          sst_files_[i].metadata.file_number ==
              plan.old_metadata[old].file_number) {
        found = i;
        break;
      }
    }
    if (found == sst_files_.size()) {
      return Status::Corruption("compaction input changed while compacting");
    }
    selected_indices.push_back(found);
  }
  std::sort(selected_indices.begin(), selected_indices.end());

  // Convert lock-free temporary outputs into numbered files during the short
  // commit section. This reserves as many file numbers as the split produced,
  // without racing with flush allocations.
  for (auto& meta : output_metadata) {
    const uint64_t file_number = next_file_number_++;
    const std::filesystem::path final_path =
        sst_dir_ + "/" + BuildSSTFileName(file_number);
    std::error_code rename_ec;
    std::filesystem::rename(meta.file_path, final_path, rename_ec);
    if (rename_ec) {
      return Status::IOError("failed to publish compacted sst: " +
                             final_path.string());
    }
    meta.file_number = file_number;
    meta.file_path = final_path.string();
  }

  bool output_added = false;
  std::vector<uint64_t> removed_numbers;
  auto rollback_manifest = [&]() {
    if (!manifest_.IsOpen()) return;
    if (output_added) {
      for (const auto& meta : output_metadata) {
        (void)manifest_.RemoveFile(meta.file_number);
      }
    }
    for (uint64_t removed : removed_numbers) {
      for (const auto& old_meta : plan.old_metadata) {
        if (old_meta.file_number == removed) {
          (void)manifest_.AddFile(old_meta);
          break;
        }
      }
    }
    (void)manifest_.Sync();
  };

  if (manifest_.IsOpen()) {
    Status s;
    for (const auto& meta : output_metadata) {
      s = manifest_.AddFile(meta);
      if (!s.ok()) return s;
    }
    output_added = true;
    s = manifest_.Sync();
    if (!s.ok()) {
      rollback_manifest();
      return s;
    }
    s = testing::MaybeInjectFailure(
        testing::FailurePoint::kAfterCompactionAddBeforeRemove);
    if (!s.ok()) {
      rollback_manifest();
      return s;
    }
    for (const auto& old_meta : plan.old_metadata) {
      s = manifest_.RemoveFile(old_meta.file_number);
      if (!s.ok()) {
        rollback_manifest();
        return s;
      }
      removed_numbers.push_back(old_meta.file_number);
      s = manifest_.Sync();
      if (!s.ok()) {
        rollback_manifest();
        return s;
      }
    }
  }

  for (const auto& old_file : plan.old_files) table_cache_->Evict(old_file);

  std::vector<SSTFileState> remaining_files;
  remaining_files.reserve(sst_files_.size() - selected_indices.size() +
                          output_metadata.size());
  for (size_t i = 0; i < sst_files_.size(); ++i) {
    if (std::binary_search(selected_indices.begin(), selected_indices.end(), i)) {
      continue;
    }
    remaining_files.push_back(std::move(sst_files_[i]));
  }
  for (auto& meta : output_metadata) {
    remaining_files.push_back({meta.file_path, std::move(meta)});
  }
  sst_files_ = std::move(remaining_files);

  for (const auto& old_file : plan.old_files) {
    std::error_code remove_ec;
    (void)std::filesystem::remove(old_file, remove_ec);
  }
  return Status::OK();
}

}  // namespace kv
