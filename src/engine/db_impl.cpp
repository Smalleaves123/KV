#include "kv/engine/db_impl.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

#include "kv/engine/recovery.h"

namespace kv {
namespace {

enum class SSTRecordType : uint8_t {
  kValue = 0,
  kDelete = 1,
};

class SnapshotImpl final : public Snapshot {
 public:
  explicit SnapshotImpl(uint64_t seq) : seq_(seq) {}
  uint64_t sequence() const noexcept override { return seq_; }

 private:
  uint64_t seq_;
};

class OCCTransaction final : public Transaction {
 public:
  OCCTransaction(DBImpl* db, TxnOptions options, uint64_t start_seq)
      : db_(db),
        options_(options),
        start_seq_(start_seq),
        active_(true),
        read_set_(),
        writes_(),
        write_index_() {}

  ~OCCTransaction() override {
    if (active_) {
      (void)Rollback();
    }
  }

  Status Get(const Slice& key, std::string* value) override {
    if (!active_) {
      return Status::InvalidArgument("transaction already finished");
    }
    if (db_ == nullptr) {
      return Status::IOError("transaction db is null");
    }
    Status s = db_->TxnValidateKey(key);
    if (!s.ok()) {
      return s;
    }
    if (value == nullptr) {
      return Status::InvalidArgument("value output pointer is null");
    }

    const std::string key_str = key.ToString();
    auto it = write_index_.find(key_str);
    if (it != write_index_.end()) {
      const auto& op = writes_[it->second];
      if (op.type == WriteBatch::ValueType::kDelete) {
        return Status::NotFound("key deleted");
      }
      *value = op.value;
      return Status::OK();
    }

    s = db_->TxnGetAtSequence(key, start_seq_, value);
    read_set_.insert(key_str);
    return s;
  }

  Status Put(const Slice& key, const Slice& value) override {
    if (!active_) {
      return Status::InvalidArgument("transaction already finished");
    }
    if (db_ == nullptr) {
      return Status::IOError("transaction db is null");
    }
    Status s = db_->TxnValidateKey(key);
    if (!s.ok()) {
      return s;
    }

    WriteBatch::Operation op;
    op.type = WriteBatch::ValueType::kPut;
    op.key = key.ToString();
    op.value = value.ToString();

    auto [it, inserted] = write_index_.insert({op.key, writes_.size()});
    if (inserted) {
      writes_.push_back(std::move(op));
    } else {
      writes_[it->second] = std::move(op);
    }
    return Status::OK();
  }

  Status Delete(const Slice& key) override {
    if (!active_) {
      return Status::InvalidArgument("transaction already finished");
    }
    if (db_ == nullptr) {
      return Status::IOError("transaction db is null");
    }
    Status s = db_->TxnValidateKey(key);
    if (!s.ok()) {
      return s;
    }

    WriteBatch::Operation op;
    op.type = WriteBatch::ValueType::kDelete;
    op.key = key.ToString();
    op.value.clear();

    auto [it, inserted] = write_index_.insert({op.key, writes_.size()});
    if (inserted) {
      writes_.push_back(std::move(op));
    } else {
      writes_[it->second] = std::move(op);
    }
    return Status::OK();
  }

  Status Commit() override {
    if (!active_) {
      return Status::InvalidArgument("transaction already finished");
    }
    if (db_ == nullptr) {
      return Status::IOError("transaction db is null");
    }

    Status s = db_->TxnCommitOCC(options_, start_seq_, read_set_, writes_);
    active_ = false;
    db_->TxnUnregister(this);
    return s;
  }

  Status Rollback() override {
    if (!active_) {
      return Status::InvalidArgument("transaction already finished");
    }
    if (db_ == nullptr) {
      return Status::IOError("transaction db is null");
    }

    active_ = false;
    read_set_.clear();
    writes_.clear();
    write_index_.clear();
    db_->TxnUnregister(this);
    return Status::OK();
  }

 private:
  DBImpl* db_;
  TxnOptions options_;
  uint64_t start_seq_;
  bool active_;
  std::unordered_set<std::string> read_set_;
  std::vector<WriteBatch::Operation> writes_;
  std::unordered_map<std::string, size_t> write_index_;
};

Status RequireOpen(bool is_open) {
  if (!is_open) {
    return Status::IOError("db is not open");
  }
  return Status::OK();
}

void AppendFixed32(std::string* out, uint32_t value) {
  out->push_back(static_cast<char>(value & 0xFF));
  out->push_back(static_cast<char>((value >> 8) & 0xFF));
  out->push_back(static_cast<char>((value >> 16) & 0xFF));
  out->push_back(static_cast<char>((value >> 24) & 0xFF));
}

void AppendFixed64(std::string* out, uint64_t value) {
  for (int i = 0; i < 8; ++i) {
    out->push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
  }
}

uint32_t DecodeFixed32(const char* p) {
  return static_cast<uint32_t>(static_cast<unsigned char>(p[0])) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[1])) << 8) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[2])) << 16) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[3])) << 24);
}

uint64_t DecodeFixed64(const char* p) {
  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value |= (static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i));
  }
  return value;
}

Status WriteSSTRecord(std::ofstream* out,
                      const MemTableEntry& entry,
                      size_t* bytes_written) {
  if (out == nullptr || bytes_written == nullptr) {
    return Status::InvalidArgument("invalid sst record output");
  }

  const SSTRecordType type =
      entry.type == RecordType::kDeletion ? SSTRecordType::kDelete
                                          : SSTRecordType::kValue;

  std::string buffer;
  buffer.reserve(1 + 8 + 4 + 4 + entry.key.size() + entry.value.size());
  buffer.push_back(static_cast<char>(type));
  AppendFixed64(&buffer, entry.seq);
  AppendFixed32(&buffer, static_cast<uint32_t>(entry.key.size()));
  AppendFixed32(&buffer, static_cast<uint32_t>(entry.value.size()));
  buffer.append(entry.key);
  buffer.append(entry.value);

  out->write(buffer.data(), static_cast<std::streamsize>(buffer.size()));
  if (!*out) {
    return Status::IOError("failed to write sst record");
  }

  *bytes_written += buffer.size();
  return Status::OK();
}

Status ReadSSTRecord(std::ifstream* in,
                     SSTRecordType* type,
                     uint64_t* seq,
                     std::string* key,
                     std::string* value) {
  if (in == nullptr || type == nullptr || seq == nullptr || key == nullptr ||
      value == nullptr) {
    return Status::InvalidArgument("invalid sst record input");
  }

  char type_raw = 0;
  in->read(&type_raw, 1);
  if (in->gcount() == 0) {
    in->clear();
    return Status::NotFound("end of sst");
  }
  if (in->gcount() != 1) {
    in->clear();
    return Status::Corruption("truncated sst record type");
  }

  switch (static_cast<uint8_t>(type_raw)) {
    case 0:
      *type = SSTRecordType::kValue;
      break;
    case 1:
      *type = SSTRecordType::kDelete;
      break;
    default:
      return Status::Corruption("unknown sst record type");
  }

  char header_buf[16];
  in->read(header_buf, 16);
  if (in->gcount() != 16) {
    in->clear();
    return Status::Corruption("truncated sst record header");
  }

  *seq = DecodeFixed64(header_buf);
  const uint32_t key_size = DecodeFixed32(header_buf + 8);
  const uint32_t value_size = DecodeFixed32(header_buf + 12);

  key->assign(key_size, '\0');
  value->assign(value_size, '\0');

  if (key_size > 0) {
    in->read(key->data(), static_cast<std::streamsize>(key_size));
    if (in->gcount() != static_cast<std::streamsize>(key_size)) {
      in->clear();
      return Status::Corruption("truncated sst key");
    }
  }

  if (value_size > 0) {
    in->read(value->data(), static_cast<std::streamsize>(value_size));
    if (in->gcount() != static_cast<std::streamsize>(value_size)) {
      in->clear();
      return Status::Corruption("truncated sst value");
    }
  }

  return Status::OK();
}

bool ParseSSTFileNumber(const std::filesystem::path& path, uint64_t* number) {
  if (number == nullptr) {
    return false;
  }

  if (path.extension() != ".sst") {
    return false;
  }

  const std::string stem = path.stem().string();
  if (stem.empty()) {
    return false;
  }

  uint64_t value = 0;
  for (char c : stem) {
    if (c < '0' || c > '9') {
      return false;
    }
    value = value * 10 + static_cast<uint64_t>(c - '0');
  }

  *number = value;
  return true;
}

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
      cache_() {}

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
        options_.cache_policy, options_.cache_capacity, options_.cache_default_ttl_ms);
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

  Status s = GetFromMemTableAt(key, read_seq, value);
  if (s.ok()) {
    return s;
  }
  if (!s.IsNotFound()) {
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
    Status s;

    if (op.type == WriteBatch::ValueType::kPut) {
      s = wal_writer_.AppendPut(seq, op.key, op.value);
      if (!s.ok()) {
        return s;
      }
      s = memtable_.Put(seq, op.key, op.value);
    } else {
      s = wal_writer_.AppendDelete(seq, op.key);
      if (!s.ok()) {
        return s;
      }
      s = memtable_.Delete(seq, op.key);
    }

    if (!s.ok()) {
      return s;
    }

    latest_key_seq_[op.key] = seq;
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
  auto occ = std::make_unique<OCCTransaction>(this, options, start_seq);
  RegisterTransaction(occ.get());
  *txn = std::move(occ);
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
  auto snapshot = std::make_unique<SnapshotImpl>(seq);
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
  Status s = wal_writer_.AppendPut(seq, key, value);
  if (!s.ok()) {
    return s;
  }

  if (ShouldSync(options)) {
    s = wal_writer_.Sync();
    if (!s.ok()) {
      return s;
    }
  }

  s = memtable_.Put(seq, key, value);
  if (!s.ok()) {
    return s;
  }
  latest_key_seq_[key.ToString()] = seq;
  return Status::OK();
}

Status DBImpl::ApplyDelete(uint64_t seq,
                           const WriteOptions& options,
                           const Slice& key) {
  Status s = wal_writer_.AppendDelete(seq, key);
  if (!s.ok()) {
    return s;
  }

  if (ShouldSync(options)) {
    s = wal_writer_.Sync();
    if (!s.ok()) {
      return s;
    }
  }

  s = memtable_.Delete(seq, key);
  if (!s.ok()) {
    return s;
  }
  latest_key_seq_[key.ToString()] = seq;
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

  const std::string sst_path = sst_dir_ + "/" + BuildSSTFileName(next_file_number_);
  const uint64_t file_number = next_file_number_;

  std::ofstream out(sst_path, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) {
    return Status::IOError("failed to open sst file: " + sst_path);
  }

  auto it = memtable_.NewIterator();
  size_t bytes_written = 0;
  uint64_t max_flushed_seq = 0;

  for (it.SeekToFirst(); it.Valid(); it.Next()) {
    const auto& entry = it.entry();
    max_flushed_seq = std::max(max_flushed_seq, entry.seq);

    Status s = WriteSSTRecord(&out, entry, &bytes_written);
    if (!s.ok()) {
      out.close();
      return s;
    }
  }

  out.flush();
  if (!out) {
    out.close();
    return Status::IOError("failed to flush sst file: " + sst_path);
  }

  out.close();
  if (!out) {
    return Status::IOError("failed to close sst file: " + sst_path);
  }

  if (bytes_written == 0) {
    std::filesystem::remove(sst_path, ec);
    return Status::NotFound("no visible entries to flush");
  }

  if (manifest_.IsOpen()) {
    ManifestFileMeta meta;
    meta.file_number = file_number;
    meta.file_path = sst_path;
    meta.max_seq = max_flushed_seq;
    Status s = manifest_.AddFile(meta);
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
                                 std::string* value) const {
  const std::string target = key.ToString();
  auto it = memtable_.NewIterator();
  it.Seek(key);

  while (it.Valid()) {
    const auto& entry = it.entry();
    if (entry.key != target) {
      break;
    }

    if (entry.seq <= read_seq) {
      if (entry.type == RecordType::kDeletion) {
        return Status::NotFound("key deleted");
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
                                 std::string* value) const {
  const std::string target = key.ToString();
  const bool allow_cache =
      cache_ != nullptr && read_seq == std::numeric_limits<uint64_t>::max();

  for (auto it = sst_files_.rbegin(); it != sst_files_.rend(); ++it) {
    const std::string cache_key = BuildSSTCacheKey(*it, target);
    if (allow_cache && cache_->Get(cache_key, value)) {
      return Status::OK();
    }

    std::ifstream in(*it, std::ios::binary);
    if (!in.is_open()) {
      return Status::IOError("failed to open sst file: " + *it);
    }

    while (true) {
      SSTRecordType type;
      uint64_t seq = 0;
      std::string entry_key;
      std::string entry_value;

      Status s = ReadSSTRecord(&in, &type, &seq, &entry_key, &entry_value);
      if (s.IsNotFound()) {
        break;
      }
      if (!s.ok()) {
        return s;
      }

      if (entry_key != target) {
        continue;
      }

      if (seq > read_seq) {
        continue;
      }

      if (type == SSTRecordType::kDelete) {
        return Status::NotFound("key deleted");
      }

      *value = std::move(entry_value);
      if (allow_cache) {
        cache_->Put(cache_key, *value);
      }
      return Status::OK();
    }
  }

  return Status::NotFound("key not found");
}

Status DBImpl::GetAtSequence(const Slice& key,
                             uint64_t read_seq,
                             std::string* value) const {
  Status validate_status = ValidateKey(key);
  if (!validate_status.ok()) {
    return validate_status;
  }
  if (value == nullptr) {
    return Status::InvalidArgument("value output pointer is null");
  }

  Status s = GetFromMemTableAt(key, read_seq, value);
  if (s.ok()) {
    return s;
  }
  if (!s.IsNotFound()) {
    return s;
  }
  return GetFromSSTFilesAt(key, read_seq, value);
}

Status DBImpl::RebuildLatestKeySeqIndex() {
  latest_key_seq_.clear();

  for (const auto& file : sst_files_) {
    std::ifstream in(file, std::ios::binary);
    if (!in.is_open()) {
      return Status::IOError("failed to open sst file: " + file);
    }

    while (true) {
      SSTRecordType type;
      uint64_t seq = 0;
      std::string key;
      std::string value;
      Status s = ReadSSTRecord(&in, &type, &seq, &key, &value);
      if (s.IsNotFound()) {
        break;
      }
      if (!s.ok()) {
        return s;
      }
      auto it = latest_key_seq_.find(key);
      if (it == latest_key_seq_.end() || seq > it->second) {
        latest_key_seq_[key] = seq;
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
    Status s;
    if (op.type == WriteBatch::ValueType::kPut) {
      s = wal_writer_.AppendPut(seq, op.key, op.value);
      if (!s.ok()) {
        return s;
      }
      s = memtable_.Put(seq, op.key, op.value);
    } else {
      s = wal_writer_.AppendDelete(seq, op.key);
      if (!s.ok()) {
        return s;
      }
      s = memtable_.Delete(seq, op.key);
    }
    if (!s.ok()) {
      return s;
    }

    latest_key_seq_[op.key] = seq;
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

    uint64_t number = 0;
    if (!ParseSSTFileNumber(entry.path(), &number)) {
      continue;
    }

    files.push_back({number, entry.path().string()});
  }

  std::sort(files.begin(), files.end(), [](const auto& a, const auto& b) {
    return a.first < b.first;
  });

  for (const auto& [number, file] : files) {
    (void)number;
    sst_files_.push_back(file);

    std::ifstream in(file, std::ios::binary);
    if (!in.is_open()) {
      return Status::IOError("failed to open sst file: " + file);
    }

    while (true) {
      SSTRecordType type;
      uint64_t seq = 0;
      std::string key;
      std::string value;
      Status s = ReadSSTRecord(&in, &type, &seq, &key, &value);
      if (s.IsNotFound()) {
        break;
      }
      if (!s.ok()) {
        return s;
      }
      *max_seq = std::max(*max_seq, seq);
    }
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

}  // namespace kv
