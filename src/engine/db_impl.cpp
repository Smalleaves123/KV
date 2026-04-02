#include "kv/engine/db_impl.h"

#include <filesystem>
#include <mutex>
#include <utility>

#include "kv/engine/recovery.h"

namespace kv {
namespace {

Status RequireOpen(bool is_open) {
  if (!is_open) {
    return Status::IOError("db is not open");
  }
  return Status::OK();
}

}  // namespace

DBImpl::DBImpl(DBOptions options)
    : options_(std::move(options)),
      wal_path_(),
      memtable_(),
      wal_writer_(),
      next_seq_(1),
      open_(false) {}

DBImpl::~DBImpl() {
  (void)Close();
}

Status DBImpl::Init() {
  std::lock_guard<std::mutex> lk(mu_);

  if (open_) {
    return Status::AlreadyExists("db is already open");
  }

  wal_path_ = BuildWalPath(options_);
  if (wal_path_.empty()) {
    return Status::InvalidArgument("wal path is empty");
  }

  const std::filesystem::path wal_fs_path(wal_path_);
  std::error_code ec;
  const bool wal_exists = std::filesystem::exists(wal_fs_path, ec);
  if (ec) {
    return Status::IOError("failed to query wal path: " + wal_path_);
  }

  if (!wal_exists && !options_.create_if_missing) {
    return Status::NotFound("wal file does not exist: " + wal_path_);
  }

  uint64_t max_seq = 0;
  if (wal_exists) {
    Status s = Recovery::ReplayWAL(wal_path_, &memtable_, &max_seq);
    if (!s.ok()) {
      return s;
    }
  }

  Status s = wal_writer_.Open(wal_path_, true);
  if (!s.ok()) {
    return s;
  }

  next_seq_ = max_seq + 1;
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
  return Status::OK();
}

Status DBImpl::Get(const ReadOptions& /*options*/,
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

  return memtable_.Get(key, value);
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
  return Status::OK();
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

  return memtable_.Put(seq, key, value);
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

  return memtable_.Delete(seq, key);
}

bool DBImpl::ShouldSync(const WriteOptions& options) const noexcept {
  return options.sync || options_.sync_on_write;
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

}  // namespace kv
