#include "kv/wal/wal_manager.h"

#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <sstream>

#include "kv/wal/wal_writer.h"
#include "kv/wal/wal_reader.h"

namespace kv {
namespace {

constexpr size_t kLogNumberWidth = 20;

bool ParseLogNumber(const std::filesystem::path& path, uint64_t* number) {
  if (number == nullptr || path.extension() != ".wal") {
    return false;
  }

  const std::string stem = path.stem().string();
  if (stem.size() != kLogNumberWidth) {
    return false;
  }

  uint64_t value = 0;
  for (char c : stem) {
    if (c < '0' || c > '9') {
      return false;
    }
    const uint64_t digit = static_cast<uint64_t>(c - '0');
    if (value > (std::numeric_limits<uint64_t>::max() - digit) / 10) {
      return false;
    }
    value = value * 10 + digit;
  }
  if (value == 0) {
    return false;
  }

  *number = value;
  return true;
}

Status ReadMaxSequence(const std::string& path, uint64_t* max_sequence) {
  if (max_sequence == nullptr) {
    return Status::InvalidArgument("max sequence output is null");
  }
  *max_sequence = 0;

  WALReader reader;
  Status s = reader.Open(path);
  if (!s.ok()) {
    return s;
  }

  while (true) {
    LogRecord record;
    s = reader.ReadNext(&record);
    if (s.IsNotFound()) {
      break;
    }
    if (!s.ok()) {
      (void)reader.Close();
      return s;
    }
    *max_sequence = std::max(*max_sequence, record.seq);
  }

  const uint64_t consumed_bytes = reader.offset();
  s = reader.Close();
  if (!s.ok()) {
    return s;
  }

  std::error_code ec;
  const auto file_size = std::filesystem::file_size(path, ec);
  if (ec) {
    return Status::IOError("failed to query wal segment size: " + path);
  }
  if (file_size != consumed_bytes) {
    return Status::Corruption("truncated closed wal segment: " + path);
  }

  return Status::OK();
}

}  // namespace

WALManager::WALManager()
    : options_(),
      active_log_path_(),
      writer_(std::make_unique<WALWriter>()),
      next_log_number_(1),
      open_(false) {}

WALManager::~WALManager() {
  (void)Close();
}

Status WALManager::Open(const WALOptions& options) {
  if (open_) {
    return Status::AlreadyExists("wal manager already open");
  }
  if (options.wal_dir.empty()) {
    return Status::InvalidArgument("wal dir is empty");
  }
  if (options.max_log_file_size == 0) {
    return Status::InvalidArgument("max log file size must be greater than 0");
  }
  options_ = options;

  std::error_code ec;
  std::filesystem::create_directories(options_.wal_dir, ec);
  if (ec) {
    return Status::IOError("failed to create wal dir: " + options_.wal_dir);
  }

  std::vector<std::string> existing;
  Status s = ListLogs(&existing);
  if (!s.ok()) {
    return s;
  }
  uint64_t max_log_number = 0;
  for (const auto& path : existing) {
    uint64_t log_number = 0;
    if (!ParseLogNumber(path, &log_number)) {
      return Status::Corruption("invalid wal segment name: " + path);
    }
    max_log_number = std::max(max_log_number, log_number);
  }
  next_log_number_ = max_log_number + 1;

  return RollToNewLog();
}

Status WALManager::Close() {
  if (!open_) {
    return Status::OK();
  }
  if (writer_ != nullptr && writer_->IsOpen()) {
    Status s = writer_->Close();
    if (!s.ok()) {
      return s;
    }
  }
  open_ = false;
  active_log_path_.clear();
  return Status::OK();
}

Status WALManager::AppendPut(uint64_t seq, const std::string& key, const std::string& value) {
  if (!open_ || writer_ == nullptr) {
    return Status::IOError("wal manager not open");
  }
  Status s = writer_->AppendPut(seq, key, value);
  if (!s.ok()) {
    return s;
  }
  if (options_.sync_on_write) {
    s = writer_->Sync();
    if (!s.ok()) {
      return s;
    }
  }
  if (writer_->file_size() >= options_.max_log_file_size) {
    return RollToNewLog();
  }
  return Status::OK();
}

Status WALManager::AppendPutWithTTL(uint64_t seq,
                                    const std::string& key,
                                    const std::string& value,
                                    uint64_t expires_at_ms) {
  if (!open_ || writer_ == nullptr) {
    return Status::IOError("wal manager not open");
  }
  Status s = writer_->AppendPutWithTTL(seq, key, value, expires_at_ms);
  if (!s.ok()) {
    return s;
  }
  if (options_.sync_on_write) {
    s = writer_->Sync();
    if (!s.ok()) {
      return s;
    }
  }
  if (writer_->file_size() >= options_.max_log_file_size) {
    return RollToNewLog();
  }
  return Status::OK();
}

Status WALManager::AppendDelete(uint64_t seq, const std::string& key) {
  if (!open_ || writer_ == nullptr) {
    return Status::IOError("wal manager not open");
  }
  Status s = writer_->AppendDelete(seq, key);
  if (!s.ok()) {
    return s;
  }
  if (options_.sync_on_write) {
    s = writer_->Sync();
    if (!s.ok()) {
      return s;
    }
  }
  if (writer_->file_size() >= options_.max_log_file_size) {
    return RollToNewLog();
  }
  return Status::OK();
}

Status WALManager::Sync() {
  if (!open_ || writer_ == nullptr) {
    return Status::IOError("wal manager not open");
  }
  return writer_->Sync();
}

Status WALManager::RemoveLogsUpTo(uint64_t max_sequence) {
  if (!open_) {
    return Status::IOError("wal manager not open");
  }

  std::vector<std::string> logs;
  Status s = ListLogs(&logs);
  if (!s.ok()) {
    return s;
  }

  for (const auto& path : logs) {
    if (std::filesystem::path(path) == std::filesystem::path(active_log_path_)) {
      continue;
    }

    uint64_t log_max_sequence = 0;
    s = ReadMaxSequence(path, &log_max_sequence);
    if (!s.ok()) {
      return s;
    }
    if (log_max_sequence > max_sequence) {
      continue;
    }

    std::error_code ec;
    if (!std::filesystem::remove(path, ec) || ec) {
      return Status::IOError("failed to remove obsolete wal segment: " + path);
    }
  }

  return Status::OK();
}

Status WALManager::ListLogs(std::vector<std::string>* out) const {
  return ListLogs(options_.wal_dir, out);
}

Status WALManager::ListLogs(const std::string& wal_dir,
                            std::vector<std::string>* out) {
  if (out == nullptr) {
    return Status::InvalidArgument("null output");
  }
  out->clear();
  if (wal_dir.empty()) {
    return Status::InvalidArgument("wal dir is empty");
  }

  std::error_code ec;
  if (!std::filesystem::exists(wal_dir, ec)) {
    if (ec) {
      return Status::IOError("failed to query wal dir: " + wal_dir);
    }
    return Status::OK();
  }
  if (ec) {
    return Status::IOError("failed to query wal dir: " + wal_dir);
  }

  for (const auto& e : std::filesystem::directory_iterator(wal_dir, ec)) {
    if (ec) {
      return Status::IOError("failed to list wal dir: " + wal_dir);
    }
    if (!e.is_regular_file()) {
      continue;
    }
    uint64_t log_number = 0;
    if (!ParseLogNumber(e.path(), &log_number)) {
      continue;
    }
    out->push_back(e.path().string());
  }
  std::sort(out->begin(), out->end());
  return Status::OK();
}

Status WALManager::RollToNewLog() {
  if (writer_ == nullptr) {
    writer_ = std::make_unique<WALWriter>();
  }
  if (writer_->IsOpen()) {
    Status s = writer_->Sync();
    if (!s.ok()) {
      return s;
    }
    s = writer_->Close();
    if (!s.ok()) {
      return s;
    }
  }

  std::ostringstream oss;
  oss << options_.wal_dir << "/" << std::setw(20) << std::setfill('0')
      << next_log_number_ << ".wal";
  active_log_path_ = oss.str();

  Status s = writer_->Open(active_log_path_, true);
  if (!s.ok()) {
    return s;
  }
  ++next_log_number_;
  open_ = true;
  return Status::OK();
}

}  // namespace kv
