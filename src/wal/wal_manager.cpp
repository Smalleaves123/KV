#include "kv/wal/wal_manager.h"

#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <sstream>

#include "kv/wal/wal_writer.h"

namespace kv {

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
  options_ = options;
  if (open_) {
    return Status::AlreadyExists("wal manager already open");
  }

  std::error_code ec;
  std::filesystem::create_directories(options_.wal_dir, ec);
  if (ec) {
    return Status::IOError("failed to create wal dir: " + options_.wal_dir);
  }

  next_log_number_ = 1;
  std::vector<std::string> existing;
  Status s = ListLogs(&existing);
  if (!s.ok()) {
    return s;
  }
  if (!existing.empty()) {
    next_log_number_ = static_cast<uint64_t>(existing.size()) + 1;
  }

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

Status WALManager::ListLogs(std::vector<std::string>* out) const {
  if (out == nullptr) {
    return Status::InvalidArgument("null output");
  }
  out->clear();

  std::error_code ec;
  if (!std::filesystem::exists(options_.wal_dir, ec)) {
    return Status::OK();
  }

  for (const auto& e : std::filesystem::directory_iterator(options_.wal_dir, ec)) {
    if (ec) {
      break;
    }
    if (!e.is_regular_file()) {
      continue;
    }
    if (e.path().extension() != ".wal") {
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
    Status s = writer_->Close();
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
