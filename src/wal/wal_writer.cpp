#include "kv/wal/wal_writer.h"

#include <filesystem>
#include <ios>
#include <system_error>

#include "kv/common/file_compat.h"
namespace kv {
namespace {

std::ios::openmode BuildOpenMode(bool append) {
  std::ios::openmode mode = std::ios::binary | std::ios::out;
  mode |= append ? std::ios::app : std::ios::trunc;
  return mode;
}

Status EnsureParentDirectory(const std::filesystem::path& path) {
  const auto parent = path.parent_path();
  if (parent.empty()) {
    return Status::OK();
  }

  std::error_code ec;
  std::filesystem::create_directories(parent, ec);
  if (ec) {
    return Status::IOError("failed to create wal directory: " + parent.string());
  }

  return Status::OK();
}

Status GetInitialFileSize(const std::filesystem::path& path,
                          bool append,
                          uint64_t* size) {
  if (size == nullptr) {
    return Status::InvalidArgument("size output is null");
  }

  *size = 0;
  if (!append) {
    return Status::OK();
  }

  std::error_code ec;
  const bool exists = std::filesystem::exists(path, ec);
  if (ec) {
    return Status::IOError("failed to query wal file existence: " + path.string());
  }

  if (!exists) {
    return Status::OK();
  }

  const auto bytes = std::filesystem::file_size(path, ec);
  if (ec) {
    return Status::IOError("failed to query wal file size: " + path.string());
  }

  *size = static_cast<uint64_t>(bytes);
  return Status::OK();
}

}  // namespace

WALWriter::WALWriter() : stream_(), file_path_(), file_size_(0), sync_fd_(-1) {}

WALWriter::~WALWriter() {
  (void)Close();
}

Status WALWriter::Open(const std::string& file_path, bool append) {
  if (file_path.empty()) {
    return Status::InvalidArgument("wal file path is empty");
  }

  if (sync_fd_ >= 0) {
    (void)platform::CloseFile(sync_fd_);
    sync_fd_ = -1;
  }

  if (stream_.is_open()) {
    Status s = Close();
    if (!s.ok()) {
      return s;
    }
  }

  stream_.clear();

  const std::filesystem::path path(file_path);

  Status s = EnsureParentDirectory(path);
  if (!s.ok()) {
    return s;
  }

  uint64_t initial_size = 0;
  s = GetInitialFileSize(path, append, &initial_size);
  if (!s.ok()) {
    return s;
  }

  stream_.open(file_path, BuildOpenMode(append));
  if (!stream_.is_open()) {
    stream_.clear();
    return Status::IOError("failed to open wal file: " + file_path);
  }

  // Open a raw fd for fsync (ofstream does not expose its file descriptor).
  sync_fd_ = platform::OpenSyncFile(file_path, append);
  if (sync_fd_ < 0) {
    const std::string err = platform::FileErrorString();
    stream_.close();
    stream_.clear();
    return Status::IOError("failed to open wal fd for sync: " + file_path + ": " + err);
  }

  file_path_ = file_path;
  file_size_ = initial_size;
  return Status::OK();
}

Status WALWriter::Close() {
  if (sync_fd_ >= 0) {
    (void)platform::CloseFile(sync_fd_);
    sync_fd_ = -1;
  }

  if (!stream_.is_open()) {
    stream_.clear();
    return Status::OK();
  }

  stream_.flush();
  if (!stream_) {
    stream_.close();
    stream_.clear();
    return Status::IOError("failed to flush wal file before close");
  }

  stream_.close();
  if (stream_.fail()) {
    stream_.clear();
    return Status::IOError("failed to close wal file");
  }

  stream_.clear();
  return Status::OK();
}

Status WALWriter::Append(const LogRecord& record) {
  if (!stream_.is_open()) {
    return Status::IOError("wal file is not open");
  }

  std::string encoded;
  Status s = LogRecordCodec::Encode(record, &encoded);
  if (!s.ok()) {
    return s;
  }

  stream_.write(encoded.data(), static_cast<std::streamsize>(encoded.size()));
  if (!stream_) {
    return Status::IOError("failed to append wal record");
  }

  file_size_ += static_cast<uint64_t>(encoded.size());
  return Status::OK();
}

Status WALWriter::AppendPut(uint64_t seq, const Slice& key, const Slice& value) {
  LogRecord record;
  record.type = LogRecordType::kPut;
  record.seq = seq;
  record.key = key.ToString();
  record.value = value.ToString();
  return Append(record);
}

Status WALWriter::AppendDelete(uint64_t seq, const Slice& key) {
  LogRecord record;
  record.type = LogRecordType::kDelete;
  record.seq = seq;
  record.key = key.ToString();
  record.value.clear();
  return Append(record);
}

Status WALWriter::Sync() {
  if (!stream_.is_open()) {
    return Status::IOError("wal file is not open");
  }

  stream_.flush();
  if (!stream_) {
    return Status::IOError("failed to flush wal file");
  }

  if (sync_fd_ >= 0) {
    if (platform::SyncFile(sync_fd_) != 0) {
      return Status::IOError("file sync failed: " +
                             platform::FileErrorString());
    }
  }

  return Status::OK();
}

bool WALWriter::IsOpen() const noexcept {
  return stream_.is_open();
}

uint64_t WALWriter::file_size() const noexcept {
  return file_size_;
}

const std::string& WALWriter::file_path() const noexcept {
  return file_path_;
}

}  // namespace kv
