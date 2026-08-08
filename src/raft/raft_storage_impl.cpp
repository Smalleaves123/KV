#include "kv/raft/raft_storage_impl.h"

#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <system_error>

#include "kv/common/encoding.h"
#include "kv/common/file_compat.h"

namespace kv {
namespace raft {
namespace {

constexpr uint32_t kMaxRaftLogEntryBytes = 10 * 1024 * 1024;

Status SyncPath(const std::string& path) {
  const int fd = platform::OpenSyncFile(path, true);
  if (fd < 0) {
    return Status::IOError("failed to open raft file for sync: " + path +
                           ": " + platform::FileErrorString());
  }
  const int sync_result = platform::SyncFile(fd);
  const std::string sync_error =
      sync_result == 0 ? "" : platform::FileErrorString();
  const int close_result = platform::CloseFile(fd);
  if (sync_result != 0) {
    return Status::IOError("failed to sync raft file: " + path + ": " +
                           sync_error);
  }
  if (close_result != 0) {
    return Status::IOError("failed to close raft file: " + path + ": " +
                           platform::FileErrorString());
  }
  return Status::OK();
}

Status AtomicWriteFile(const std::string& path, const std::string& data) {
  const std::filesystem::path fs_path(path);
  const std::filesystem::path parent = fs_path.parent_path();
  std::error_code ec;
  if (!parent.empty()) {
    std::filesystem::create_directories(parent, ec);
    if (ec) {
      return Status::IOError("failed to create raft directory: " +
                             parent.string());
    }
  }

  const std::string temporary_path = path + ".tmp";
  {
    std::ofstream out(temporary_path, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) {
      return Status::IOError("failed to open temporary raft file: " +
                             temporary_path);
    }
    out.write(data.data(), static_cast<std::streamsize>(data.size()));
    out.close();
    if (!out) {
      return Status::IOError("failed to write temporary raft file: " +
                             temporary_path);
    }
  }

  Status s = SyncPath(temporary_path);
  if (!s.ok()) {
    return s;
  }
  if (platform::ReplaceFile(temporary_path, path) != 0) {
    return Status::IOError("failed to publish raft file: " + path + ": " +
                           platform::FileErrorString());
  }
  if (!parent.empty() && platform::SyncDirectory(parent.string()) != 0) {
    return Status::IOError("failed to sync raft directory: " +
                           parent.string() + ": " + platform::FileErrorString());
  }
  return Status::OK();
}

Status EncodeLogEntry(const LogEntry& entry, std::string* out) {
  if (out == nullptr) {
    return Status::InvalidArgument("raft log output is null");
  }
  if (entry.index == 0 || entry.data.size() > kMaxRaftLogEntryBytes) {
    return Status::InvalidArgument("invalid raft log entry");
  }
  out->clear();
  out->reserve(24 + entry.data.size());
  EncodeFixed32(out, static_cast<uint32_t>(20 + entry.data.size()));
  EncodeFixed64(out, entry.index);
  EncodeFixed64(out, entry.term);
  EncodeFixed32(out, static_cast<uint32_t>(entry.data.size()));
  out->append(entry.data);
  return Status::OK();
}

}  // namespace

FileRaftStorage::FileRaftStorage(const std::string& dir_path)
    : dir_path_(dir_path),
      state_path_(dir_path + "/raft_state"),
      snapshot_meta_path_(dir_path + "/snapshot_meta"),
      members_path_(dir_path + "/raft_members"),
      log_path_(dir_path + "/raft_log"),
      hard_state_{},
      snapshot_meta_{},
      members_(),
      first_index_(1),
      last_index_(0),
      initial_status_(Status::OK()) {
  std::error_code ec;
  std::filesystem::create_directories(dir_path_, ec);
  if (ec) {
    initial_status_ = Status::IOError("failed to create raft directory: " +
                                      dir_path_);
    return;
  }

  // Load hard state
  std::ifstream sf(state_path_, std::ios::binary);
  if (sf.is_open()) {
    char buf[32];
    sf.read(buf, 32);
    if (sf.gcount() == 32) {
      hard_state_.term = DecodeFixed64(buf);
      hard_state_.vote_for = DecodeFixed64(buf + 8);
      hard_state_.commit_index = DecodeFixed64(buf + 16);
      hard_state_.applied_index = DecodeFixed64(buf + 24);
    } else if (sf.gcount() != 0) {
      initial_status_ = Status::Corruption("truncated raft hard state");
      return;
    }
    sf.close();
  }

  std::ifstream meta_file(snapshot_meta_path_, std::ios::binary);
  if (meta_file.is_open()) {
    char buf[16];
    meta_file.read(buf, sizeof(buf));
    if (meta_file.gcount() == static_cast<std::streamsize>(sizeof(buf))) {
      snapshot_meta_.last_included_index = DecodeFixed64(buf);
      snapshot_meta_.last_included_term = DecodeFixed64(buf + 8);
    }
  }

  std::ifstream members_file(members_path_, std::ios::binary);
  if (members_file.is_open()) {
    char count_buf[4];
    members_file.read(count_buf, sizeof(count_buf));
    if (members_file.gcount() == static_cast<std::streamsize>(sizeof(count_buf))) {
      const uint32_t count = DecodeFixed32(count_buf);
      if (count <= 10000) {
        members_.reserve(count);
        for (uint32_t i = 0; i < count; ++i) {
          char id_buf[8];
          members_file.read(id_buf, sizeof(id_buf));
          if (members_file.gcount() != static_cast<std::streamsize>(sizeof(id_buf))) {
            members_.clear();
            break;
          }
          members_.push_back(DecodeFixed64(id_buf));
        }
      }
    }
  }

  // Load log index
  LoadIndex();
}

Status FileRaftStorage::InitialStatus() const { return initial_status_; }

HardState FileRaftStorage::InitialState() const {
  return hard_state_;
}

Status FileRaftStorage::SaveHardState(const HardState& state) {
  std::string data;
  data.reserve(32);
  EncodeFixed64(&data, state.term);
  EncodeFixed64(&data, state.vote_for);
  EncodeFixed64(&data, state.commit_index);
  EncodeFixed64(&data, state.applied_index);

  Status s = AtomicWriteFile(state_path_, data);
  if (s.ok()) {
    hard_state_ = state;
  }
  return s;
}

std::vector<uint64_t> FileRaftStorage::InitialMembers() const {
  return members_;
}

Status FileRaftStorage::SaveMembers(const std::vector<uint64_t>& members) {
  if (members.empty() || members.size() > 10000) {
    return Status::InvalidArgument("invalid raft membership");
  }

  std::string data;
  data.reserve(4 + members.size() * 8);
  EncodeFixed32(&data, static_cast<uint32_t>(members.size()));
  for (uint64_t member : members) EncodeFixed64(&data, member);

  Status s = AtomicWriteFile(members_path_, data);
  if (s.ok()) {
    members_ = members;
  }
  return s;
}

RaftSnapshotMeta FileRaftStorage::SnapshotMeta() const {
  return snapshot_meta_;
}

Status FileRaftStorage::SaveSnapshotMeta(const RaftSnapshotMeta& meta) {
  std::string data;
  data.reserve(16);
  EncodeFixed64(&data, meta.last_included_index);
  EncodeFixed64(&data, meta.last_included_term);

  Status s = AtomicWriteFile(snapshot_meta_path_, data);
  if (s.ok()) {
    snapshot_meta_ = meta;
  }
  return s;
}

void FileRaftStorage::LoadIndex() {
  if (!initial_status_.ok()) {
    return;
  }
  index_offset_.clear();
  first_index_ = snapshot_meta_.last_included_index + 1;
  last_index_ = snapshot_meta_.last_included_index;

  std::ifstream lf(log_path_, std::ios::binary);
  if (!lf.is_open()) return;

  uint64_t offset = 0;
  while (true) {
    char size_buf[4];
    lf.read(size_buf, 4);
    if (lf.gcount() == 0) break;
    if (lf.gcount() != 4) {
      initial_status_ = Status::Corruption("truncated raft log length");
      return;
    }

    uint32_t rec_size = DecodeFixed32(size_buf);
    if (rec_size < 20 || rec_size > 20 + kMaxRaftLogEntryBytes) {
      initial_status_ = Status::Corruption("invalid raft log record size");
      return;
    }

    char rec_buf[20];  // index + term + datalen
    lf.read(rec_buf, 20);
    if (lf.gcount() != 20) {
      initial_status_ = Status::Corruption("truncated raft log header");
      return;
    }

    uint64_t index = DecodeFixed64(rec_buf);
    uint32_t data_len = DecodeFixed32(rec_buf + 16);
    if (data_len != rec_size - 20) {
      initial_status_ = Status::Corruption("invalid raft log payload size");
      return;
    }

    lf.seekg(static_cast<std::streamoff>(data_len), std::ios::cur);
    if (!lf) {
      initial_status_ = Status::Corruption("truncated raft log payload");
      return;
    }

    if (index <= snapshot_meta_.last_included_index) {
      offset += 4 + 20 + data_len;
      continue;
    }

    if (index == 0 || (last_index_ != snapshot_meta_.last_included_index &&
                       index != last_index_ + 1)) {
      initial_status_ = Status::Corruption("non-contiguous raft log index");
      return;
    }
    index_offset_[index] = offset;
    if (first_index_ == snapshot_meta_.last_included_index + 1 &&
        last_index_ == snapshot_meta_.last_included_index) {
      first_index_ = index;
    }
    last_index_ = index;

    offset += 4 + 20 + data_len;
  }
}

std::vector<LogEntry> FileRaftStorage::Entries(uint64_t low,
                                               uint64_t high) const {
  std::vector<LogEntry> result;
  if (low > high) return result;

  for (uint64_t idx = low; idx <= high && idx <= last_index_; ++idx) {
    auto it = index_offset_.find(idx);
    if (it == index_offset_.end()) break;
    LogEntry entry;
    if (!ReadEntryAt(it->second, &entry).ok()) {
      return {};
    }
    result.push_back(std::move(entry));
  }
  return result;
}

uint64_t FileRaftStorage::Term(uint64_t index) const {
  if (index < first_index_ || index > last_index_) return 0;
  auto it = index_offset_.find(index);
  if (it == index_offset_.end()) return 0;
  LogEntry entry;
  return ReadEntryAt(it->second, &entry).ok() ? entry.term : 0;
}

uint64_t FileRaftStorage::FirstIndex() const {
  return first_index_;
}

uint64_t FileRaftStorage::LastIndex() const {
  return last_index_;
}

Status FileRaftStorage::Append(const std::vector<LogEntry>& entries) {
  if (entries.empty()) return Status::OK();
  if (!initial_status_.ok()) return initial_status_;
  for (const auto& entry : entries) {
    Status s = WriteEntry(entry);
    if (!s.ok()) return s;
  }
  return Status::OK();
}

Status FileRaftStorage::TruncatePrefix(uint64_t index) {
  if (index <= first_index_) return Status::OK();

  // Remove entries before 'index' from the offset map
  for (auto it = index_offset_.begin(); it != index_offset_.end();) {
    if (it->first < index) {
      it = index_offset_.erase(it);
    } else {
      break;
    }
  }

  if (index > last_index_) {
    last_index_ = snapshot_meta_.last_included_index;
    first_index_ = snapshot_meta_.last_included_index + 1;
  } else if (!index_offset_.empty()) {
    first_index_ = index;
  } else {
    last_index_ = snapshot_meta_.last_included_index;
    first_index_ = snapshot_meta_.last_included_index + 1;
  }

  // Rewrite log file with remaining entries
  std::string tmp_path = log_path_ + ".tmp";
  {
    std::ofstream tmp(tmp_path, std::ios::binary | std::ios::trunc);
    for (auto& [idx, off] : index_offset_) {
      std::string rec;
      LogEntry entry;
      Status s = ReadEntryAt(off, &entry);
      if (!s.ok() || !EncodeLogEntry(entry, &rec).ok()) {
        return s.ok() ? Status::Corruption("invalid raft log entry") : s;
      }

      tmp.write(rec.data(), static_cast<std::streamsize>(rec.size()));
      if (!tmp) {
        return Status::IOError("failed to rewrite raft log");
      }
    }
    tmp.close();
  }

  Status s = SyncPath(tmp_path);
  if (!s.ok()) return s;
  if (platform::ReplaceFile(tmp_path, log_path_) != 0) {
    return Status::IOError("failed to publish compacted raft log: " +
                           platform::FileErrorString());
  }
  const std::filesystem::path parent =
      std::filesystem::path(log_path_).parent_path();
  if (!parent.empty() && platform::SyncDirectory(parent.string()) != 0) {
    return Status::IOError("failed to sync raft log directory: " +
                           platform::FileErrorString());
  }

  // Rebuild offset map
  LoadIndex();
  return initial_status_;
}

Status FileRaftStorage::TruncateSuffix(uint64_t index) {
  if (index >= last_index_) return Status::OK();

  // Remove entries after 'index' from the offset map
  for (auto it = index_offset_.upper_bound(index);
       it != index_offset_.end();) {
    it = index_offset_.erase(it);
  }

  if (index < first_index_) {
    last_index_ = snapshot_meta_.last_included_index;
    first_index_ = snapshot_meta_.last_included_index + 1;
  } else {
    last_index_ = index;
  }

  if (index_offset_.empty()) {
    last_index_ = snapshot_meta_.last_included_index;
    first_index_ = snapshot_meta_.last_included_index + 1;
  }

  // Truncate the file
  if (!index_offset_.empty()) {
    auto last_it = index_offset_.rbegin();
    uint64_t last_offset = last_it->second;
    LogEntry entry;
    Status s = ReadEntryAt(last_offset, &entry);
    if (!s.ok()) return s;
    uint32_t data_len = static_cast<uint32_t>(entry.data.size());
    uint64_t end_offset = last_offset + 4 + 20 + data_len;

    std::error_code ec;
    std::filesystem::resize_file(log_path_, end_offset, ec);
    if (ec) return Status::IOError("failed to truncate raft log");
  } else {
    std::error_code ec;
    std::filesystem::resize_file(log_path_, 0, ec);
    if (ec) return Status::IOError("failed to truncate raft log");
  }
  Status s = SyncPath(log_path_);
  if (!s.ok()) return s;
  return Status::OK();
}

Status FileRaftStorage::WriteEntry(const LogEntry& entry) {
  if (entry.index <= snapshot_meta_.last_included_index) {
    return Status::OK();
  }
  if (entry.index != last_index_ + 1) {
    return Status::InvalidArgument("raft log append is not contiguous");
  }
  std::string rec;
  Status s = EncodeLogEntry(entry, &rec);
  if (!s.ok()) return s;
  std::ofstream lf(log_path_, std::ios::binary | std::ios::app);
  if (!lf.is_open()) return Status::IOError("failed to open raft log");

  uint64_t offset = static_cast<uint64_t>(lf.tellp());
  lf.write(rec.data(), static_cast<std::streamsize>(rec.size()));
  if (!lf) return Status::IOError("failed to append raft log");
  lf.close();
  if (!lf) return Status::IOError("failed to close raft log");
  s = SyncPath(log_path_);
  if (!s.ok()) return s;

  index_offset_[entry.index] = offset;
  if (first_index_ == snapshot_meta_.last_included_index + 1 &&
      last_index_ == snapshot_meta_.last_included_index) {
    first_index_ = entry.index;
  }
  if (entry.index > last_index_) {
    last_index_ = entry.index;
  }
  return Status::OK();
}

Status FileRaftStorage::ReadEntryAt(uint64_t offset, LogEntry* entry) const {
  if (entry == nullptr) return Status::InvalidArgument("raft log entry is null");
  *entry = LogEntry{};
  std::ifstream lf(log_path_, std::ios::binary);
  if (!lf.is_open()) return Status::IOError("failed to open raft log");

  lf.seekg(static_cast<std::streamoff>(offset));

  char size_buf[4];
  lf.read(size_buf, 4);
  if (lf.gcount() != 4) return Status::Corruption("truncated raft log length");

  uint32_t rec_size = DecodeFixed32(size_buf);
  if (rec_size < 20 || rec_size > 20 + kMaxRaftLogEntryBytes) {
    return Status::Corruption("invalid raft log record size");
  }
  std::string rec(rec_size, '\0');
  lf.read(rec.data(), static_cast<std::streamsize>(rec_size));
  if (lf.gcount() != static_cast<std::streamsize>(rec_size)) {
    return Status::Corruption("truncated raft log record");
  }

  entry->index = DecodeFixed64(rec.data());
  entry->term = DecodeFixed64(rec.data() + 8);
  uint32_t data_len = DecodeFixed32(rec.data() + 16);
  if (data_len != rec_size - 20 || entry->index == 0) {
    return Status::Corruption("invalid raft log record");
  }
  entry->data.assign(rec.data() + 20, data_len);

  return Status::OK();
}

}  // namespace raft
}  // namespace kv
