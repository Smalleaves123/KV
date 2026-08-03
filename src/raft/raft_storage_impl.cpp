#include "kv/raft/raft_storage_impl.h"

#include <cstring>
#include <filesystem>
#include <system_error>

#include "kv/common/encoding.h"

namespace kv {
namespace raft {

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
      last_index_(0) {
  std::error_code ec;
  std::filesystem::create_directories(dir_path_, ec);

  // Load hard state
  std::ifstream sf(state_path_, std::ios::binary);
  if (sf.is_open()) {
    char buf[32];
    sf.read(buf, 32);
    if (sf.gcount() >= 24) {
      hard_state_.term = DecodeFixed64(buf);
      hard_state_.vote_for = DecodeFixed64(buf + 8);
      hard_state_.commit_index = DecodeFixed64(buf + 16);
      if (sf.gcount() >= 32) {
        hard_state_.applied_index = DecodeFixed64(buf + 24);
      }
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

HardState FileRaftStorage::InitialState() const {
  return hard_state_;
}

void FileRaftStorage::SaveHardState(const HardState& state) {
  hard_state_ = state;

  std::string data;
  data.reserve(32);
  EncodeFixed64(&data, state.term);
  EncodeFixed64(&data, state.vote_for);
  EncodeFixed64(&data, state.commit_index);
  EncodeFixed64(&data, state.applied_index);

  std::ofstream sf(state_path_, std::ios::binary | std::ios::trunc);
  if (sf.is_open()) {
    sf.write(data.data(), static_cast<std::streamsize>(data.size()));
    sf.close();
  }
}

std::vector<uint64_t> FileRaftStorage::InitialMembers() const {
  return members_;
}

void FileRaftStorage::SaveMembers(const std::vector<uint64_t>& members) {
  if (members.empty()) return;

  std::string data;
  data.reserve(4 + members.size() * 8);
  EncodeFixed32(&data, static_cast<uint32_t>(members.size()));
  for (uint64_t member : members) EncodeFixed64(&data, member);

  const std::string tmp_path = members_path_ + ".tmp";
  {
    std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) return;
    out.write(data.data(), static_cast<std::streamsize>(data.size()));
    if (!out) return;
  }

  std::error_code ec;
  std::filesystem::rename(tmp_path, members_path_, ec);
  if (ec) {
    std::filesystem::remove(members_path_, ec);
    ec.clear();
    std::filesystem::rename(tmp_path, members_path_, ec);
  }
  if (!ec) members_ = members;
}

RaftSnapshotMeta FileRaftStorage::SnapshotMeta() const {
  return snapshot_meta_;
}

void FileRaftStorage::SaveSnapshotMeta(const RaftSnapshotMeta& meta) {
  std::string data;
  data.reserve(16);
  EncodeFixed64(&data, meta.last_included_index);
  EncodeFixed64(&data, meta.last_included_term);

  const std::string tmp_path = snapshot_meta_path_ + ".tmp";
  {
    std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) return;
    out.write(data.data(), static_cast<std::streamsize>(data.size()));
    if (!out) return;
  }

  std::error_code ec;
  std::filesystem::rename(tmp_path, snapshot_meta_path_, ec);
  if (ec) {
    std::filesystem::remove(snapshot_meta_path_, ec);
    ec.clear();
    std::filesystem::rename(tmp_path, snapshot_meta_path_, ec);
  }
  if (!ec) snapshot_meta_ = meta;
}

void FileRaftStorage::LoadIndex() {
  index_offset_.clear();
  first_index_ = snapshot_meta_.last_included_index + 1;
  last_index_ = snapshot_meta_.last_included_index;

  std::ifstream lf(log_path_, std::ios::binary);
  if (!lf.is_open()) return;

  uint64_t offset = 0;
  while (true) {
    char size_buf[4];
    lf.read(size_buf, 4);
    if (lf.gcount() != 4) break;

    uint32_t rec_size = DecodeFixed32(size_buf);
    if (rec_size < 20) break;  // minimum: index(8) + term(8) + datalen(4)

    char rec_buf[20];  // index + term + datalen
    lf.read(rec_buf, 20);
    if (lf.gcount() != 20) break;

    uint64_t index = DecodeFixed64(rec_buf);
    uint32_t data_len = DecodeFixed32(rec_buf + 16);
    if (data_len > rec_size - 20) break;

    lf.seekg(static_cast<std::streamoff>(data_len), std::ios::cur);
    if (!lf) break;

    if (index <= snapshot_meta_.last_included_index) {
      offset += 4 + 20 + data_len;
      continue;
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
    result.push_back(ReadEntryAt(it->second));
  }
  return result;
}

uint64_t FileRaftStorage::Term(uint64_t index) const {
  if (index < first_index_ || index > last_index_) return 0;
  auto it = index_offset_.find(index);
  if (it == index_offset_.end()) return 0;
  LogEntry entry = ReadEntryAt(it->second);
  return entry.term;
}

uint64_t FileRaftStorage::FirstIndex() const {
  return first_index_;
}

uint64_t FileRaftStorage::LastIndex() const {
  return last_index_;
}

void FileRaftStorage::Append(const std::vector<LogEntry>& entries) {
  if (entries.empty()) return;

  for (const auto& entry : entries) {
    WriteEntry(entry);
  }
}

void FileRaftStorage::TruncatePrefix(uint64_t index) {
  if (index <= first_index_) return;

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
      LogEntry entry = ReadEntryAt(off);

      std::string rec;
      uint32_t data_len = static_cast<uint32_t>(entry.data.size());
      uint32_t rec_size = 20 + data_len;

      EncodeFixed32(&rec, rec_size);
      EncodeFixed64(&rec, entry.index);
      EncodeFixed64(&rec, entry.term);
      EncodeFixed32(&rec, data_len);
      rec.append(entry.data);

      tmp.write(rec.data(), static_cast<std::streamsize>(rec.size()));
      if (!tmp) break;
    }
    tmp.close();
  }

  std::error_code ec;
  std::filesystem::remove(log_path_, ec);
  std::filesystem::rename(tmp_path, log_path_, ec);

  // Rebuild offset map
  LoadIndex();
}

void FileRaftStorage::TruncateSuffix(uint64_t index) {
  if (index >= last_index_) return;

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
    LogEntry entry = ReadEntryAt(last_offset);
    uint32_t data_len = static_cast<uint32_t>(entry.data.size());
    uint64_t end_offset = last_offset + 4 + 20 + data_len;

    std::error_code ec;
    std::filesystem::resize_file(log_path_, end_offset, ec);
  } else {
    std::error_code ec;
    std::filesystem::resize_file(log_path_, 0, ec);
  }
}

void FileRaftStorage::WriteEntry(const LogEntry& entry) {
  if (entry.index <= snapshot_meta_.last_included_index) {
    return;
  }
  std::ofstream lf(log_path_, std::ios::binary | std::ios::app);
  if (!lf.is_open()) return;

  uint32_t data_len = static_cast<uint32_t>(entry.data.size());
  uint32_t rec_size = 20 + data_len;

  uint64_t offset = static_cast<uint64_t>(lf.tellp());

  std::string rec;
  rec.reserve(4 + rec_size);
  EncodeFixed32(&rec, rec_size);
  EncodeFixed64(&rec, entry.index);
  EncodeFixed64(&rec, entry.term);
  EncodeFixed32(&rec, data_len);
  rec.append(entry.data);

  lf.write(rec.data(), static_cast<std::streamsize>(rec.size()));
  lf.close();

  index_offset_[entry.index] = offset;
  if (first_index_ == snapshot_meta_.last_included_index + 1 &&
      last_index_ == snapshot_meta_.last_included_index) {
    first_index_ = entry.index;
  }
  if (entry.index > last_index_) {
    last_index_ = entry.index;
  }
}

LogEntry FileRaftStorage::ReadEntryAt(uint64_t offset) const {
  LogEntry entry;
  std::ifstream lf(log_path_, std::ios::binary);
  if (!lf.is_open()) return entry;

  lf.seekg(static_cast<std::streamoff>(offset));

  char size_buf[4];
  lf.read(size_buf, 4);
  if (lf.gcount() != 4) return entry;

  uint32_t rec_size = DecodeFixed32(size_buf);
  std::string rec(rec_size, '\0');
  lf.read(rec.data(), static_cast<std::streamsize>(rec_size));
  if (lf.gcount() != static_cast<std::streamsize>(rec_size)) return entry;

  entry.index = DecodeFixed64(rec.data());
  entry.term = DecodeFixed64(rec.data() + 8);
  uint32_t data_len = DecodeFixed32(rec.data() + 16);
  entry.data.assign(rec.data() + 20, data_len);

  return entry;
}

}  // namespace raft
}  // namespace kv
