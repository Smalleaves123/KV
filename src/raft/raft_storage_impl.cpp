#include "kv/raft/raft_storage_impl.h"

#include <cstring>
#include <filesystem>
#include <system_error>

namespace kv {
namespace raft {

namespace {

void PutFixed64(std::string* out, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    out->push_back(static_cast<char>(v & 0xFF));
    v >>= 8;
  }
}

void PutFixed32(std::string* out, uint32_t v) {
  out->push_back(static_cast<char>(v & 0xFF));
  out->push_back(static_cast<char>((v >> 8) & 0xFF));
  out->push_back(static_cast<char>((v >> 16) & 0xFF));
  out->push_back(static_cast<char>((v >> 24) & 0xFF));
}

uint64_t DecodeFixed64(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) {
    v |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (8 * i);
  }
  return v;
}

uint32_t DecodeFixed32(const char* p) {
  uint32_t v = 0;
  v |= static_cast<uint32_t>(static_cast<uint8_t>(p[0]));
  v |= static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 8;
  v |= static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 16;
  v |= static_cast<uint32_t>(static_cast<uint8_t>(p[3])) << 24;
  return v;
}

}  // namespace

FileRaftStorage::FileRaftStorage(const std::string& dir_path)
    : dir_path_(dir_path),
      state_path_(dir_path + "/raft_state"),
      log_path_(dir_path + "/raft_log"),
      hard_state_{},
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
  PutFixed64(&data, state.term);
  PutFixed64(&data, state.vote_for);
  PutFixed64(&data, state.commit_index);
  PutFixed64(&data, state.applied_index);

  std::ofstream sf(state_path_, std::ios::binary | std::ios::trunc);
  if (sf.is_open()) {
    sf.write(data.data(), static_cast<std::streamsize>(data.size()));
    sf.close();
  }
}

void FileRaftStorage::LoadIndex() {
  index_offset_.clear();
  first_index_ = 1;
  last_index_ = 0;

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

    lf.seekg(static_cast<std::streamoff>(data_len), std::ios::cur);
    if (!lf) break;

    index_offset_[index] = offset;
    if (first_index_ == 1 && last_index_ == 0) {
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
    last_index_ = 0;
    first_index_ = 1;
  } else {
    first_index_ = index;
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

      PutFixed32(&rec, rec_size);
      PutFixed64(&rec, entry.index);
      PutFixed64(&rec, entry.term);
      PutFixed32(&rec, data_len);
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
    last_index_ = 0;
    first_index_ = 1;
  } else {
    last_index_ = index;
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
  }
}

void FileRaftStorage::WriteEntry(const LogEntry& entry) {
  std::ofstream lf(log_path_, std::ios::binary | std::ios::app);
  if (!lf.is_open()) return;

  uint32_t data_len = static_cast<uint32_t>(entry.data.size());
  uint32_t rec_size = 20 + data_len;

  uint64_t offset = static_cast<uint64_t>(lf.tellp());

  std::string rec;
  rec.reserve(4 + rec_size);
  PutFixed32(&rec, rec_size);
  PutFixed64(&rec, entry.index);
  PutFixed64(&rec, entry.term);
  PutFixed32(&rec, data_len);
  rec.append(entry.data);

  lf.write(rec.data(), static_cast<std::streamsize>(rec.size()));
  lf.close();

  index_offset_[entry.index] = offset;
  if (first_index_ == 1 && last_index_ == 0) {
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
