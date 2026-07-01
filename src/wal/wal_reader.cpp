#include "kv/wal/wal_reader.h"

#include <array>
#include <cstdint>
#include <string>

namespace kv {
namespace {

uint32_t DecodeFixed32(const char* p) {
  return static_cast<uint32_t>(static_cast<unsigned char>(p[0])) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[1])) << 8) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[2])) << 16) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[3])) << 24);
}

}  // namespace

WALReader::WALReader() : stream_(), file_path_(), offset_(0) {}

WALReader::~WALReader() {
  (void)Close();
}

Status WALReader::Open(const std::string& file_path) {
  if (file_path.empty()) {
    return Status::InvalidArgument("wal file path is empty");
  }

  Status s = Close();
  if (!s.ok()) {
    return s;
  }

  stream_.clear();
  stream_.open(file_path, std::ios::binary | std::ios::in);
  if (!stream_.is_open()) {
    stream_.clear();
    return Status::IOError("failed to open wal file: " + file_path);
  }

  file_path_ = file_path;
  offset_ = 0;
  return Status::OK();
}

Status WALReader::Close() {
  if (!stream_.is_open()) {
    stream_.clear();
    return Status::OK();
  }

  stream_.close();
  if (stream_.fail()) {
    stream_.clear();
    return Status::IOError("failed to close wal file");
  }

  stream_.clear();
  return Status::OK();
}

Status WALReader::ReadNext(LogRecord* record) {
  if (record == nullptr) {
    return Status::InvalidArgument("record output is null");
  }

  if (!stream_.is_open()) {
    return Status::IOError("wal file is not open");
  }

  std::array<char, LogRecordCodec::kHeaderSize> header{};
  stream_.read(header.data(),
               static_cast<std::streamsize>(header.size()));
  const std::streamsize header_bytes = stream_.gcount();

  if (header_bytes == 0) {
    stream_.clear();
    return Status::NotFound("end of wal");
  }

  if (header_bytes != static_cast<std::streamsize>(header.size())) {
    stream_.clear();
    return Status::NotFound("truncated wal record header");
  }

  const uint32_t key_size = DecodeFixed32(header.data() + 13);
  const uint32_t value_size = DecodeFixed32(header.data() + 17);
  const size_t payload_size =
      static_cast<size_t>(key_size) + static_cast<size_t>(value_size);

  std::string encoded;
  encoded.reserve(header.size() + payload_size);
  encoded.append(header.data(), header.size());

  if (payload_size > 0) {
    std::string payload(payload_size, '\0');
    stream_.read(payload.data(),
                 static_cast<std::streamsize>(payload.size()));
    const std::streamsize payload_bytes = stream_.gcount();

    if (payload_bytes != static_cast<std::streamsize>(payload.size())) {
      stream_.clear();
      return Status::NotFound("truncated wal record payload");
    }

    encoded.append(payload);
  }

  size_t consumed = 0;
  Status s = LogRecordCodec::Decode(encoded, record, &consumed);
  if (!s.ok()) {
    return s;
  }

  offset_ += static_cast<uint64_t>(consumed);
  return Status::OK();
}

bool WALReader::IsOpen() const noexcept {
  return stream_.is_open();
}

uint64_t WALReader::offset() const noexcept {
  return offset_;
}

const std::string& WALReader::file_path() const noexcept {
  return file_path_;
}

}  // namespace kv