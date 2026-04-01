#include "kv/wal/log_record.h"

#include <array>
#include <cstring>

namespace kv {
namespace {

inline void AppendFixed32(std::string* out, uint32_t value) {
  out->push_back(static_cast<char>(value & 0xFF));
  out->push_back(static_cast<char>((value >> 8) & 0xFF));
  out->push_back(static_cast<char>((value >> 16) & 0xFF));
  out->push_back(static_cast<char>((value >> 24) & 0xFF));
}

inline void AppendFixed64(std::string* out, uint64_t value) {
  for (int i = 0; i < 8; ++i) {
    out->push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
  }
}

inline uint32_t DecodeFixed32(const char* p) {
  return static_cast<uint32_t>(static_cast<unsigned char>(p[0])) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[1])) << 8) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[2])) << 16) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[3])) << 24);
}

inline uint64_t DecodeFixed64(const char* p) {
  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value |= (static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i));
  }
  return value;
}

const std::array<uint32_t, 256>& GetCrc32Table() {
  static const std::array<uint32_t, 256> table = [] {
    std::array<uint32_t, 256> t{};
    for (uint32_t i = 0; i < 256; ++i) {
      uint32_t c = i;
      for (int j = 0; j < 8; ++j) {
        if (c & 1U) {
          c = 0xEDB88320U ^ (c >> 1);
        } else {
          c >>= 1;
        }
      }
      t[i] = c;
    }
    return t;
  }();
  return table;
}

}  // namespace

Status LogRecordCodec::Encode(const LogRecord& record, std::string* out) {
  if (out == nullptr) {
    return Status::InvalidArgument("output buffer is null");
  }
  if (record.seq == 0) {
    return Status::InvalidArgument("log record sequence must be greater than 0");
  }
  if (record.type == LogRecordType::kDelete && !record.value.empty()) {
    return Status::InvalidArgument("delete record must not contain value");
  }

  std::string payload;
  payload.reserve(1 + 8 + 4 + 4 + record.key.size() + record.value.size());

  payload.push_back(static_cast<char>(record.type));
  AppendFixed64(&payload, record.seq);
  AppendFixed32(&payload, static_cast<uint32_t>(record.key.size()));
  AppendFixed32(&payload, static_cast<uint32_t>(record.value.size()));
  payload.append(record.key);
  payload.append(record.value);

  const uint32_t checksum = ComputeChecksum(payload.data(), payload.size());

  out->clear();
  out->reserve(4 + payload.size());
  AppendFixed32(out, checksum);
  out->append(payload);

  return Status::OK();
}

Status LogRecordCodec::Decode(const Slice& input,
                              LogRecord* record,
                              size_t* bytes_consumed) {
  if (record == nullptr) {
    return Status::InvalidArgument("record output is null");
  }
  if (input.size() < kHeaderSize) {
    return Status::Corruption("buffer too small for log record header");
  }

  const char* p = input.data();
  const uint32_t stored_checksum = DecodeFixed32(p);
  p += 4;

  const uint8_t type_raw = static_cast<uint8_t>(static_cast<unsigned char>(*p));
  ++p;

  const uint64_t seq = DecodeFixed64(p);
  p += 8;

  const uint32_t key_size = DecodeFixed32(p);
  p += 4;

  const uint32_t value_size = DecodeFixed32(p);
  p += 4;

  const size_t total_size =
      kHeaderSize + static_cast<size_t>(key_size) + static_cast<size_t>(value_size);

  if (input.size() < total_size) {
    return Status::Corruption("incomplete log record payload");
  }

  const uint32_t actual_checksum =
      ComputeChecksum(input.data() + 4, total_size - 4);
  if (actual_checksum != stored_checksum) {
    return Status::Corruption("log record checksum mismatch");
  }

  LogRecordType type;
  switch (type_raw) {
    case 0:
      type = LogRecordType::kPut;
      break;
    case 1:
      type = LogRecordType::kDelete;
      break;
    default:
      return Status::Corruption("unknown log record type");
  }

  if (seq == 0) {
    return Status::Corruption("log record sequence must be greater than 0");
  }

  const char* key_ptr = p;
  const char* value_ptr = p + key_size;

  if (type == LogRecordType::kDelete && value_size != 0) {
    return Status::Corruption("delete log record contains unexpected value");
  }

  record->type = type;
  record->seq = seq;
  record->key.assign(key_ptr, key_size);
  record->value.assign(value_ptr, value_size);

  if (bytes_consumed != nullptr) {
    *bytes_consumed = total_size;
  }

  return Status::OK();
}

uint32_t LogRecordCodec::ComputeChecksum(const char* data, size_t size) {
  const auto& table = GetCrc32Table();

  uint32_t crc = 0xFFFFFFFFU;
  for (size_t i = 0; i < size; ++i) {
    const uint8_t byte = static_cast<uint8_t>(data[i]);
    crc = table[(crc ^ byte) & 0xFFU] ^ (crc >> 8);
  }
  return ~crc;
}

}  // namespace kv