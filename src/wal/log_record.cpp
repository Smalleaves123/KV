#include "kv/wal/log_record.h"

#include <array>
#include <cstring>
#include <limits>

#include "kv/common/encoding.h"

namespace kv {
namespace {

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
  if (record.type == LogRecordType::kDelete &&
      (!record.value.empty() || record.expires_at_ms != 0)) {
    return Status::InvalidArgument("delete record must not contain value");
  }
  if (record.type == LogRecordType::kPut && record.expires_at_ms != 0) {
    return Status::InvalidArgument("put record must not contain ttl");
  }
  if (record.type == LogRecordType::kPutWithTTL &&
      record.expires_at_ms == 0) {
    return Status::InvalidArgument("ttl record must contain expiry");
  }
  if (record.type != LogRecordType::kPut &&
      record.type != LogRecordType::kDelete &&
      record.type != LogRecordType::kPutWithTTL) {
    return Status::InvalidArgument("unknown log record type");
  }

  std::string payload;
  const size_t ttl_prefix =
      record.type == LogRecordType::kPutWithTTL ? sizeof(uint64_t) : 0;
  if (record.value.size() >
          std::numeric_limits<uint32_t>::max() - ttl_prefix ||
      record.key.size() > std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument("log record payload is too large");
  }
  const size_t encoded_value_size = record.value.size() + ttl_prefix;
  payload.reserve(1 + 8 + 4 + 4 + record.key.size() + encoded_value_size);

  payload.push_back(static_cast<char>(record.type));
  EncodeFixed64(&payload, record.seq);
  EncodeFixed32(&payload, static_cast<uint32_t>(record.key.size()));
  EncodeFixed32(&payload, static_cast<uint32_t>(encoded_value_size));
  payload.append(record.key);
  if (record.type == LogRecordType::kPutWithTTL) {
    EncodeFixed64(&payload, record.expires_at_ms);
  }
  payload.append(record.value);

  const uint32_t checksum = ComputeChecksum(payload.data(), payload.size());

  out->clear();
  out->reserve(4 + payload.size());
  EncodeFixed32(out, checksum);
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
    case 2:
      type = LogRecordType::kPutWithTTL;
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
  if (type == LogRecordType::kPutWithTTL && value_size < sizeof(uint64_t)) {
    return Status::Corruption("ttl log record is missing expiry");
  }

  record->type = type;
  record->seq = seq;
  record->key.assign(key_ptr, key_size);
  record->expires_at_ms = 0;
  if (type == LogRecordType::kPutWithTTL) {
    record->expires_at_ms = DecodeFixed64(value_ptr);
    if (record->expires_at_ms == 0) {
      return Status::Corruption("ttl log record contains zero expiry");
    }
    record->value.assign(value_ptr + sizeof(uint64_t),
                         value_size - sizeof(uint64_t));
  } else {
    record->value.assign(value_ptr, value_size);
  }

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
