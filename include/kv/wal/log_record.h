#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "kv/common/slice.h"
#include "kv/common/status.h"

namespace kv {

enum class LogRecordType : uint8_t {
  kPut = 0,
  kDelete = 1,
  kPutWithTTL = 2,
};

struct LogRecord {
  LogRecordType type = LogRecordType::kPut;
  uint64_t seq = 0;
  std::string key;
  std::string value;
  uint64_t expires_at_ms = 0;
};

class LogRecordCodec {
 public:
  // | checksum:4 | type:1 | seq:8 | key_size:4 | value_size:4 | key | value |
  static constexpr size_t kHeaderSize = 4 + 1 + 8 + 4 + 4;
  static constexpr size_t kMaxPayloadSize = 64 * 1024 * 1024;

  static Status Encode(const LogRecord& record, std::string* out);

  // input 可以是一整段 buffer；bytes_consumed 返回本条记录占用字节数
  static Status Decode(const Slice& input,
                       LogRecord* record,
                       size_t* bytes_consumed = nullptr);

 private:
  static uint32_t ComputeChecksum(const char* data, size_t size);
};

}
