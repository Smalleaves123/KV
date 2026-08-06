#pragma once

#include <cstddef>
#include <string>
#include <vector>

namespace kv::net {

enum class RequestDecodeResult {
  kNeedMore,
  kOk,
  kError,
};

class LineCodec {
 public:
  // Try decode one line from buffer. Supports '\n' and strips trailing '\r'.
  // Returns true when one full line is decoded.
  static bool TryDecodeLine(std::string* buffer, std::string* line);

  // Encode a line for wire transport by appending CRLF.
  static std::string EncodeLine(const std::string& line);
};

class RequestCodec {
 public:
  // Hard limits keep a client from retaining unbounded partial requests in
  // the connection buffer or forcing an excessive number of allocations.
  static constexpr size_t kMaxLineBytes = 1 * 1024 * 1024;
  static constexpr size_t kMaxRequestBytes = 16 * 1024 * 1024;
  static constexpr size_t kMaxBulkStringBytes = 8 * 1024 * 1024;
  static constexpr size_t kMaxArguments = 1024;

  // Decodes one request without consuming incomplete or malformed input.
  // On success, removes exactly one request and leaves pipelined bytes intact.
  static RequestDecodeResult TryDecode(std::string* buffer,
                                       std::vector<std::string>* tokens,
                                       std::string* error);
};

}  // namespace kv::net
