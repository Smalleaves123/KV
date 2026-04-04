#pragma once

#include <string>

namespace kv::net {

class LineCodec {
 public:
  // Try decode one line from buffer. Supports '\n' and strips trailing '\r'.
  // Returns true when one full line is decoded.
  static bool TryDecodeLine(std::string* buffer, std::string* line);

  // Encode a line for wire transport by appending CRLF.
  static std::string EncodeLine(const std::string& line);
};

}  // namespace kv::net
