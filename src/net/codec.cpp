#include "kv/net/codec.h"

namespace kv::net {

bool LineCodec::TryDecodeLine(std::string* buffer, std::string* line) {
  if (buffer == nullptr || line == nullptr) {
    return false;
  }

  const size_t nl = buffer->find('\n');
  if (nl == std::string::npos) {
    return false;
  }

  *line = buffer->substr(0, nl);
  if (!line->empty() && line->back() == '\r') {
    line->pop_back();
  }

  buffer->erase(0, nl + 1);
  return true;
}

std::string LineCodec::EncodeLine(const std::string& line) {
  return line + "\r\n";
}

}  // namespace kv::net
