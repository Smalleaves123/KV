#include "kv/net/codec.h"

#include <cstdlib>
#include <sstream>

namespace kv::net {
namespace {

bool ReadCRLFLine(const std::string& buffer, size_t* pos, std::string* line) {
  const size_t end = buffer.find("\r\n", *pos);
  if (end == std::string::npos) {
    return false;
  }
  *line = buffer.substr(*pos, end - *pos);
  *pos = end + 2;
  return true;
}

bool ParseInt64(const std::string& s, int64_t* out) {
  if (out == nullptr) return false;
  char* end = nullptr;
  const long long v = std::strtoll(s.c_str(), &end, 10);
  if (end == s.c_str() || *end != '\0') return false;
  *out = static_cast<int64_t>(v);
  return true;
}

}  // namespace

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

bool RequestCodec::TryDecode(std::string* buffer,
                             std::vector<std::string>* tokens,
                             std::string* error) {
  if (buffer == nullptr || tokens == nullptr) {
    if (error != nullptr) *error = "request output is null";
    return false;
  }
  tokens->clear();
  if (error != nullptr) error->clear();
  if (buffer->empty()) {
    return false;
  }

  if ((*buffer)[0] != '*') {
    std::string line;
    if (!LineCodec::TryDecodeLine(buffer, &line)) {
      return false;
    }
    std::istringstream iss(line);
    std::string token;
    while (iss >> token) {
      tokens->push_back(token);
    }
    return true;
  }

  size_t pos = 1;
  std::string line;
  if (!ReadCRLFLine(*buffer, &pos, &line)) {
    return false;
  }

  int64_t count = 0;
  if (!ParseInt64(line, &count) || count < 0) {
    if (error != nullptr) *error = "invalid RESP array length";
    buffer->clear();
    return true;
  }

  std::vector<std::string> parsed;
  parsed.reserve(static_cast<size_t>(count));
  for (int64_t i = 0; i < count; ++i) {
    if (pos >= buffer->size()) {
      return false;
    }
    if ((*buffer)[pos] != '$') {
      if (error != nullptr) *error = "expected RESP bulk string";
      buffer->erase(0, pos);
      return true;
    }
    ++pos;

    if (!ReadCRLFLine(*buffer, &pos, &line)) {
      return false;
    }
    int64_t len = 0;
    if (!ParseInt64(line, &len) || len < 0) {
      if (error != nullptr) *error = "invalid RESP bulk string length";
      buffer->erase(0, pos);
      return true;
    }

    const size_t body_len = static_cast<size_t>(len);
    if (buffer->size() < pos + body_len + 2) {
      return false;
    }
    if ((*buffer)[pos + body_len] != '\r' ||
        (*buffer)[pos + body_len + 1] != '\n') {
      if (error != nullptr) *error = "bulk string missing CRLF terminator";
      buffer->erase(0, pos + body_len);
      return true;
    }

    parsed.push_back(buffer->substr(pos, body_len));
    pos += body_len + 2;
  }

  buffer->erase(0, pos);
  *tokens = std::move(parsed);
  return true;
}

}  // namespace kv::net
