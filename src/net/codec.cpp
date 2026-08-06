#include "kv/net/codec.h"

#include <cerrno>
#include <cstdlib>
#include <limits>
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
  errno = 0;
  char* end = nullptr;
  const long long v = std::strtoll(s.c_str(), &end, 10);
  if (errno == ERANGE || end == s.c_str() || *end != '\0') return false;
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

RequestDecodeResult RequestCodec::TryDecode(
    std::string* buffer, std::vector<std::string>* tokens,
    std::string* error) {
  if (buffer == nullptr || tokens == nullptr) {
    if (error != nullptr) *error = "request output is null";
    return RequestDecodeResult::kError;
  }
  tokens->clear();
  if (error != nullptr) error->clear();
  if (buffer->empty()) {
    return RequestDecodeResult::kNeedMore;
  }
  if (buffer->size() > RequestCodec::kMaxRequestBytes) {
    if (error != nullptr) *error = "request is too large";
    return RequestDecodeResult::kError;
  }

  if ((*buffer)[0] != '*') {
    std::string line;
    const size_t newline = buffer->find('\n');
    if (newline == std::string::npos) {
      if (buffer->size() > RequestCodec::kMaxLineBytes) {
        if (error != nullptr) *error = "line request is too large";
        return RequestDecodeResult::kError;
      }
      return RequestDecodeResult::kNeedMore;
    }
    if (newline > RequestCodec::kMaxLineBytes) {
      if (error != nullptr) *error = "line request is too large";
      return RequestDecodeResult::kError;
    }
    line = buffer->substr(0, newline);
    if (!line.empty() && line.back() == '\r') line.pop_back();
    std::istringstream iss(line);
    std::string token;
    while (iss >> token) {
      tokens->push_back(token);
    }
    buffer->erase(0, newline + 1);
    return RequestDecodeResult::kOk;
  }

  enum class ParseState { kArrayLen, kBulkLen, kBulkData };
  ParseState state = ParseState::kArrayLen;
  size_t pos = 1;
  int64_t count = 0;
  int64_t index = 0;
  int64_t bulk_len = 0;
  std::string line;
  std::vector<std::string> parsed;
  while (true) {
    switch (state) {
      case ParseState::kArrayLen:
        if (!ReadCRLFLine(*buffer, &pos, &line)) {
          return RequestDecodeResult::kNeedMore;
        }
        if (!ParseInt64(line, &count) || count < 0) {
          if (error != nullptr) *error = "invalid RESP array length";
          return RequestDecodeResult::kError;
        }
        if (static_cast<uint64_t>(count) > RequestCodec::kMaxArguments) {
          if (error != nullptr) *error = "too many RESP arguments";
          return RequestDecodeResult::kError;
        }
        state = ParseState::kBulkLen;
        if (count == 0) {
          buffer->erase(0, pos);
          *tokens = std::move(parsed);
          return RequestDecodeResult::kOk;
        }
        break;

      case ParseState::kBulkLen:
        if (pos >= buffer->size()) {
          return RequestDecodeResult::kNeedMore;
        }
        if ((*buffer)[pos] != '$') {
          if (error != nullptr) *error = "expected RESP bulk string";
          return RequestDecodeResult::kError;
        }
        ++pos;
        if (!ReadCRLFLine(*buffer, &pos, &line)) {
          return RequestDecodeResult::kNeedMore;
        }
        if (!ParseInt64(line, &bulk_len) || bulk_len < 0) {
          if (error != nullptr) *error = "invalid RESP bulk string length";
          return RequestDecodeResult::kError;
        }
        if (static_cast<uint64_t>(bulk_len) >
            std::numeric_limits<size_t>::max()) {
          if (error != nullptr) *error = "RESP bulk string is too large";
          return RequestDecodeResult::kError;
        }
        if (static_cast<uint64_t>(bulk_len) >
            RequestCodec::kMaxBulkStringBytes) {
          if (error != nullptr) *error = "RESP bulk string is too large";
          return RequestDecodeResult::kError;
        }
        state = ParseState::kBulkData;
        break;

      case ParseState::kBulkData: {
        const size_t body_len = static_cast<size_t>(bulk_len);
        if (body_len > buffer->size() - pos ||
            buffer->size() - pos - body_len < 2) {
          return RequestDecodeResult::kNeedMore;
        }
        if ((*buffer)[pos + body_len] != '\r' ||
            (*buffer)[pos + body_len + 1] != '\n') {
          if (error != nullptr) *error = "bulk string missing CRLF terminator";
          return RequestDecodeResult::kError;
        }
        parsed.push_back(buffer->substr(pos, body_len));
        pos += body_len + 2;
        ++index;
        if (index == count) {
          buffer->erase(0, pos);
          *tokens = std::move(parsed);
          return RequestDecodeResult::kOk;
        }
        state = ParseState::kBulkLen;
        break;
      }
    }
  }
}

}  // namespace kv::net
