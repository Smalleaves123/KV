#include "kv/sstable/block_builder.h"

#include <algorithm>
#include <cstring>

namespace kv {
namespace {

void PutVarint32(std::string* out, uint32_t v) {
  while (v >= 0x80) {
    out->push_back(static_cast<char>(v | 0x80));
    v >>= 7;
  }
  out->push_back(static_cast<char>(v));
}

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

size_t SharedPrefixLen(const std::string& a, const std::string& b) {
  const size_t n = std::min(a.size(), b.size());
  for (size_t i = 0; i < n; ++i) {
    if (a[i] != b[i]) {
      return i;
    }
  }
  return n;
}

}  // namespace

BlockBuilder::BlockBuilder(size_t restart_interval)
    : buffer_(),
      last_key_(),
      restarts_(),
      restart_interval_(restart_interval == 0 ? 1 : restart_interval),
      entry_count_(0),
      finished_(false) {}

void BlockBuilder::Add(const std::string& key, uint64_t seq, uint8_t type,
                       const std::string& value) {
  if (finished_) return;

  const bool is_restart = (entry_count_ % restart_interval_ == 0);

  if (is_restart) {
    restarts_.push_back(static_cast<uint32_t>(buffer_.size()));
    last_key_.clear();
  }

  const size_t shared = is_restart ? 0 : SharedPrefixLen(last_key_, key);
  const size_t unshared = key.size() - shared;

  PutVarint32(&buffer_, static_cast<uint32_t>(shared));
  PutVarint32(&buffer_, static_cast<uint32_t>(unshared));
  buffer_.append(key.data() + shared, unshared);

  PutFixed64(&buffer_, seq);
  buffer_.push_back(static_cast<char>(type));

  PutVarint32(&buffer_, static_cast<uint32_t>(value.size()));
  buffer_.append(value);

  last_key_ = key;
  ++entry_count_;
}

std::string BlockBuilder::Finish() {
  if (finished_) return buffer_;

  for (uint32_t r : restarts_) {
    PutFixed32(&buffer_, r);
  }
  PutFixed32(&buffer_, static_cast<uint32_t>(restarts_.size()));

  finished_ = true;
  return buffer_;
}

void BlockBuilder::Reset() {
  buffer_.clear();
  last_key_.clear();
  restarts_.clear();
  entry_count_ = 0;
  finished_ = false;
}

size_t BlockBuilder::Size() const noexcept {
  return buffer_.size();
}

bool BlockBuilder::Empty() const noexcept {
  return entry_count_ == 0;
}

}  // namespace kv
