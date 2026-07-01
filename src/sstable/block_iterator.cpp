#include "kv/sstable/block_iterator.h"

#include <cstring>

#include "kv/common/encoding.h"

#include "kv/sstable/block.h"

namespace kv {
namespace {

const char* DecodeVarint32(const char* p, const char* limit, uint32_t* out) {
  if (p >= limit) return nullptr;
  uint32_t result = 0;
  for (size_t shift = 0; shift <= 28; shift += 7) {
    if (p >= limit) return nullptr;
    uint8_t b = static_cast<uint8_t>(*p);
    ++p;
    result |= (static_cast<uint32_t>(b & 0x7F)) << shift;
    if ((b & 0x80) == 0) {
      *out = result;
      return p;
    }
  }
  return nullptr;
}

}  // namespace

BlockIterator::BlockIterator(const Block& block)
    : block_(block),
      current_(0),
      current_entry_(),
      current_key_storage_(),
      valid_(false) {}

void BlockIterator::SeekToFirst() {
  SeekToRestart(0);
  valid_ = current_entry_.offset > 0 || !current_entry_.key.empty();
}

void BlockIterator::Seek(std::string_view target) {
  uint32_t lo = 0;
  uint32_t hi = block_.NumRestarts();

  while (lo < hi) {
    const uint32_t mid = lo + (hi - lo) / 2;
    SeekToRestart(static_cast<int>(mid));
    if (!Valid()) {
      hi = mid;
      continue;
    }
    auto k = current_entry_.key;
    if (k < target) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }

  if (lo > 0) {
    SeekToRestart(static_cast<int>(lo - 1));
  } else {
    SeekToRestart(0);
  }

  while (Valid() && current_entry_.key < target) {
    Next();
  }

  valid_ = Valid() && current_entry_.key >= target;
}

void BlockIterator::Next() {
  if (!Valid()) return;

  const uint32_t data_end = block_.restart_offset_;
  if (current_entry_.offset >= data_end) {
    valid_ = false;
    return;
  }

  Entry next = ParseEntryAt(current_entry_.offset, current_key_storage_);
  if (next.offset <= current_) {
    valid_ = false;
    return;
  }

  current_ = current_entry_.offset;
  current_entry_ = next;
  valid_ = true;
}

bool BlockIterator::Valid() const noexcept {
  return valid_;
}

std::string_view BlockIterator::key() const noexcept {
  return current_entry_.key;
}

uint64_t BlockIterator::seq() const noexcept {
  return current_entry_.seq;
}

uint8_t BlockIterator::type() const noexcept {
  return current_entry_.type;
}

std::string_view BlockIterator::value() const noexcept {
  return current_entry_.value;
}

uint32_t BlockIterator::RestartOffset(int index) const {
  if (index < 0 || static_cast<uint32_t>(index) >= block_.NumRestarts()) {
    return 0;
  }
  const char* p = block_.data_ + block_.restart_offset_
                  + static_cast<size_t>(index) * sizeof(uint32_t);
  return DecodeFixed32(p);
}

void BlockIterator::SeekToRestart(int index) {
  current_ = RestartOffset(index);
  current_entry_ = ParseEntryAt(current_, std::string());
  valid_ = current_entry_.offset > 0 || !current_entry_.key.empty();
}

BlockIterator::Entry BlockIterator::ParseEntryAt(uint32_t offset,
                                                  const std::string& prev_key) const {
  Entry e{};
  e.offset = offset;

  const char* p = block_.data_ + offset;
  const char* limit = block_.data_ + block_.restart_offset_;
  if (p >= limit) return e;

  uint32_t shared = 0;
  p = DecodeVarint32(p, limit, &shared);
  if (p == nullptr) return e;

  uint32_t unshared = 0;
  p = DecodeVarint32(p, limit, &unshared);
  if (p == nullptr) return e;

  if (p + unshared + 8 + 1 > limit) return e;

  if (shared > prev_key.size()) return e;

  current_key_storage_.assign(prev_key.data(), shared);
  const char* key_start = p;
  p += unshared;
  current_key_storage_.append(key_start, unshared);
  e.key = std::string_view(current_key_storage_);

  e.seq = DecodeFixed64(p);
  p += 8;
  e.type = static_cast<uint8_t>(*p);
  p += 1;

  uint32_t val_len = 0;
  p = DecodeVarint32(p, limit, &val_len);
  if (p == nullptr || p + val_len > limit) return e;

  e.value = std::string_view(p, val_len);
  p += val_len;

  e.offset = static_cast<uint32_t>(p - block_.data_);
  return e;
}

}  // namespace kv
