#include "kv/sstable/block_iterator.h"

#include <algorithm>
#include <cstring>

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

uint64_t DecodeFixed64(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) {
    v |= (static_cast<uint64_t>(static_cast<uint8_t>(p[i]))) << (8 * i);
  }
  return v;
}

uint32_t DecodeFixed32(const char* p) {
  uint32_t v = 0;
  v |= static_cast<uint32_t>(static_cast<uint8_t>(p[0]));
  v |= static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 8;
  v |= static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 16;
  v |= static_cast<uint32_t>(static_cast<uint8_t>(p[3])) << 24;
  return v;
}

}  // namespace

BlockIterator::BlockIterator(const Block& block)
    : block_(block),
      current_(0),
      current_entry_(),
      valid_(false) {}

void BlockIterator::SeekToFirst() {
  SeekToRestart(0);
  valid_ = true;
}

void BlockIterator::SeekToLast() {
  const uint32_t nr = block_.NumRestarts();
  if (nr == 0) {
    valid_ = false;
    return;
  }

  SeekToRestart(static_cast<int>(nr - 1));

  const uint32_t data_end = block_.restart_offset_;
  while (current_ < data_end) {
    Entry e = ParseEntryAt(current_);
    if (e.offset == 0 && e.key.empty()) {
      break;
    }
    uint32_t next = e.offset;
    if (next <= current_) break;

    const char* p_next = block_.data_ + next;
    if (p_next >= block_.data_ + data_end) {
      current_entry_ = e;
      break;
    }

    current_ = next;
  }

  if (current_ < data_end) {
    current_entry_ = ParseEntryAt(current_);
    valid_ = current_entry_.offset > 0 || !current_entry_.key.empty();
  }
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
  const char* p = block_.data_ + current_entry_.offset;
  if (p >= block_.data_ + data_end) {
    valid_ = false;
    return;
  }

  current_ = current_entry_.offset;
  if (current_ >= data_end) {
    valid_ = false;
    return;
  }

  current_entry_ = ParseEntryAt(current_);
  valid_ = true;
}

void BlockIterator::Prev() {
  if (!Valid()) return;

  const uint32_t orig = current_;
  uint32_t pos = current_;

  for (int r = static_cast<int>(block_.NumRestarts()) - 1; r >= 0; --r) {
    uint32_t restart_off = RestartOffset(r);
    if (restart_off <= pos) {
      SeekToRestart(r);
      while (current_ < pos - 1) {
        uint32_t saved = current_;
        Entry e = ParseEntryAt(current_);
        if (e.offset <= saved) break;
        if (e.offset >= orig) break;
        current_ = e.offset;
        current_entry_ = ParseEntryAt(current_);
        if (current_entry_.offset <= current_) break;
      }
      valid_ = true;
      return;
    }
  }

  valid_ = false;
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
  current_entry_ = ParseEntryAt(current_);
  valid_ = true;
}

BlockIterator::Entry BlockIterator::ParseEntryAt(uint32_t offset) const {
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

  const char* key_start = p;
  p += unshared;
  e.key = std::string_view(key_start, unshared);

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
