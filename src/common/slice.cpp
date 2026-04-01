#include "kv/common/slice.h"

#include <cassert>
#include <cstring>

namespace kv {

Slice::Slice() noexcept : data_(nullptr), size_(0) {}

Slice::Slice(const char* d, size_t n) : data_(d), size_(n) {}

Slice::Slice(const char* s) : data_(s), size_(s == nullptr ? 0 : std::strlen(s)) {}

Slice::Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}

Slice::Slice(std::string_view sv) : data_(sv.data()), size_(sv.size()) {}

const char* Slice::data() const noexcept {
  return data_;
}

size_t Slice::size() const noexcept {
  return size_;
}

bool Slice::empty() const noexcept {
  return size_ == 0;
}

char Slice::operator[](size_t i) const {
  assert(i < size_);
  return data_[i];
}

void Slice::clear() noexcept {
  data_ = nullptr;
  size_ = 0;
}

void Slice::remove_prefix(size_t n) {
  assert(n <= size_);
  data_ += n;
  size_ -= n;
}

std::string Slice::ToString() const {
  return data_ == nullptr ? std::string() : std::string(data_, size_);
}

std::string_view Slice::ToStringView() const noexcept {
  return std::string_view(data_, size_);
}

int Slice::compare(const Slice& rhs) const noexcept {
  const size_t min_len = size_ < rhs.size_ ? size_ : rhs.size_;
  int r = 0;
  if (min_len > 0) {
    r = std::memcmp(data_, rhs.data_, min_len);
  }
  if (r == 0) {
    if (size_ < rhs.size_) return -1;
    if (size_ > rhs.size_) return 1;
    return 0;
  }
  return r < 0 ? -1 : 1;
}

bool Slice::starts_with(const Slice& prefix) const noexcept {
  return size_ >= prefix.size() &&
         std::memcmp(data_, prefix.data(), prefix.size()) == 0;
}

bool operator==(const Slice& lhs, const Slice& rhs) noexcept {
  return lhs.size() == rhs.size() &&
         (lhs.size() == 0 ||
          std::memcmp(lhs.data(), rhs.data(), lhs.size()) == 0);
}

bool operator!=(const Slice& lhs, const Slice& rhs) noexcept {
  return !(lhs == rhs);
}

bool operator<(const Slice& lhs, const Slice& rhs) noexcept {
  return lhs.compare(rhs) < 0;
}

}  // namespace kv