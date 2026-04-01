#pragma once

#include <cstddef>
#include <string>
#include <string_view>

namespace kv {

class Slice {
 public:
  Slice() noexcept;
  Slice(const char* d, size_t n);
  Slice(const char* s);
  Slice(const std::string& s);
  Slice(std::string_view sv);

  const char* data() const noexcept;
  size_t size() const noexcept;
  bool empty() const noexcept;

  char operator[](size_t i) const;

  void clear() noexcept;
  void remove_prefix(size_t n);

  std::string ToString() const;
  std::string_view ToStringView() const noexcept;

  int compare(const Slice& rhs) const noexcept;
  bool starts_with(const Slice& prefix) const noexcept;

 private:
  const char* data_;
  size_t size_;
};

bool operator==(const Slice& lhs, const Slice& rhs) noexcept;
bool operator!=(const Slice& lhs, const Slice& rhs) noexcept;
bool operator<(const Slice& lhs, const Slice& rhs) noexcept;

}  // namespace kv