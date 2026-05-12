#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace kv {

class Block {
 public:
  Block() : data_(nullptr), size_(0), restart_offset_(0), num_restarts_(0) {}
  Block(const char* data, size_t size);

  const char* data() const noexcept { return data_; }
  size_t size() const noexcept { return size_; }
  uint32_t NumRestarts() const noexcept { return num_restarts_; }
  bool empty() const noexcept { return size_ == 0; }

 private:
  friend class BlockIterator;
  const char* data_;
  size_t size_;
  uint32_t restart_offset_;
  uint32_t num_restarts_;
};

}  // namespace kv
