#include "kv/sstable/block.h"

#include <cstring>

namespace kv {

Block::Block(const char* data, size_t size)
    : data_(data),
      size_(size),
      restart_offset_(0),
      num_restarts_(0) {
  if (size_ < sizeof(uint32_t)) {
    data_ = nullptr;
    size_ = 0;
    return;
  }

  const char* p = data_ + size_ - sizeof(uint32_t);
  uint32_t nr = 0;
  std::memcpy(&nr, p, sizeof(uint32_t));
  num_restarts_ = nr;

  const size_t needed = num_restarts_ * sizeof(uint32_t) + sizeof(uint32_t);
  if (size_ < needed) {
    data_ = nullptr;
    size_ = 0;
    return;
  }

  if (num_restarts_ > 0) {
    restart_offset_ = static_cast<uint32_t>(size_ - needed);
  }
}

}  // namespace kv
