#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace kv {

struct FileMeta {
  uint64_t file_number = 0;
  std::string file_path;
  uint64_t max_seq = 0;
  size_t file_size = 0;

  bool IsValid() const noexcept;
};

}  // namespace kv
