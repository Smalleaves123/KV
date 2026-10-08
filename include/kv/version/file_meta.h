#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace kv {

struct FileMeta {
  uint64_t file_number = 0;
  std::string file_path;
  uint64_t max_seq = 0;
  // Physical SST size in bytes, including filters, index and footer.
  // Zero means unknown for legacy manifest records.
  uint64_t file_size = 0;
  uint32_t level = 0;
  // Inclusive user-key bounds, including tombstones and expired versions.
  // Both empty means unknown; such files must not be excluded by range.
  std::string smallest_key = {};
  std::string largest_key = {};

  bool IsValid() const noexcept;
  bool HasKeyRange() const noexcept;
};

}  // namespace kv
