#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace kv {

struct TableIndexEntry {
  std::string key;
  uint64_t offset = 0;
};

struct TableIndex {
  std::vector<TableIndexEntry> entries;
};

}  // namespace kv
