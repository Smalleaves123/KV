#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "kv/common/status.h"

namespace kv {

class MemTable;

class LogRecovery {
 public:
  static Status ReplayLogs(const std::vector<std::string>& wal_files,
                           MemTable* memtable,
                           uint64_t* max_seq);
};

}  // namespace kv
