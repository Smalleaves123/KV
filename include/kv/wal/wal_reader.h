#pragma once

#include <cstdint>
#include <fstream>
#include <string>

#include "kv/common/status.h"
#include "kv/wal/log_record.h"

namespace kv {

class WALReader {
 public:
  WALReader();
  ~WALReader();

  WALReader(const WALReader&) = delete;
  WALReader& operator=(const WALReader&) = delete;

  Status Open(const std::string& file_path);
  Status Close();

  // 成功读取一条记录 -> OK
  // 正常读到文件结尾 -> NotFound
  // 读到残缺/损坏记录 -> Corruption
  Status ReadNext(LogRecord* record);

  bool IsOpen() const noexcept;
  uint64_t offset() const noexcept;
  const std::string& file_path() const noexcept;

 private:
  std::ifstream stream_;
  std::string file_path_;
  uint64_t offset_;
};

}  // namespace kv