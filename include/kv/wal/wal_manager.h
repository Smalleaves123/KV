#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "kv/common/status.h"

namespace kv {

class WALWriter;

struct WALOptions {
  bool sync_on_write = false;
  size_t max_log_file_size = 64 * 1024 * 1024;
  std::string wal_dir = "data/wal";
};

class WALManager {
 public:
  WALManager();
  ~WALManager();

  WALManager(const WALManager&) = delete;
  WALManager& operator=(const WALManager&) = delete;

  Status Open(const WALOptions& options);
  Status Close();

  Status AppendPut(uint64_t seq, const std::string& key, const std::string& value);
  Status AppendPutWithTTL(uint64_t seq,
                          const std::string& key,
                          const std::string& value,
                          uint64_t expires_at_ms);
  Status AppendDelete(uint64_t seq, const std::string& key);

  Status Sync();
  static Status ListLogs(const std::string& wal_dir,
                         std::vector<std::string>* out);
  Status ListLogs(std::vector<std::string>* out) const;

  bool IsOpen() const noexcept { return open_; }
  const std::string& active_log_path() const noexcept { return active_log_path_; }

 private:
  Status RollToNewLog();

  WALOptions options_;
  std::string active_log_path_;
  std::unique_ptr<WALWriter> writer_;
  uint64_t next_log_number_;
  bool open_;
};

}  // namespace kv
