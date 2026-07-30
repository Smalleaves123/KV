#pragma once

#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

#include "kv/common/status.h"

namespace kv {

struct ManifestFileMeta {
  uint64_t file_number = 0;
  std::string file_path;
  uint64_t max_seq = 0;
};

class Manifest {
 public:
  Manifest();
  ~Manifest();

  Manifest(const Manifest&) = delete;
  Manifest& operator=(const Manifest&) = delete;

  Status Open(const std::string& file_path, bool create_if_missing);
  Status Close();

  Status AddFile(const ManifestFileMeta& file_meta);
  Status RemoveFile(uint64_t file_number);
  // Flush buffered records and make them durable before dependent files change.
  Status Sync();
  Status Recover(std::vector<ManifestFileMeta>* files) const;

  bool IsOpen() const noexcept;
  const std::string& file_path() const noexcept;

 private:
  std::ofstream append_stream_;
  std::string file_path_;
  int sync_fd_;
  bool is_open_;
};

}  // namespace kv
