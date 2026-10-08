#pragma once

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string>
#include <vector>

#include "kv/common/status.h"
#include "kv/version/file_meta.h"

namespace kv {

// Share the full file description with the recovered in-memory Version.
using ManifestFileMeta = FileMeta;

class Manifest {
 public:
  static constexpr size_t kMaxFilePathSize = 4096;
  // Match the maximum user-key size accepted by the WAL codec.
  static constexpr size_t kMaxKeySize = 64 * 1024 * 1024;

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
