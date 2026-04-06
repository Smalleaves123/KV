#pragma once

#include <cstdint>
#include <vector>

#include "kv/version/file_meta.h"

namespace kv {

class Version {
 public:
  const std::vector<FileMeta>& files() const noexcept { return files_; }
  size_t FileCount() const noexcept { return files_.size(); }

  void AddFile(const FileMeta& meta);
  bool RemoveFile(uint64_t file_number);
  bool FindFile(uint64_t file_number, FileMeta* out) const;

 private:
  std::vector<FileMeta> files_;
};

}  // namespace kv
