#include "kv/version/version.h"

namespace kv {

void Version::AddFile(const FileMeta& meta) {
  files_.push_back(meta);
}

bool Version::RemoveFile(uint64_t file_number) {
  for (auto it = files_.begin(); it != files_.end(); ++it) {
    if (it->file_number == file_number) {
      files_.erase(it);
      return true;
    }
  }
  return false;
}

bool Version::FindFile(uint64_t file_number, FileMeta* out) const {
  if (out == nullptr) {
    return false;
  }
  for (const auto& meta : files_) {
    if (meta.file_number == file_number) {
      *out = meta;
      return true;
    }
  }
  return false;
}

}  // namespace kv
