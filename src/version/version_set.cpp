#include "kv/version/version_set.h"

#include "kv/version/manifest.h"

namespace kv {

void VersionSet::ApplyAdd(const FileMeta& meta) {
  current_.AddFile(meta);
}

bool VersionSet::ApplyRemove(uint64_t file_number) {
  return current_.RemoveFile(file_number);
}

Status VersionSet::RecoverFromManifest(Manifest* manifest) {
  if (manifest == nullptr) {
    return Status::InvalidArgument("manifest is null");
  }

  std::vector<ManifestFileMeta> files;
  Status s = manifest->Recover(&files);
  if (!s.ok()) {
    return s;
  }

  current_ = Version();
  for (const auto& f : files) {
    current_.AddFile(f);
  }
  return Status::OK();
}

}  // namespace kv
