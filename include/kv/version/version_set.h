#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include "kv/version/version.h"
#include "kv/version/file_meta.h"
#include "kv/common/status.h"
namespace kv {

class Manifest;
class VersionSet {
public:
const Version& current() const noexcept { return current_; }
Version* mutable_current() noexcept { return &current_; }

void ApplyAdd(const FileMeta& meta);
bool ApplyRemove(uint64_t file_number);

Status RecoverFromManifest(Manifest * manifest);
private:
Version current_;
};

}  // namespace kv
