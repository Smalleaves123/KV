#pragma once

#include <cstdint>

#include "kv/common/status.h"

#include <string>

namespace kv {

// Applies a write that has already been ordered by an external consensus
// layer. Implementations are responsible for making the write durable in the
// local database.
class WriteApplier {
public:
  virtual ~WriteApplier() = default;

  virtual Status ApplyPut(const std::string& key, const std::string& value) = 0;
  virtual Status ApplyDelete(const std::string& key) = 0;
  virtual Status ApplyPutWithExpiry(const std::string& key,
                                    const std::string& value,
                                    uint64_t expires_at_ms) {
    (void)expires_at_ms;
    return ApplyPut(key, value);
  }
  virtual Status ApplyExpireAt(const std::string& key,
                               uint64_t expires_at_ms) = 0;
  virtual Status CreateCheckpoint(const std::string&) {
    return Status::InvalidArgument("state machine does not support checkpoints");
  }
  virtual Status InstallCheckpoint(const std::string&) {
    return Status::InvalidArgument("state machine does not support checkpoint install");
  }
};

} // namespace kv
