#pragma once

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
};

} // namespace kv
