#pragma once

#include <string>

#include "kv/common/slice.h"
#include "kv/common/status.h"

namespace kv {

struct TxnOptions {
  bool sync_on_commit = false;
};

class Transaction {
 public:
  virtual ~Transaction() = default;

  virtual Status Get(const Slice& key, std::string* value) = 0;
  virtual Status Put(const Slice& key, const Slice& value) = 0;
  virtual Status Delete(const Slice& key) = 0;

  virtual Status Commit() = 0;
  virtual Status Rollback() = 0;
};

}  // namespace kv
