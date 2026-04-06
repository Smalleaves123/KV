#pragma once

#include "kv/common/status.h"

namespace kv {

class DB;

class CompactionRunner {
 public:
  explicit CompactionRunner(DB* db);

  Status MaybeRunOnce();
  Status RunManual();

 private:
  DB* db_;
};

}  // namespace kv
