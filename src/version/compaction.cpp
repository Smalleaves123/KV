#include "kv/version/compaction.h"

#include "kv/engine/db.h"

namespace kv {

CompactionRunner::CompactionRunner(DB* db) : db_(db) {}

Status CompactionRunner::MaybeRunOnce() {
  if (db_ == nullptr) {
    return Status::InvalidArgument("db is null");
  }
  Status s = db_->Compact();
  if (s.IsNotFound()) {
    // For background scheduling, "nothing to compact" is a normal no-op.
    return Status::OK();
  }
  return s;
}

Status CompactionRunner::RunManual() {
  if (db_ == nullptr) {
    return Status::InvalidArgument("db is null");
  }
  return db_->Compact();
}

}  // namespace kv
