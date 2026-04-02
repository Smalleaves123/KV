#include "kv/engine/db.h"

#include <memory>

#include "kv/engine/db_impl.h"

namespace kv {

Status DB::Open(const DBOptions& options, std::unique_ptr<DB>* db) {
  if (db == nullptr) {
    return Status::InvalidArgument("db output is null");
  }

  auto impl = std::make_unique<DBImpl>(options);
  Status s = impl->Init();
  if (!s.ok()) {
    return s;
  }

  *db = std::move(impl);
  return Status::OK();
}

}  // namespace kv
