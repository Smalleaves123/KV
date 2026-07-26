#include "kv/data_types/counter.h"

#include <cerrno>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string>

#include "kv/engine/db.h"

namespace kv {

// 将 int64_t 转为字符串
static std::string I64ToString(int64_t v) {
  return std::to_string(v);
}

// 将字符串转为 int64_t，失败返回 false
static bool StringToI64(const std::string& s, int64_t* v) {
  if (s.empty()) return false;
  errno = 0;
  char* end = nullptr;
  int64_t val = std::strtoll(s.c_str(), &end, 10);
  if (errno == ERANGE || end == nullptr || *end != '\0') return false;
  *v = val;
  return true;
}

Status Counter::Incr(DB* db, const Slice& key, int64_t* new_value) {
  return IncrBy(db, key, 1, new_value);
}

Status Counter::IncrBy(DB* db, const Slice& key, int64_t delta,
                       int64_t* new_value) {
  if (db == nullptr) return Status::InvalidArgument("db is null");

  // Compound counter updates use OCC so concurrent clients cannot lose an
  // increment. Raft-backed DBs intentionally reject local transactions.
  constexpr int kMaxAttempts = 8;
  for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
    std::unique_ptr<Transaction> txn;
    Status s = db->BeginTransaction(TxnOptions{}, &txn);
    if (!s.ok()) return s;

    std::string current;
    s = txn->Get(key, &current);
    int64_t value = 0;
    if (s.IsNotFound()) {
      value = 0;
    } else if (s.ok()) {
      if (!StringToI64(current, &value)) {
        (void)txn->Rollback();
        return Status::InvalidArgument("value is not an integer");
      }
    } else {
      (void)txn->Rollback();
      return s;
    }

    if ((delta > 0 && value > std::numeric_limits<int64_t>::max() - delta) ||
        (delta < 0 && value < std::numeric_limits<int64_t>::min() - delta)) {
      (void)txn->Rollback();
      return Status::InvalidArgument("counter overflow");
    }
    value += delta;

    s = txn->Put(key, I64ToString(value));
    if (!s.ok()) {
      (void)txn->Rollback();
      return s;
    }
    s = txn->Commit();
    if (s.ok()) {
      if (new_value) *new_value = value;
      return Status::OK();
    }
    if (!s.IsAlreadyExists()) return s;
  }

  return Status::AlreadyExists("counter update conflict");
}

Status Counter::Decr(DB* db, const Slice& key, int64_t* new_value) {
  return IncrBy(db, key, -1, new_value);
}

Status Counter::DecrBy(DB* db, const Slice& key, int64_t delta,
                       int64_t* new_value) {
  if (delta == std::numeric_limits<int64_t>::min()) {
    return Status::InvalidArgument("counter overflow");
  }
  return IncrBy(db, key, -delta, new_value);
}

Status Counter::Get(DB* db, const Slice& key, int64_t* value) {
  std::string raw;
  Status s = db->Get(ReadOptions{}, key, &raw);
  if (s.IsNotFound()) {
    return s;
  }
  if (!s.ok()) return s;

  if (!StringToI64(raw, value)) {
    return Status::InvalidArgument("value is not an integer");
  }
  return Status::OK();
}

}  // namespace kv
