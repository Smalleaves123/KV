#include "kv/data_types/counter.h"

#include <cstdlib>
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
  char* end = nullptr;
  int64_t val = std::strtoll(s.c_str(), &end, 10);
  if (*end != '\0') return false;
  *v = val;
  return true;
}

Status Counter::Incr(DB* db, const Slice& key, int64_t* new_value) {
  return IncrBy(db, key, 1, new_value);
}

Status Counter::IncrBy(DB* db, const Slice& key, int64_t delta,
                       int64_t* new_value) {
  std::string current;
  Status s = db->Get(ReadOptions{}, key, &current);

  int64_t val = 0;
  if (s.IsNotFound()) {
    val = 0;
  } else if (s.ok()) {
    if (!StringToI64(current, &val)) {
      return Status::InvalidArgument("value is not an integer");
    }
  } else {
    return s;
  }

  val += delta;
  s = db->Put(WriteOptions{}, key, I64ToString(val));
  if (!s.ok()) return s;

  if (new_value) *new_value = val;
  return Status::OK();
}

Status Counter::Decr(DB* db, const Slice& key, int64_t* new_value) {
  return IncrBy(db, key, -1, new_value);
}

Status Counter::DecrBy(DB* db, const Slice& key, int64_t delta,
                       int64_t* new_value) {
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
