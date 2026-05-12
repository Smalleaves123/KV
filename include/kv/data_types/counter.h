#pragma once

#include <cstdint>
#include <string>

#include "kv/common/slice.h"
#include "kv/common/status.h"

namespace kv {

class DB;

/// 原子计数器 (INCR / DECR)
/// 底层存储为字符串格式的数字
class Counter {
 public:
  /// INCR key — 原子自增 1，返回新值
  static Status Incr(DB* db, const Slice& key, int64_t* new_value);

  /// INCRBY key delta — 原子增加 delta，返回新值
  static Status IncrBy(DB* db, const Slice& key, int64_t delta,
                       int64_t* new_value);

  /// DECR key — 原子自减 1，返回新值
  static Status Decr(DB* db, const Slice& key, int64_t* new_value);

  /// DECRBY key delta — 原子减少 delta，返回新值
  static Status DecrBy(DB* db, const Slice& key, int64_t delta,
                       int64_t* new_value);

  /// 获取当前计数值（如果不存在返回 0, IsNotFound）
  static Status Get(DB* db, const Slice& key, int64_t* value);
};

}  // namespace kv
