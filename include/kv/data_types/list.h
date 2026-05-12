#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "kv/common/slice.h"
#include "kv/common/status.h"

namespace kv {

class DB;

/// List 数据类型 — 双向链表（模拟队列/栈）
///
/// 编码格式（单 KV 存储整个 List）：
///   [elem_count:4B][elem1_len:4B][elem1]...
class List {
 public:
  /// LPUSH key value — 左侧（头部）插入，返回列表长度
  static Status LPush(DB* db, const Slice& key, const Slice& value,
                      size_t* new_len = nullptr);

  /// RPUSH key value — 右侧（尾部）插入，返回列表长度
  static Status RPush(DB* db, const Slice& key, const Slice& value,
                      size_t* new_len = nullptr);

  /// LPOP key — 左侧弹出，返回弹出的值
  static Status LPop(DB* db, const Slice& key, std::string* value = nullptr);

  /// RPOP key — 右侧弹出，返回弹出的值
  static Status RPop(DB* db, const Slice& key, std::string* value = nullptr);

  /// LLEN key — 获取列表长度
  static Status LLen(DB* db, const Slice& key, size_t* len);

  /// LINDEX key index — 按下标获取元素（0=头, -1=尾）
  static Status LIndex(DB* db, const Slice& key, int64_t index,
                       std::string* value);

  /// LRANGE key start stop — 获取子列表
  static Status LRange(DB* db, const Slice& key, int64_t start,
                       int64_t stop,
                       std::vector<std::string>* values);

  /// Encode / Decode — 序列化工具（公开以便测试和扩展）
  static std::string Encode(const std::vector<std::string>& elems);
  static bool Decode(const std::string& data,
                     std::vector<std::string>* elems);
};

}  // namespace kv
