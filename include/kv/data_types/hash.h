#pragma once

#include <cstddef>
#include <map>
#include <string>
#include <vector>

#include "kv/common/slice.h"
#include "kv/common/status.h"

namespace kv {

class DB;

/// Hash 数据类型 — 键下挂多个 field-value 对
///
/// 编码格式（单 KV 存储整个 Hash）：
///   [field1_len:4B][field1][value1_len:4B][value1]...
///
/// 优点：HSET/HGET 都是单次 DB 操作
/// 限制：不适合超大 Hash（整个 Hash 序列化后读写）
class Hash {
 public:
  /// HSET key field value — 设置字段，返回 1=新增, 0=覆盖
  static Status HSet(DB* db, const Slice& key, const Slice& field,
                     const Slice& value, int* created = nullptr);

  /// HGET key field — 读取字段
  static Status HGet(DB* db, const Slice& key, const Slice& field,
                     std::string* value);

  /// HDEL key field — 删除字段，返回删除的字段数 (0 或 1)
  static Status HDel(DB* db, const Slice& key, const Slice& field,
                     int* deleted);

  /// HEXISTS key field — 检查字段是否存在
  static Status HExists(DB* db, const Slice& key, const Slice& field,
                        bool* exists);

  /// HLEN key — 获取字段数量
  static Status HLen(DB* db, const Slice& key, size_t* len);

  /// HGETALL key — 获取所有 field-value 对
  static Status HGetAll(DB* db, const Slice& key,
                        std::map<std::string, std::string>* fields);

  /// HKEYS key — 获取所有字段名
  static Status HKeys(DB* db, const Slice& key,
                      std::vector<std::string>* field_names);

  /// HVALS key — 获取所有字段值
  static Status HVals(DB* db, const Slice& key,
                      std::vector<std::string>* values);

  /// Encode / Decode — 序列化工具（公开以便测试和扩展）
  static std::string Encode(
      const std::map<std::string, std::string>& fields);
  static bool Decode(const std::string& data,
                     std::map<std::string, std::string>* fields);
};

}  // namespace kv
