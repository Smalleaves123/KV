#pragma once

#include <memory>
#include <string>

#include "kv/common/slice.h"
#include "kv/common/status.h"

namespace kv {

struct DBOptions {
  // 数据目录；当前版本主要用于推导 wal_path
  std::string db_path = "data/db";

  // 如果非空，优先使用这个 WAL 文件路径
  std::string wal_path;

  // 打开时如果 WAL 不存在，是否允许创建新库
  bool create_if_missing = true;

  // 每次写入后是否默认调用 Sync
  bool sync_on_write = false;
};

struct ReadOptions {};

struct WriteOptions {
  bool sync = false;
};

class DB {
 public:
  virtual ~DB() = default;

  static Status Open(const DBOptions& options, std::unique_ptr<DB>* db);

  virtual Status Put(const WriteOptions& options,
                     const Slice& key,
                     const Slice& value) = 0;

  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value) = 0;

  virtual Status Delete(const WriteOptions& options,
                        const Slice& key) = 0;

  virtual Status Close() = 0;
};

}