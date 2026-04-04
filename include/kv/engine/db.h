#pragma once

#include <cstddef>
#include <memory>
#include <string>

#include "kv/common/slice.h"
#include "kv/common/status.h"

namespace kv {

class WriteBatch;
class Snapshot;

struct DBOptions {
  // 数据目录；当前版本主要用于推导 wal_path
  std::string db_path = "data/db";

  // 如果非空，优先使用这个 WAL 文件路径
  std::string wal_path;

  // 打开时如果 WAL 不存在，是否允许创建新库
  bool create_if_missing = true;

  // 每次写入后是否默认调用 Sync
  bool sync_on_write = false;

  // MemTable 触发刷盘阈值（字节）
  size_t memtable_write_buffer_size = 4 * 1024 * 1024;

  // 如果为空，默认使用 db_path + "/sst"
  std::string sst_dir;

  // 如果为空，默认使用 db_path + "/MANIFEST"
  std::string manifest_path;
};

struct ReadOptions {
  // 为空表示读取最新可见版本；非空则读取该快照序列号视图
  const Snapshot* snapshot = nullptr;
};

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

  virtual Status Write(const WriteOptions& options,
                       const WriteBatch& batch) = 0;

  virtual const Snapshot* GetSnapshot() = 0;
  virtual Status ReleaseSnapshot(const Snapshot* snapshot) = 0;

  virtual Status Close() = 0;
};

}  // namespace kv
