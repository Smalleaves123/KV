#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "kv/cache/cache.h"
#include "kv/common/slice.h"
#include "kv/common/status.h"
#include "kv/engine/iterator.h"
#include "kv/engine/transaction.h"

namespace kv {

class WriteBatch;
class Snapshot;

struct DBOptions {
  // 数据目录；当前版本主要用于推导 wal_path
  std::string db_path = "data/db";

  // 如果非空，优先使用这个 WAL 文件路径
  std::string wal_path;

  // 未设置 wal_path 时使用分段 WAL。段文件默认写入 db_path + "/wal"。
  bool wal_segmented = true;
  size_t wal_segment_size_bytes = 64 * 1024 * 1024;
  std::string wal_dir;

  // 打开时如果 WAL 不存在，是否允许创建新库
  bool create_if_missing = true;

  // 每次写入后是否默认调用 Sync
  bool sync_on_write = false;

  // MemTable 触发刷盘阈值（字节）
  size_t memtable_write_buffer_size = 4 * 1024 * 1024;

  // 后台 flush 前允许排队的 immutable MemTable 数量。
  size_t max_immutable_memtables = 2;

  // 如果为空，默认使用 db_path + "/sst"
  std::string sst_dir;

  // SSTable 数据块目标大小（字节）。
  size_t sstable_block_size_bytes = 4 * 1024;

  // 每个 SSTable key 使用的 Bloom filter bit 数。
  size_t bloom_bits_per_key = 10;

  // 已打开 SSTable reader 的 LRU 缓存容量。
  size_t table_cache_capacity = 64;

  // 如果为空，默认使用 db_path + "/MANIFEST"
  std::string manifest_path;

  // 是否开启 SST 读缓存
  bool cache_enabled = false;

  // 缓存策略（LRU/LFU/ShardLRU）
  CachePolicy cache_policy = CachePolicy::kLRU;

  // 缓存容量（条目数）
  size_t cache_capacity = 1024;

  // 默认 TTL（毫秒），0 表示不过期
  int64_t cache_default_ttl_ms = 0;

  // 手动/后台 compaction 最少输入 SST 文件数
  size_t compaction_min_input_files = 2;

  // 是否在 flush 后自动尝试 compaction
  bool auto_compaction_enabled = true;
};

struct ReadOptions {
  // 为空表示读取最新可见版本；非空则读取该快照序列号视图
  const Snapshot* snapshot = nullptr;
};

struct WriteOptions {
  bool sync = false;
};

struct ReadPathStats {
  uint64_t table_cache_hits = 0;
  uint64_t table_cache_misses = 0;
  uint64_t table_cache_evictions = 0;
  uint64_t table_cache_entries = 0;
  uint64_t bloom_queries = 0;
  uint64_t bloom_negatives = 0;
};

struct CompactionStats {
  uint64_t trigger_attempts = 0;
  uint64_t skipped_due_snapshot = 0;
  uint64_t skipped_due_threshold = 0;
  uint64_t succeeded = 0;
  uint64_t failed = 0;
};

class DB {
 public:
  virtual ~DB() = default;

  virtual bool IsOpen() const noexcept = 0;

  static Status Open(const DBOptions& options, std::unique_ptr<DB>* db);

  virtual Status Put(const WriteOptions& options,
                     const Slice& key,
                     const Slice& value) = 0;

  virtual Status Get(const ReadOptions& options,
                     const Slice& key,
                     std::string* value) = 0;

  virtual Status Delete(const WriteOptions& options,
                        const Slice& key) = 0;

  // Set a wall-clock expiry in seconds. A non-positive TTL deletes the key.
  virtual Status Expire(const WriteOptions& options, const Slice& key,
                        int64_t ttl_seconds) = 0;
  // Redis-compatible result: -2 missing/expired, -1 persistent, otherwise
  // remaining whole seconds (rounded down).
  virtual Status TTL(const ReadOptions& options, const Slice& key,
                     int64_t* ttl_seconds) = 0;
  virtual Status Persist(const WriteOptions& options, const Slice& key) = 0;

  virtual Status Write(const WriteOptions& options,
                       const WriteBatch& batch) = 0;

  virtual Status BeginTransaction(const TxnOptions& options,
                                  std::unique_ptr<Transaction>* txn) = 0;
  virtual Status Compact() = 0;

  virtual Status GetCacheStats(CacheStats* stats) const = 0;
  virtual Status GetReadPathStats(ReadPathStats* stats) const = 0;
  virtual Status GetCompactionStats(CompactionStats* stats) const = 0;

  virtual const Snapshot* GetSnapshot() = 0;
  virtual Status ReleaseSnapshot(const Snapshot* snapshot) = 0;

  virtual Status Close() = 0;

  // Return a key-ordered iterator over entries visible at the given snapshot
  // (or the current state if options.snapshot is null).
  // The iterator is created under a fixed snapshot of the DB state at call time:
  // entries added after NewIterator() are not visible.
  virtual std::unique_ptr<Iterator> NewIterator(const ReadOptions& options) = 0;

};

}  // namespace kv
