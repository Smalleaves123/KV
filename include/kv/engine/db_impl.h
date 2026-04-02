#pragma once

#include <cstdint>
#include <mutex>
#include <string>

#include "kv/engine/db.h"
#include "kv/memtable/memtable.h"
#include "kv/wal/wal_writer.h"

namespace kv {

class DBImpl final : public DB {
 public:
  explicit DBImpl(DBOptions options);
  ~DBImpl() override;

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  Status Init();

  Status Put(const WriteOptions& options,
             const Slice& key,
             const Slice& value) override;

  Status Get(const ReadOptions& options,
             const Slice& key,
             std::string* value) override;

  Status Delete(const WriteOptions& options,
                const Slice& key) override;

  Status Close() override;

  bool is_open() const noexcept;
  uint64_t LatestSequence() const noexcept;
  const std::string& wal_path() const noexcept;

 private:
  Status ApplyPut(uint64_t seq,
                  const WriteOptions& options,
                  const Slice& key,
                  const Slice& value);

  Status ApplyDelete(uint64_t seq,
                     const WriteOptions& options,
                     const Slice& key);

  bool ShouldSync(const WriteOptions& options) const noexcept;
  Status ValidateKey(const Slice& key) const;

  static std::string BuildWalPath(const DBOptions& options);

  DBOptions options_;
  std::string wal_path_;
  MemTable memtable_;
  WALWriter wal_writer_;
  uint64_t next_seq_;
  bool open_;
  mutable std::mutex mu_;
};

}  // namespace kv
