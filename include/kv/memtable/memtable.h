#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>

#include "kv/common/slice.h"
#include "kv/common/status.h"
#include "kv/memtable/skiplist.h"

namespace kv {

enum class RecordType : uint8_t {
  kValue = 0,
  kDeletion = 1,
};

struct MemTableEntry {
  std::string key;
  std::string value;
  uint64_t seq = 0;
  uint64_t expires_at_ms = 0;
  RecordType type = RecordType::kValue;
};

struct MemTableEntryComparator {
  bool operator()(const MemTableEntry& lhs,
                  const MemTableEntry& rhs) const noexcept {
    if (lhs.key < rhs.key) {
      return true;
    }
    if (lhs.key > rhs.key) {
      return false;
    }

    // 相同 user key 下，较新的 seq 排在前面
    if (lhs.seq != rhs.seq) {
      return lhs.seq > rhs.seq;
    }

    return static_cast<uint8_t>(lhs.type) < static_cast<uint8_t>(rhs.type);
  }
};

class MemTable {
 public:
  using Table = SkipList<MemTableEntry, MemTableEntryComparator>;

  class Iterator {
   public:
    Iterator() = default;

    bool Valid() const noexcept { return it_.Valid(); }

    void Next() { it_.Next(); }

    void SeekToFirst() { it_.SeekToFirst(); }

    void Seek(const Slice& key);

    const MemTableEntry& entry() const { return it_.key(); }

   private:
    friend class MemTable;
    explicit Iterator(Table::Iterator it) : it_(std::move(it)) {}

    Table::Iterator it_;
  };

  MemTable();

  Status Put(const Slice& key, const Slice& value);
  Status Put(uint64_t seq, const Slice& key, const Slice& value);
  Status Put(uint64_t seq, const Slice& key, const Slice& value,
             uint64_t expires_at_ms);

  Status Delete(const Slice& key);
  Status Delete(uint64_t seq, const Slice& key);

  Status Get(const Slice& key, std::string* value) const;

  bool Empty() const noexcept { return table_.Empty(); }
  size_t Size() const noexcept { return table_.Size(); }
  uint64_t LatestSequence() const noexcept {
    return next_seq_ == 0 ? 0 : next_seq_ - 1;
  }

  size_t ApproximateMemoryUsage() const noexcept { return memory_usage_; }
  void Clear();

  Iterator NewIterator() const { return Iterator(table_.Begin()); }

  std::string DebugString() const;

 private:
  Status Add(uint64_t seq,
             RecordType type,
             const Slice& key,
             const Slice& value,
             uint64_t expires_at_ms);

  uint64_t AcquireSequence();

  Table table_;
  uint64_t next_seq_;
  size_t memory_usage_;
};

}  // namespace kv
