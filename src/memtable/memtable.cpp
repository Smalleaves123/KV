#include "kv/memtable/memtable.h"

#include <algorithm>
#include <limits>
#include <sstream>
#include <utility>

namespace kv {

MemTable::MemTable() : table_(), next_seq_(1), memory_usage_(0) {}

void MemTable::Iterator::Seek(const Slice& key) {
  MemTableEntry target;
  target.key = key.ToString();
  target.seq = std::numeric_limits<uint64_t>::max();
  target.type = RecordType::kValue;
  it_.Seek(target);
}

uint64_t MemTable::AcquireSequence() {
  return next_seq_++;
}

Status MemTable::Put(const Slice& key, const Slice& value) {
  return Add(AcquireSequence(), RecordType::kValue, key, value);
}

Status MemTable::Put(uint64_t seq, const Slice& key, const Slice& value) {
  return Add(seq, RecordType::kValue, key, value);
}

Status MemTable::Delete(const Slice& key) {
  return Add(AcquireSequence(), RecordType::kDeletion, key, Slice());
}

Status MemTable::Delete(uint64_t seq, const Slice& key) {
  return Add(seq, RecordType::kDeletion, key, Slice());
}

Status MemTable::Add(uint64_t seq,
                     RecordType type,
                     const Slice& key,
                     const Slice& value) {
  if (seq == 0) {
    return Status::InvalidArgument("sequence must be greater than 0");
  }

  MemTableEntry entry;
  entry.key = key.ToString();
  entry.value = value.ToString();
  entry.seq = seq;
  entry.type = type;

  memory_usage_ += sizeof(MemTableEntry);
  memory_usage_ += entry.key.size();
  memory_usage_ += entry.value.size();

  table_.Insert(std::move(entry));

  if (seq >= next_seq_) {
    next_seq_ = seq + 1;
  }

  return Status::OK();
}

Status MemTable::Get(const Slice& key, std::string* value) const {
  if (value == nullptr) {
    return Status::InvalidArgument("value output pointer is null");
  }

  MemTableEntry target;
  target.key = key.ToString();
  target.seq = std::numeric_limits<uint64_t>::max();
  target.type = RecordType::kValue;

  auto it = table_.FindGreaterOrEqual(target);
  if (!it.Valid()) {
    return Status::NotFound("key not found");
  }

  const MemTableEntry& entry = it.key();
  if (entry.key != target.key) {
    return Status::NotFound("key not found");
  }

  if (entry.type == RecordType::kDeletion) {
    return Status::NotFound("key deleted");
  }

  *value = entry.value;
  return Status::OK();
}

std::string MemTable::DebugString() const {
  std::ostringstream oss;

  auto it = table_.Begin();
  while (it.Valid()) {
    const auto& e = it.key();
    oss << "[key=" << e.key << ", seq=" << e.seq << ", type="
        << (e.type == RecordType::kValue ? "put" : "del")
        << ", value=" << e.value << "]\n";
    it.Next();
  }

  return oss.str();
}

}  // namespace kv