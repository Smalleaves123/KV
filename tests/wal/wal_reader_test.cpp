#include "kv/engine/recovery.h"

#include <algorithm>
#include <cstdint>

#include "kv/memtable/memtable.h"
#include "kv/wal/log_record.h"
#include "kv/wal/wal_reader.h"

namespace kv {

Status Recovery::ReplayWAL(const std::string& wal_path, MemTable* memtable) {
  return ReplayWAL(wal_path, memtable, nullptr);
}

Status Recovery::ReplayWAL(const std::string& wal_path,
                           MemTable* memtable,
                           uint64_t* max_seq) {
  if (memtable == nullptr) {
    return Status::InvalidArgument("memtable is null");
  }

  WALReader reader;
  Status s = reader.Open(wal_path);
  if (!s.ok()) {
    return s;
  }

  uint64_t local_max_seq = 0;

  while (true) {
    LogRecord record;
    s = reader.ReadNext(&record);

    if (s.ok()) {
      if (record.type == LogRecordType::kPut) {
        s = memtable->Put(record.seq, record.key, record.value);
      } else if (record.type == LogRecordType::kDelete) {
        s = memtable->Delete(record.seq, record.key);
      } else {
        s = Status::Corruption("unknown log record type during recovery");
      }

      if (!s.ok()) {
        reader.Close();
        return s;
      }

      local_max_seq = std::max(local_max_seq, record.seq);
      continue;
    }

    if (s.IsNotFound()) {
      break;
    }

    reader.Close();
    return s;
  }

  reader.Close();

  if (max_seq != nullptr) {
    *max_seq = local_max_seq;
  }

  return Status::OK();
}

}  // namespace kv