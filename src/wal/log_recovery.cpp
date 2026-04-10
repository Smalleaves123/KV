#include "kv/wal/log_recovery.h"

#include <algorithm>
#include <vector>

#include "kv/memtable/memtable.h"
#include "kv/wal/wal_reader.h"
#include "kv/wal/log_record.h"

namespace kv {

Status LogRecovery::ReplayLogs(const std::vector<std::string>& wal_files,
                               MemTable* memtable,
                               uint64_t* max_seq) {
  if (memtable == nullptr || max_seq == nullptr) {
    return Status::InvalidArgument("null output");
  }
  *max_seq = 0;

  std::vector<std::string> files = wal_files;
  std::sort(files.begin(), files.end());

  for (const auto& path : files) {
    WALReader reader;
    Status s = reader.Open(path);
    if (!s.ok()) {
      return s;
    }

    LogRecord rec;
    while (true) {
      s = reader.ReadNext(&rec);
      if (s.IsNotFound()) {
        break;
      }
      if (!s.ok()) {
        return s;
      }

      *max_seq = std::max(*max_seq, rec.seq);
      if (rec.type == LogRecordType::kPut) {
        s = memtable->Put(rec.seq, rec.key, rec.value);
      } else if (rec.type == LogRecordType::kDelete) {
        s = memtable->Delete(rec.seq, rec.key);
      } else {
        return Status::Corruption("unknown log record type");
      }
      if (!s.ok()) {
        return s;
      }
    }

    s = reader.Close();
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

}  // namespace kv
