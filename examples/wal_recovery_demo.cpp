#include "kv/engine/recovery.h"
#include "kv/memtable/memtable.h"
#include "kv/wal/wal_writer.h"

#include <filesystem>
#include <iostream>

int main() {
  const std::string path = "test_tmp/example_wal/recovery.log";
  std::filesystem::create_directories(std::filesystem::path(path).parent_path());

  {
    kv::WALWriter writer;
    kv::Status s = writer.Open(path, false);
    if (!s.ok()) {
      std::cerr << "open failed: " << s.ToString() << "\n";
      return 1;
    }
    if (!(s = writer.AppendPut(1, "name", "alice")).ok() ||
        !(s = writer.AppendDelete(2, "old")).ok() || !(s = writer.Close()).ok()) {
      std::cerr << "write failed: " << s.ToString() << "\n";
      return 1;
    }
  }

  kv::MemTable memtable;
  uint64_t max_seq = 0;
  kv::Status s = kv::Recovery::ReplayWAL(path, &memtable, &max_seq);
  if (!s.ok()) {
    std::cerr << "recovery failed: " << s.ToString() << "\n";
    return 1;
  }

  std::string value;
  if (!(s = memtable.Get("name", &value)).ok()) {
    std::cerr << "recovered get failed: " << s.ToString() << "\n";
    return 1;
  }
  std::cout << "recovered name = " << value << ", max_seq = " << max_seq << "\n";
  return 0;
}
