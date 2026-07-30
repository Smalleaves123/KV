#include "kv/wal/wal_manager.h"

#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "kv/memtable/memtable.h"
#include "kv/wal/log_recovery.h"

namespace kv {
namespace {

std::string MakeWALDirectory(const std::string& name) {
  static int counter = 0;
  ++counter;
  std::ostringstream oss;
  oss << "test_tmp/wal_manager/" << name << "_" << counter;
  return oss.str();
}

void RemoveDirectory(const std::string& path) {
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
}

TEST(WALManagerTest, RotatesAndReplaysAllSegmentsInOrder) {
  const std::string wal_dir = MakeWALDirectory("rotate_replay");
  RemoveDirectory(wal_dir);

  WALOptions options;
  options.wal_dir = wal_dir;
  options.max_log_file_size = 1;

  WALManager manager;
  ASSERT_TRUE(manager.Open(options).ok());
  ASSERT_TRUE(manager.AppendPut(1, "key", "v1").ok());
  ASSERT_TRUE(manager.AppendPutWithTTL(
      2, "ttl", "v2", std::numeric_limits<uint64_t>::max()).ok());
  ASSERT_TRUE(manager.AppendDelete(3, "key").ok());

  std::vector<std::string> logs;
  ASSERT_TRUE(manager.ListLogs(&logs).ok());
  ASSERT_EQ(logs.size(), 4U);
  EXPECT_EQ(std::filesystem::path(logs[0]).filename(),
            "00000000000000000001.wal");
  EXPECT_EQ(std::filesystem::path(logs[3]).filename(),
            "00000000000000000004.wal");
  ASSERT_TRUE(manager.Close().ok());

  MemTable memtable;
  uint64_t max_seq = 0;
  ASSERT_TRUE(LogRecovery::ReplayLogs(logs, &memtable, &max_seq).ok());
  EXPECT_EQ(max_seq, 3U);

  std::string value;
  EXPECT_TRUE(memtable.Get("key", &value).IsNotFound());
  ASSERT_TRUE(memtable.Get("ttl", &value).ok());
  EXPECT_EQ(value, "v2");
}

TEST(WALManagerTest, ReopenUsesLargestExistingSegmentNumber) {
  const std::string wal_dir = MakeWALDirectory("reopen_largest_number");
  RemoveDirectory(wal_dir);
  ASSERT_TRUE(std::filesystem::create_directories(wal_dir));

  const std::string existing = wal_dir + "/00000000000000000007.wal";
  {
    std::ofstream out(existing, std::ios::binary);
    ASSERT_TRUE(out.is_open());
  }
  {
    std::ofstream out(wal_dir + "/not_a_segment.wal", std::ios::binary);
    ASSERT_TRUE(out.is_open());
  }

  WALOptions options;
  options.wal_dir = wal_dir;

  WALManager manager;
  ASSERT_TRUE(manager.Open(options).ok());
  EXPECT_EQ(std::filesystem::path(manager.active_log_path()).filename(),
            "00000000000000000008.wal");
  ASSERT_TRUE(manager.Close().ok());
}

}  // namespace
}  // namespace kv
