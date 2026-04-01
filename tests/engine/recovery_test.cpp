#include "kv/engine/recovery.h"

#include <cstdio>
#include <sstream>
#include <string>

#include "gtest/gtest.h"
#include "kv/memtable/memtable.h"
#include "kv/wal/wal_writer.h"

namespace kv {
namespace {

std::string MakeRecoveryTestPath(const std::string& file_name) {
  static int counter = 0;
  ++counter;

  std::ostringstream oss;
  oss << "test_tmp/wal/" << file_name << "_" << counter << ".log";
  return oss.str();
}

void RemoveIfExists(const std::string& file_path) {
  std::remove(file_path.c_str());
}

TEST(RecoveryTest, ReplayWALRestoresLatestState) {
  const std::string path = MakeRecoveryTestPath("replay_latest_state");
  RemoveIfExists(path);

  WALWriter writer;
  ASSERT_TRUE(writer.Open(path, false).ok());
  ASSERT_TRUE(writer.AppendPut(1, "name", "td").ok());
  ASSERT_TRUE(writer.AppendPut(2, "name", "tdmpc2").ok());
  ASSERT_TRUE(writer.AppendPut(3, "lang", "cpp").ok());
  ASSERT_TRUE(writer.AppendDelete(4, "lang").ok());
  ASSERT_TRUE(writer.Close().ok());

  MemTable mem;
  uint64_t max_seq = 0;

  Status s = Recovery::ReplayWAL(path, &mem, &max_seq);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(max_seq, 4U);

  std::string value;
  s = mem.Get("name", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "tdmpc2");

  s = mem.Get("lang", &value);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNotFound());
}

TEST(RecoveryTest, ReplayMissingFileFails) {
  MemTable mem;
  Status s = Recovery::ReplayWAL("test_tmp/wal/no_such_recovery_file.log", &mem);

  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());
}

TEST(RecoveryTest, NullMemTableIsRejected) {
  Status s = Recovery::ReplayWAL("dummy.log", nullptr);

  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST(RecoveryTest, ReplayReturnsMaxSequence) {
  const std::string path = MakeRecoveryTestPath("max_sequence");
  RemoveIfExists(path);

  WALWriter writer;
  ASSERT_TRUE(writer.Open(path, false).ok());
  ASSERT_TRUE(writer.AppendPut(100, "a", "1").ok());
  ASSERT_TRUE(writer.AppendPut(120, "b", "2").ok());
  ASSERT_TRUE(writer.AppendDelete(121, "a").ok());
  ASSERT_TRUE(writer.Close().ok());

  MemTable mem;
  uint64_t max_seq = 0;

  Status s = Recovery::ReplayWAL(path, &mem, &max_seq);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(max_seq, 121U);
}

}  // namespace
}  // namespace kv