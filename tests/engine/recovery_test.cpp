#include "kv/engine/recovery.h"

#include <filesystem>
#include <cstdio>
#include <memory>
#include <sstream>
#include <string>

#include "gtest/gtest.h"
#include "kv/engine/db.h"
#include "kv/memtable/memtable.h"
#include "kv/sstable/table_builder.h"
#include "kv/testing/failure_injection.h"
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

DBOptions MakeRecoveryDBOptions(const std::string& name) {
  static int counter = 0;
  ++counter;

  std::ostringstream oss;
  oss << "test_tmp/recovery_db/" << name << "_" << counter;
  const std::string base = oss.str();

  DBOptions options;
  options.wal_path = base + ".wal";
  options.sst_dir = base + "_sst";
  options.manifest_path = base + ".manifest";
  options.memtable_write_buffer_size = 1;
  options.auto_compaction_enabled = false;
  return options;
}

void RemoveDBFiles(const DBOptions& options) {
  std::error_code ec;
  std::filesystem::remove(options.wal_path, ec);
  ec.clear();
  std::filesystem::remove(options.manifest_path, ec);
  ec.clear();
  std::filesystem::remove_all(options.sst_dir, ec);
}

void WriteSingleEntrySST(const std::string& file_path,
                         const std::string& key,
                         uint64_t seq,
                         const std::string& value) {
  TableBuilder builder(file_path, 256);
  ASSERT_TRUE(builder.Add(key, seq, 0, value).ok());
  ASSERT_TRUE(builder.Finish().ok());
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

TEST(RecoveryTest, TruncatedWALTrailingRecordIsIgnored) {
  const std::string path = MakeRecoveryTestPath("truncated_trailing");
  RemoveIfExists(path);

  // Write two complete records
  {
    WALWriter writer;
    ASSERT_TRUE(writer.Open(path, false).ok());
    ASSERT_TRUE(writer.AppendPut(10, "k1", "v1").ok());
    ASSERT_TRUE(writer.AppendPut(20, "k2", "v2").ok());
    ASSERT_TRUE(writer.Close().ok());
  }

  // Truncate the last few bytes so the trailing record is incomplete
  {
    std::string content;
    {
      std::ifstream in(path, std::ios::binary);
      std::ostringstream oss;
      oss << in.rdbuf();
      content = oss.str();
    }
    ASSERT_FALSE(content.empty());
    content.resize(content.size() - 4);
    {
      std::ofstream out(path, std::ios::binary | std::ios::trunc);
      out.write(content.data(), static_cast<std::streamsize>(content.size()));
    }
  }

  // Recovery should succeed, replaying only the first complete record
  MemTable mem;
  uint64_t max_seq = 0;
  Status s = Recovery::ReplayWAL(path, &mem, &max_seq);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(max_seq, 10U);

  std::string value;
  EXPECT_TRUE(mem.Get("k1", &value).ok());
  EXPECT_EQ(value, "v1");
  EXPECT_TRUE(mem.Get("k2", &value).IsNotFound());
}

TEST(RecoveryTest, DBOpenIgnoresOrphanSSTableWhenManifestExists) {
  DBOptions options = MakeRecoveryDBOptions("orphan_sstable");
  RemoveDBFiles(options);

  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "live", "manifested").ok());
    ASSERT_TRUE(db->Close().ok());
  }

  std::error_code ec;
  ASSERT_TRUE(std::filesystem::exists(options.manifest_path, ec));
  ASSERT_FALSE(ec);

  const std::string orphan_path =
      options.sst_dir + "/00000000000000099999.sst";
  WriteSingleEntrySST(orphan_path, "live", 99999, "orphan");

  RemoveIfExists(options.wal_path);

  {
    DBOptions reopen_options = options;
    reopen_options.create_if_missing = false;

    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(reopen_options, &db).ok());

    std::string value;
    ASSERT_TRUE(db->Get(ReadOptions{}, "live", &value).ok());
    EXPECT_EQ(value, "manifested");
  }
}

TEST(RecoveryTest, InjectedFlushFailureLeavesOrphanSSTableOutOfRecovery) {
  DBOptions options = MakeRecoveryDBOptions("injected_orphan_sstable");
  RemoveDBFiles(options);
  testing::ClearFailureInjection();

  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "live", "manifested").ok());
    ASSERT_TRUE(db->Close().ok());
  }

  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());

    testing::InjectFailure(
        testing::FailurePoint::kAfterSSTableWriteBeforeManifest,
        Status::IOError("injected failure after sstable write"));
    Status s = db->Put(WriteOptions{}, "live", "orphan");
    EXPECT_TRUE(s.IsIOError()) << s.ToString();
    testing::ClearFailureInjection();
  }

  RemoveIfExists(options.wal_path);

  {
    DBOptions reopen_options = options;
    reopen_options.create_if_missing = false;

    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(reopen_options, &db).ok());

    std::string value;
    ASSERT_TRUE(db->Get(ReadOptions{}, "live", &value).ok());
    EXPECT_EQ(value, "manifested");
  }
}

}  // namespace
}  // namespace kv
