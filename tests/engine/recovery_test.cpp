#include "kv/engine/recovery.h"

#include <filesystem>
#include <cstdio>
#include <fstream>
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

void TruncateFileTail(const std::string& file_path, uintmax_t bytes_to_remove) {
  std::error_code ec;
  const auto size = std::filesystem::file_size(file_path, ec);
  ASSERT_FALSE(ec);
  ASSERT_GT(size, bytes_to_remove);
  std::filesystem::resize_file(file_path, size - bytes_to_remove, ec);
  ASSERT_FALSE(ec);
}

void RemoveAllSSTFiles(const std::string& dir_path) {
  std::error_code ec;
  if (!std::filesystem::exists(dir_path, ec)) {
    ASSERT_FALSE(ec);
    return;
  }

  for (const auto& entry : std::filesystem::directory_iterator(dir_path, ec)) {
    ASSERT_FALSE(ec);
    if (entry.is_regular_file() && entry.path().extension() == ".sst") {
      std::filesystem::remove(entry.path(), ec);
      ASSERT_FALSE(ec);
    }
  }
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

TEST(RecoveryTest, DBOpenIgnoresTruncatedTrailingWALRecord) {
  DBOptions options = MakeRecoveryDBOptions("db_truncated_wal_tail");
  options.memtable_write_buffer_size = 1024 * 1024;
  RemoveDBFiles(options);

  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "first", "v1").ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "second", "v2").ok());
    ASSERT_TRUE(db->Close().ok());
  }

  TruncateFileTail(options.wal_path, 4);

  DBOptions reopen_options = options;
  reopen_options.create_if_missing = false;
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(reopen_options, &db).ok());

  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions{}, "first", &value).ok());
  EXPECT_EQ(value, "v1");
  EXPECT_TRUE(db->Get(ReadOptions{}, "second", &value).IsNotFound());
}

TEST(RecoveryTest, DBOpenRejectsWALChecksumCorruption) {
  DBOptions options = MakeRecoveryDBOptions("db_wal_checksum_corruption");
  options.memtable_write_buffer_size = 1024 * 1024;
  RemoveDBFiles(options);

  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "key", "value").ok());
    ASSERT_TRUE(db->Close().ok());
  }

  std::fstream wal(options.wal_path,
                   std::ios::binary | std::ios::in | std::ios::out);
  ASSERT_TRUE(wal.is_open());
  char checksum_byte = 0;
  wal.read(&checksum_byte, 1);
  ASSERT_EQ(wal.gcount(), 1);
  wal.seekp(0);
  wal.put(static_cast<char>(checksum_byte ^ 0xFF));
  wal.close();

  DBOptions reopen_options = options;
  reopen_options.create_if_missing = false;
  std::unique_ptr<DB> db;
  Status s = DB::Open(reopen_options, &db);
  EXPECT_TRUE(s.IsCorruption()) << s.ToString();
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

TEST(RecoveryTest, ManifestAppendFailureCanDiscardUndurableManifestTail) {
  DBOptions options = MakeRecoveryDBOptions("manifest_append_before_sync");
  RemoveDBFiles(options);
  testing::ClearFailureInjection();

  uintmax_t durable_manifest_size = 0;
  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "live", "v1").ok());
    ASSERT_TRUE(db->Close().ok());

    std::error_code ec;
    durable_manifest_size = std::filesystem::file_size(options.manifest_path, ec);
    ASSERT_FALSE(ec);
  }

  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    testing::InjectFailure(
        testing::FailurePoint::kAfterManifestAppendBeforeSync,
        Status::IOError("injected failure before manifest sync"));
    Status s = db->Put(WriteOptions{}, "live", "v2");
    EXPECT_TRUE(s.IsIOError()) << s.ToString();
    testing::ClearFailureInjection();
    ASSERT_TRUE(db->Close().ok());
  }

  // Model a crash that loses the manifest record written after the last fsync.
  std::error_code ec;
  std::filesystem::resize_file(options.manifest_path, durable_manifest_size, ec);
  ASSERT_FALSE(ec);
  RemoveIfExists(options.wal_path);

  DBOptions reopen_options = options;
  reopen_options.create_if_missing = false;
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(reopen_options, &db).ok());
  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions{}, "live", &value).ok());
  EXPECT_EQ(value, "v1");
}

TEST(RecoveryTest, ManifestSyncBeforeWALCleanupRecoversFromSSTAndWAL) {
  DBOptions options = MakeRecoveryDBOptions("manifest_sync_before_wal_cleanup");
  RemoveDBFiles(options);
  testing::ClearFailureInjection();

  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    testing::InjectFailure(
        testing::FailurePoint::kAfterManifestSyncBeforeWALCleanup,
        Status::IOError("injected failure after manifest sync"));
    Status s = db->Put(WriteOptions{}, "live", "v1");
    EXPECT_TRUE(s.IsIOError()) << s.ToString();
    testing::ClearFailureInjection();
    ASSERT_TRUE(db->Close().ok());
  }

  DBOptions reopen_options = options;
  reopen_options.create_if_missing = false;
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(reopen_options, &db).ok());

  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions{}, "live", &value).ok());
  EXPECT_EQ(value, "v1");
  ASSERT_TRUE(db->Put(WriteOptions{}, "after_reopen", "v2").ok());
  ASSERT_TRUE(db->Close().ok());

  ASSERT_TRUE(DB::Open(reopen_options, &db).ok());
  ASSERT_TRUE(db->Get(ReadOptions{}, "live", &value).ok());
  EXPECT_EQ(value, "v1");
  ASSERT_TRUE(db->Get(ReadOptions{}, "after_reopen", &value).ok());
  EXPECT_EQ(value, "v2");
}

TEST(RecoveryTest, DBOpenIgnoresSSTableWithPartialTrailingManifestRecord) {
  DBOptions options = MakeRecoveryDBOptions("partial_manifest_tail");
  RemoveDBFiles(options);

  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "live", "v1").ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "live", "v2").ok());
    ASSERT_TRUE(db->Close().ok());
  }

  TruncateFileTail(options.manifest_path, 4);
  RemoveIfExists(options.wal_path);

  {
    DBOptions reopen_options = options;
    reopen_options.create_if_missing = false;

    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(reopen_options, &db).ok());

    std::string value;
    ASSERT_TRUE(db->Get(ReadOptions{}, "live", &value).ok());
    EXPECT_EQ(value, "v1");
  }
}

TEST(RecoveryTest, DBOpenRejectsMissingSSTableReferencedByManifest) {
  DBOptions options = MakeRecoveryDBOptions("missing_manifest_sstable");
  RemoveDBFiles(options);

  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "live", "value").ok());
    ASSERT_TRUE(db->Close().ok());
  }

  RemoveAllSSTFiles(options.sst_dir);
  RemoveIfExists(options.wal_path);

  DBOptions reopen_options = options;
  reopen_options.create_if_missing = false;

  std::unique_ptr<DB> db;
  Status s = DB::Open(reopen_options, &db);
  EXPECT_TRUE(s.IsCorruption()) << s.ToString();
}

}  // namespace
}  // namespace kv
