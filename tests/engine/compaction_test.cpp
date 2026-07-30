#include "kv/engine/db.h"

#include <filesystem>
#include <sstream>
#include <string>

#include "gtest/gtest.h"
#include "kv/testing/failure_injection.h"

namespace kv {
namespace {

DBOptions MakeCompactionDBOptions(const std::string& name) {
  static int counter = 0;
  ++counter;

  DBOptions options;
  std::ostringstream oss;
  oss << "test_tmp/db/" << name << "_" << counter;
  const std::string base = oss.str();

  options.wal_path = base + ".wal";
  options.sst_dir = base + "_sst";
  options.manifest_path = base + ".manifest";
  options.memtable_write_buffer_size = 1;
  options.auto_compaction_enabled = false;
  return options;
}

void RemovePathIfExists(const std::string& path) {
  std::error_code ec;
  std::filesystem::remove(path, ec);
}

void RemoveDirIfExists(const std::string& dir_path) {
  std::error_code ec;
  std::filesystem::remove_all(dir_path, ec);
}

size_t CountSSTFiles(const std::string& dir_path) {
  std::error_code ec;
  if (!std::filesystem::exists(dir_path, ec)) {
    return 0;
  }
  size_t count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(dir_path, ec)) {
    if (ec) {
      break;
    }
    if (entry.is_regular_file() && entry.path().extension() == ".sst") {
      ++count;
    }
  }
  return count;
}

TEST(CompactionTest, CompactKeepsLatestValueAndDeletion) {
  DBOptions options = MakeCompactionDBOptions("compact_latest_delete");
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());

  ASSERT_TRUE(db->Put(WriteOptions{}, "k", "v1").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "k", "v2").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "x", "x1").ok());
  ASSERT_TRUE(db->Delete(WriteOptions{}, "x").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "z", "z1").ok());

  const size_t before = CountSSTFiles(options.sst_dir);
  ASSERT_GE(before, 2U);

  ASSERT_TRUE(db->Compact().ok());

  const size_t after = CountSSTFiles(options.sst_dir);
  ASSERT_EQ(after, 1U);

  std::string value;
  Status s = db->Get(ReadOptions{}, "k", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "v2");

  s = db->Get(ReadOptions{}, "x", &value);
  EXPECT_TRUE(s.IsNotFound());

  s = db->Get(ReadOptions{}, "z", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "z1");

  ASSERT_TRUE(db->Close().ok());

  std::unique_ptr<DB> reopen_db;
  DBOptions reopen = options;
  reopen.create_if_missing = false;
  ASSERT_TRUE(DB::Open(reopen, &reopen_db).ok());
  EXPECT_EQ(CountSSTFiles(options.sst_dir), 1U);
}

TEST(CompactionTest, CompactRejectedWhenSnapshotActive) {
  DBOptions options = MakeCompactionDBOptions("compact_snapshot_guard");
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());

  ASSERT_TRUE(db->Put(WriteOptions{}, "a", "1").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "a", "2").ok());

  const Snapshot* snap = db->GetSnapshot();
  ASSERT_NE(snap, nullptr);

  Status s = db->Compact();
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsAlreadyExists());

  ASSERT_TRUE(db->ReleaseSnapshot(snap).ok());
  ASSERT_TRUE(db->Compact().ok());
}

TEST(CompactionTest, CompactNeedsEnoughInputFiles) {
  DBOptions options = MakeCompactionDBOptions("compact_need_enough_files");
  options.compaction_min_input_files = 2;
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "single", "v1").ok());

  Status s = db->Compact();
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNotFound());
}

TEST(CompactionTest, AutoCompactionTriggeredAfterFlush) {
  DBOptions options = MakeCompactionDBOptions("auto_compaction_triggered");
  options.auto_compaction_enabled = true;
  options.compaction_min_input_files = 2;
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());

  ASSERT_TRUE(db->Put(WriteOptions{}, "a", "1").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "b", "2").ok());
  Status compact_status = db->Compact();
  EXPECT_TRUE(compact_status.ok() || compact_status.IsNotFound())
      << compact_status.ToString();

  CompactionStats stats;
  ASSERT_TRUE(db->GetCompactionStats(&stats).ok());
  EXPECT_GE(stats.trigger_attempts, 1U);
  EXPECT_GE(stats.succeeded, 1U);
  EXPECT_EQ(CountSSTFiles(options.sst_dir), 1U);
}

TEST(CompactionTest, OutputBeforeManifestIsIgnoredDuringRecovery) {
  DBOptions options = MakeCompactionDBOptions("compaction_orphan_output");
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);
  testing::ClearFailureInjection();

  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "key", "v1").ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "key", "v2").ok());

    testing::InjectFailure(testing::FailurePoint::kDuringCompactionOutput,
                           Status::IOError("injected compaction output failure"));
    Status s = db->Compact();
    EXPECT_TRUE(s.IsIOError()) << s.ToString();
    testing::ClearFailureInjection();
    ASSERT_TRUE(db->Close().ok());
  }

  RemovePathIfExists(options.wal_path);
  DBOptions reopen_options = options;
  reopen_options.create_if_missing = false;
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(reopen_options, &db).ok());

  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions{}, "key", &value).ok());
  EXPECT_EQ(value, "v2");
  EXPECT_EQ(CountSSTFiles(options.sst_dir), 3U);
}

TEST(CompactionTest, ManifestAddBeforeRemoveRecoversWithOldFilesPresent) {
  DBOptions options = MakeCompactionDBOptions("compaction_add_before_remove");
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);
  testing::ClearFailureInjection();

  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "key", "v1").ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "key", "v2").ok());

    testing::InjectFailure(
        testing::FailurePoint::kAfterCompactionAddBeforeRemove,
        Status::IOError("injected compaction manifest failure"));
    Status s = db->Compact();
    EXPECT_TRUE(s.IsIOError()) << s.ToString();
    testing::ClearFailureInjection();
    ASSERT_TRUE(db->Close().ok());
  }

  RemovePathIfExists(options.wal_path);
  DBOptions reopen_options = options;
  reopen_options.create_if_missing = false;
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(reopen_options, &db).ok());

  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions{}, "key", &value).ok());
  EXPECT_EQ(value, "v2");
  EXPECT_EQ(CountSSTFiles(options.sst_dir), 3U);
}

}  // namespace
}  // namespace kv
