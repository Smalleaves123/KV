#include "kv/engine/db.h"

#include <filesystem>
#include <chrono>
#include <thread>
#include <sstream>
#include <string>

#include "gtest/gtest.h"
#include "kv/testing/failure_injection.h"
#include "kv/engine/write_applier.h"
#include "kv/engine/write_batch.h"
#include "kv/version/manifest.h"

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

TEST(CompactionTest, ConcurrentFlushKeepsVersionPriorityAfterCommitAndReopen) {
  DBOptions options = MakeCompactionDBOptions("compact_concurrent_flush");
  options.cache_enabled = true;
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);
  testing::ClearFailureInjection();
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "k", "v1").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "deleted", "old").ok());
  ASSERT_TRUE(db->Delete(WriteOptions{}, "resurrected").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "expired", "old").ok());

  bool hook_ran = false;
  testing::SetFailurePointHook(
      testing::FailurePoint::kDuringCompactionOutput, [&] {
        hook_ran = true;
        FlushStats before;
        ASSERT_TRUE(db->GetFlushStats(&before).ok());
        ASSERT_TRUE(db->Put(WriteOptions{}, "k", "v2").ok());
        ASSERT_TRUE(db->Delete(WriteOptions{}, "deleted").ok());
        ASSERT_TRUE(db->Put(WriteOptions{}, "resurrected", "new").ok());
        auto* applier = dynamic_cast<WriteApplier*>(db.get());
        ASSERT_NE(applier, nullptr);
        ASSERT_TRUE(applier->ApplyPutWithExpiry("expired", "new", 1).ok());
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::seconds(10);
        FlushStats after;
        do {
          ASSERT_TRUE(db->GetFlushStats(&after).ok());
          if (after.completed >= before.completed + 4) break;
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
        } while (std::chrono::steady_clock::now() < deadline);
        ASSERT_EQ(after.completed, before.completed + 4);
      });
  const Status compact_status = db->Compact();
  testing::ClearFailureInjection();
  ASSERT_TRUE(compact_status.ok()) << compact_status.ToString();
  ASSERT_TRUE(hook_ran);

  const auto check_versions = [&] {
    std::string value;
    // Read twice to cover both SST resolution and cache backfill.
    for (int i = 0; i < 2; ++i) {
      ASSERT_TRUE(db->Get(ReadOptions{}, "k", &value).ok());
      EXPECT_EQ(value, "v2");
      EXPECT_TRUE(db->Get(ReadOptions{}, "deleted", &value).IsNotFound());
      ASSERT_TRUE(db->Get(ReadOptions{}, "resurrected", &value).ok());
      EXPECT_EQ(value, "new");
      EXPECT_TRUE(db->Get(ReadOptions{}, "expired", &value).IsNotFound());
    }
  };
  ASSERT_NO_FATAL_FAILURE(check_versions());
  ASSERT_TRUE(db->Close().ok());
  db.reset();
  RemovePathIfExists(options.wal_path);
  options.create_if_missing = false;
  ASSERT_TRUE(DB::Open(options, &db).ok());
  ASSERT_NO_FATAL_FAILURE(check_versions());
}

TEST(CompactionTest, EmptyL2OutputRemovesTombstonesAndExpiredValues) {
  DBOptions options = MakeCompactionDBOptions("compact_empty_l2");
  options.compaction_min_input_files = 1;
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "deleted", "old").ok());
  ASSERT_TRUE(db->Delete(WriteOptions{}, "deleted").ok());
  auto* applier = dynamic_cast<WriteApplier*>(db.get());
  ASSERT_NE(applier, nullptr);
  ASSERT_TRUE(applier->ApplyPutWithExpiry("expired", "old", 1).ok());
  ASSERT_TRUE(db->Compact().ok());
  ASSERT_EQ(CountSSTFiles(options.sst_dir), 1U);

  // A failed empty commit must still roll back the manifest removals.
  testing::InjectFailure(testing::FailurePoint::kAfterCompactionAddBeforeRemove,
                         Status::IOError("empty output commit failure"));
  const Status failed = db->Compact();
  testing::ClearFailureInjection();
  ASSERT_TRUE(failed.IsIOError());
  EXPECT_EQ(CountSSTFiles(options.sst_dir), 1U);
  ASSERT_TRUE(db->Compact().ok());
  EXPECT_EQ(CountSSTFiles(options.sst_dir), 0U);
  CompactionStats stats;
  ASSERT_TRUE(db->GetCompactionStats(&stats).ok());
  EXPECT_EQ(stats.succeeded, 2U);
  EXPECT_EQ(stats.failed, 1U);

  ASSERT_TRUE(db->Close().ok());
  db.reset();
  Manifest manifest;
  ASSERT_TRUE(manifest.Open(options.manifest_path, false).ok());
  std::vector<ManifestFileMeta> files;
  ASSERT_TRUE(manifest.Recover(&files).ok());
  EXPECT_TRUE(files.empty());
  options.create_if_missing = false;
  ASSERT_TRUE(DB::Open(options, &db).ok());
  std::string value;
  EXPECT_TRUE(db->Get(ReadOptions{}, "deleted", &value).IsNotFound());
  EXPECT_TRUE(db->Get(ReadOptions{}, "expired", &value).IsNotFound());
  ASSERT_TRUE(db->Put(WriteOptions{}, "deleted", "new").ok());
  ASSERT_TRUE(db->Get(ReadOptions{}, "deleted", &value).ok());
  EXPECT_EQ(value, "new");
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

TEST(CompactionTest, AutoCompactionAcceptsEmptyL2Output) {
  DBOptions options = MakeCompactionDBOptions("auto_compaction_empty_l2");
  options.auto_compaction_enabled = true;
  options.compaction_min_input_files = 1;
  options.compaction_output_file_size_bytes = 128;
  options.sstable_block_size_bytes = 128;
  options.compaction_level_max_bytes = 0;
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());
  WriteBatch batch;
  for (int i = 0; i < 40; ++i) {
    batch.Delete("key" + std::to_string(100 + i));
  }
  ASSERT_TRUE(db->Write(WriteOptions{}, batch).ok());
  ASSERT_TRUE(db->Close().ok());
  CompactionStats stats;
  ASSERT_TRUE(db->GetCompactionStats(&stats).ok());
  EXPECT_EQ(stats.succeeded, 2U);
  EXPECT_EQ(stats.failed, 0U);
  EXPECT_EQ(CountSSTFiles(options.sst_dir), 0U);
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
