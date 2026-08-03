#include "kv/engine/db.h"

#include <filesystem>
#include <sstream>
#include <string>
#include <chrono>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "kv/engine/write_applier.h"

namespace kv {
namespace {

DBOptions MakeDBOptions(const std::string& name) {
  static int counter = 0;
  ++counter;

  DBOptions options;
  std::ostringstream oss;
  oss << "test_tmp/db/" << name << "_" << counter;
  const std::string base = oss.str();

  options.wal_path = base + ".wal";
  options.sst_dir = base + "_sst";
  options.manifest_path = base + ".manifest";
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
      return count;
    }
    if (entry.is_regular_file() && entry.path().extension() == ".sst") {
      ++count;
    }
  }
  return count;
}

TEST(DBTest, OpenNullOutputIsRejected) {
  DBOptions options;
  Status s = DB::Open(options, nullptr);

  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST(DBTest, PutGetDeleteWorks) {
  DBOptions options = MakeDBOptions("put_get_delete");
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  Status s = DB::Open(options, &db);
  ASSERT_TRUE(s.ok());
  ASSERT_NE(db, nullptr);

  s = db->Put(WriteOptions{}, "k1", "v1");
  ASSERT_TRUE(s.ok());

  std::string value;
  s = db->Get(ReadOptions{}, "k1", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "v1");

  s = db->Delete(WriteOptions{}, "k1");
  ASSERT_TRUE(s.ok());

  s = db->Get(ReadOptions{}, "k1", &value);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNotFound());

  EXPECT_TRUE(db->Close().ok());
}

TEST(DBTest, DataTTLSupportsSnapshotsAndSSTRecovery) {
  DBOptions options = MakeDBOptions("data_ttl");
  options.memtable_write_buffer_size = 1;
  options.auto_compaction_enabled = false;
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "session", "value").ok());

    const Snapshot* snapshot = db->GetSnapshot();
    ASSERT_NE(snapshot, nullptr);
    ASSERT_TRUE(db->Expire(WriteOptions{}, "session", 2).ok());

    int64_t ttl = -2;
    ASSERT_TRUE(db->TTL(ReadOptions{}, "session", &ttl).ok());
    EXPECT_GE(ttl, 1);
    EXPECT_LE(ttl, 2);

    ReadOptions snapshot_options;
    snapshot_options.snapshot = snapshot;
    std::string value;
    ASSERT_TRUE(db->Get(snapshot_options, "session", &value).ok());
    EXPECT_EQ(value, "value");
    ASSERT_TRUE(db->TTL(snapshot_options, "session", &ttl).ok());
    EXPECT_EQ(ttl, -1);

    ASSERT_TRUE(db->Persist(WriteOptions{}, "session").ok());
    ASSERT_TRUE(db->TTL(ReadOptions{}, "session", &ttl).ok());
    EXPECT_EQ(ttl, -1);
    ASSERT_TRUE(db->ReleaseSnapshot(snapshot).ok());
    ASSERT_TRUE(db->Close().ok());
  }

  RemovePathIfExists(options.wal_path);
  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "expiring", "value").ok());
    ASSERT_TRUE(db->Expire(WriteOptions{}, "expiring", 1).ok());
    ASSERT_TRUE(db->Close().ok());
  }
  RemovePathIfExists(options.wal_path);

  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    int64_t ttl = -2;
    ASSERT_TRUE(db->TTL(ReadOptions{}, "expiring", &ttl).ok());
    EXPECT_GE(ttl, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    std::string value;
    Status s = db->Get(ReadOptions{}, "expiring", &value);
    EXPECT_TRUE(s.IsNotFound());
    ASSERT_TRUE(db->TTL(ReadOptions{}, "expiring", &ttl).ok());
    EXPECT_EQ(ttl, -2);
  }
}

TEST(DBTest, ReopenReplaysWAL) {
  DBOptions options = MakeDBOptions("reopen_replay");
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  {
    std::unique_ptr<DB> db;
    Status s = DB::Open(options, &db);
    ASSERT_TRUE(s.ok());

    ASSERT_TRUE(db->Put(WriteOptions{}, "name", "alice").ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "name", "bob").ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "lang", "cpp").ok());
    ASSERT_TRUE(db->Delete(WriteOptions{}, "lang").ok());
    ASSERT_TRUE(db->Close().ok());
  }

  {
    std::unique_ptr<DB> db;
    Status s = DB::Open(options, &db);
    ASSERT_TRUE(s.ok());

    std::string value;
    s = db->Get(ReadOptions{}, "name", &value);
    ASSERT_TRUE(s.ok());
    EXPECT_EQ(value, "bob");

    s = db->Get(ReadOptions{}, "lang", &value);
    EXPECT_FALSE(s.ok());
    EXPECT_TRUE(s.IsNotFound());
  }
}

TEST(DBTest, CheckpointReopensWithoutSourceFiles) {
  DBOptions options = MakeDBOptions("checkpoint_reopen");
  options.memtable_write_buffer_size = 1024 * 1024;
  options.auto_compaction_enabled = false;
  const std::string checkpoint_dir = options.sst_dir + "_checkpoint";
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);
  RemoveDirIfExists(checkpoint_dir);

  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "present", "value").ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "deleted", "value").ok());
    ASSERT_TRUE(db->Delete(WriteOptions{}, "deleted").ok());
    ASSERT_TRUE(db->CreateCheckpoint(checkpoint_dir).ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "after-checkpoint", "later").ok());
    ASSERT_TRUE(db->Close().ok());
  }

  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  DBOptions checkpoint_options;
  checkpoint_options.db_path = checkpoint_dir;
  checkpoint_options.wal_path = checkpoint_dir + "/wal.log";
  checkpoint_options.sst_dir = checkpoint_dir + "/sst";
  checkpoint_options.manifest_path = checkpoint_dir + "/MANIFEST";
  checkpoint_options.create_if_missing = false;
  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(checkpoint_options, &db).ok());
    std::string value;
    ASSERT_TRUE(db->Get(ReadOptions{}, "present", &value).ok());
    EXPECT_EQ(value, "value");
    EXPECT_TRUE(db->Get(ReadOptions{}, "deleted", &value).IsNotFound());
    EXPECT_TRUE(
        db->Get(ReadOptions{}, "after-checkpoint", &value).IsNotFound());
    ASSERT_TRUE(db->Close().ok());
  }

  RemoveDirIfExists(checkpoint_dir);
}

TEST(DBTest, InstallCheckpointReplacesOpenState) {
  DBOptions source_options = MakeDBOptions("install_checkpoint_source");
  DBOptions target_options = MakeDBOptions("install_checkpoint_target");
  const std::string checkpoint_dir = source_options.sst_dir + "_checkpoint";
  RemovePathIfExists(source_options.wal_path);
  RemovePathIfExists(source_options.manifest_path);
  RemoveDirIfExists(source_options.sst_dir);
  RemoveDirIfExists(checkpoint_dir);
  RemovePathIfExists(target_options.wal_path);
  RemovePathIfExists(target_options.manifest_path);
  RemoveDirIfExists(target_options.sst_dir);

  std::unique_ptr<DB> source;
  ASSERT_TRUE(DB::Open(source_options, &source).ok());
  ASSERT_TRUE(source->Put(WriteOptions{}, "snapshot-key", "snapshot-value")
                  .ok());
  ASSERT_TRUE(source->CreateCheckpoint(checkpoint_dir).ok());
  ASSERT_TRUE(source->Close().ok());

  std::unique_ptr<DB> target;
  ASSERT_TRUE(DB::Open(target_options, &target).ok());
  ASSERT_TRUE(target->Put(WriteOptions{}, "stale-key", "stale-value").ok());
  auto* applier = dynamic_cast<WriteApplier*>(target.get());
  ASSERT_NE(applier, nullptr);
  ASSERT_TRUE(applier->InstallCheckpoint(checkpoint_dir).ok());

  std::string value;
  ASSERT_TRUE(target->Get(ReadOptions{}, "snapshot-key", &value).ok());
  EXPECT_EQ(value, "snapshot-value");
  EXPECT_TRUE(target->Get(ReadOptions{}, "stale-key", &value).IsNotFound());
  ASSERT_TRUE(target->Close().ok());

  RemovePathIfExists(source_options.wal_path);
  RemovePathIfExists(source_options.manifest_path);
  RemoveDirIfExists(source_options.sst_dir);
  RemovePathIfExists(target_options.wal_path);
  RemovePathIfExists(target_options.manifest_path);
  RemoveDirIfExists(target_options.sst_dir);
  RemoveDirIfExists(checkpoint_dir);
}

TEST(DBTest, CreateIfMissingFalseRejectsMissingWAL) {
  DBOptions options = MakeDBOptions("missing_reject");
  options.create_if_missing = false;
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  Status s = DB::Open(options, &db);

  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNotFound());
}

TEST(DBTest, ConcurrentPutAndReadback) {
  DBOptions options = MakeDBOptions("concurrent_put");
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());

  constexpr int kThreads = 8;
  constexpr int kPerThread = 100;

  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < kPerThread; ++i) {
        const std::string key =
            "k_" + std::to_string(t) + "_" + std::to_string(i);
        const std::string val = "v_" + std::to_string(i);
        ASSERT_TRUE(db->Put(WriteOptions{}, key, val).ok());
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  for (int t = 0; t < kThreads; ++t) {
    for (int i = 0; i < kPerThread; ++i) {
      const std::string key =
          "k_" + std::to_string(t) + "_" + std::to_string(i);
      std::string val;
      ASSERT_TRUE(db->Get(ReadOptions{}, key, &val).ok());
      ASSERT_EQ(val, "v_" + std::to_string(i));
    }
  }
}

TEST(DBTest, EmptyKeyRejected) {
  DBOptions options = MakeDBOptions("empty_key");
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());

  Status s = db->Put(WriteOptions{}, "", "v");
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST(DBTest, SnapshotReadsOldValue) {
  DBOptions options = MakeDBOptions("snapshot_old_value");
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());

  ASSERT_TRUE(db->Put(WriteOptions{}, "name", "v1").ok());
  const Snapshot* snap = db->GetSnapshot();
  ASSERT_NE(snap, nullptr);

  ASSERT_TRUE(db->Put(WriteOptions{}, "name", "v2").ok());

  ReadOptions ro;
  ro.snapshot = snap;

  std::string value;
  Status s = db->Get(ro, "name", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "v1");

  ASSERT_TRUE(db->ReleaseSnapshot(snap).ok());
}

TEST(DBTest, SnapshotReadsOldValueAfterFlush) {
  DBOptions options = MakeDBOptions("snapshot_after_flush");
  options.memtable_write_buffer_size = 1;
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());

  ASSERT_TRUE(db->Put(WriteOptions{}, "name", "v1").ok());
  const Snapshot* snap = db->GetSnapshot();
  ASSERT_NE(snap, nullptr);

  ASSERT_TRUE(db->Put(WriteOptions{}, "name", "v2").ok());

  ReadOptions ro;
  ro.snapshot = snap;

  std::string value;
  Status s = db->Get(ro, "name", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "v1");

  ASSERT_TRUE(db->ReleaseSnapshot(snap).ok());
}

TEST(DBTest, ReleaseInvalidSnapshotIsRejected) {
  DBOptions options = MakeDBOptions("snapshot_invalid_release");
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());

  Status s = db->ReleaseSnapshot(nullptr);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST(DBTest, FlushCreatesSSTAndCanReadBack) {
  DBOptions options = MakeDBOptions("flush_creates_sst");
  options.memtable_write_buffer_size = 1;
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());

  ASSERT_TRUE(db->Put(WriteOptions{}, "name", "tdmpc2").ok());
  Status flush_status = db->Compact();
  EXPECT_TRUE(flush_status.ok() || flush_status.IsNotFound())
      << flush_status.ToString();
  EXPECT_GE(CountSSTFiles(options.sst_dir), 1U);

  std::string value;
  Status s = db->Get(ReadOptions{}, "name", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "tdmpc2");
}

TEST(DBTest, ReopenCanLoadSSTWithoutWAL) {
  DBOptions options = MakeDBOptions("reopen_sst_only");
  options.memtable_write_buffer_size = 1;
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "lang", "cpp").ok());
    ASSERT_TRUE(db->Close().ok());
  }

  RemovePathIfExists(options.wal_path);

  {
    DBOptions reopen_options = options;
    reopen_options.create_if_missing = false;

    std::unique_ptr<DB> db;
    Status s = DB::Open(reopen_options, &db);
    ASSERT_TRUE(s.ok());

    std::string value;
    s = db->Get(ReadOptions{}, "lang", &value);
    ASSERT_TRUE(s.ok());
    EXPECT_EQ(value, "cpp");
  }
}

TEST(DBTest, CloseFlushesActiveMemTableBeforeClosingWAL) {
  DBOptions options = MakeDBOptions("close_flushes_active_memtable");
  options.memtable_write_buffer_size = 1024 * 1024;
  options.auto_compaction_enabled = false;
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  {
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    ASSERT_TRUE(db->Put(WriteOptions{}, "persisted", "value").ok());
    ASSERT_TRUE(db->Close().ok());
  }

  RemovePathIfExists(options.wal_path);
  DBOptions reopen_options = options;
  reopen_options.create_if_missing = false;
  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(reopen_options, &db).ok());
  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions{}, "persisted", &value).ok());
  EXPECT_EQ(value, "value");
}

TEST(DBTest, FlushStatsTrackCompletedBackgroundFlush) {
  DBOptions options = MakeDBOptions("flush_stats");
  options.memtable_write_buffer_size = 1;
  options.auto_compaction_enabled = false;
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "key", "value").ok());
  EXPECT_TRUE(db->Compact().IsNotFound());

  FlushStats stats;
  ASSERT_TRUE(db->GetFlushStats(&stats).ok());
  EXPECT_EQ(stats.completed, 1U);
  EXPECT_EQ(stats.failed, 0U);
  EXPECT_GE(stats.total_duration_us, 0U);
  EXPECT_EQ(stats.queue_length, 0U);
  EXPECT_TRUE(db->GetFlushStats(nullptr).IsInvalidArgument());
}

TEST(DBTest, FlushAppendsManifestRecord) {
  DBOptions options = MakeDBOptions("manifest_append");
  options.memtable_write_buffer_size = 1;
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "k", "v").ok());
  ASSERT_TRUE(db->Close().ok());

  std::error_code ec;
  EXPECT_TRUE(std::filesystem::exists(options.manifest_path, ec));
  ASSERT_FALSE(ec);
  EXPECT_GT(std::filesystem::file_size(options.manifest_path, ec), 0U);
  ASSERT_FALSE(ec);
}

}  // namespace
}  // namespace kv
