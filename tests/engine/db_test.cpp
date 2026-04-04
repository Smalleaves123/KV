#include "kv/engine/db.h"

#include <filesystem>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

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
