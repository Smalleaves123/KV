#include "kv/engine/db.h"

#include <filesystem>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace kv {
namespace {

std::string MakeDBWalPath(const std::string& name) {
  static int counter = 0;
  ++counter;

  std::ostringstream oss;
  oss << "test_tmp/db/" << name << "_" << counter << ".wal";
  return oss.str();
}

void RemovePathIfExists(const std::string& path) {
  std::error_code ec;
  std::filesystem::remove(path, ec);
}

TEST(DBTest, OpenNullOutputIsRejected) {
  DBOptions options;
  Status s = DB::Open(options, nullptr);

  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST(DBTest, PutGetDeleteWorks) {
  DBOptions options;
  options.wal_path = MakeDBWalPath("put_get_delete");
  RemovePathIfExists(options.wal_path);

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
  DBOptions options;
  options.wal_path = MakeDBWalPath("reopen_replay");
  RemovePathIfExists(options.wal_path);

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
  DBOptions options;
  options.wal_path = MakeDBWalPath("missing_reject");
  options.create_if_missing = false;
  RemovePathIfExists(options.wal_path);

  std::unique_ptr<DB> db;
  Status s = DB::Open(options, &db);

  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNotFound());
}

TEST(DBTest, ConcurrentPutAndReadback) {
  DBOptions options;
  options.wal_path = MakeDBWalPath("concurrent_put");
  RemovePathIfExists(options.wal_path);

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
  DBOptions options;
  options.wal_path = MakeDBWalPath("empty_key");
  RemovePathIfExists(options.wal_path);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());

  Status s = db->Put(WriteOptions{}, "", "v");
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

}  // namespace
}  // namespace kv
