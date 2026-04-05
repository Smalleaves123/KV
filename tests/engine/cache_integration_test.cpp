#include "kv/engine/db.h"

#include <filesystem>
#include <sstream>
#include <string>

#include "gtest/gtest.h"

namespace kv {
namespace {

DBOptions MakeDBOptionsWithCache(const std::string& name) {
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
  options.cache_enabled = true;
  options.cache_policy = CachePolicy::kLRU;
  options.cache_capacity = 1024;
  options.cache_default_ttl_ms = 0;
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

TEST(CacheIntegrationTest, SSTReadBackFillsAndHitsCache) {
  DBOptions options = MakeDBOptionsWithCache("sst_cache_fill");
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());

  ASSERT_TRUE(db->Put(WriteOptions{}, "k", "v").ok());

  CacheStats stats0;
  ASSERT_TRUE(db->GetCacheStats(&stats0).ok());

  std::string value;
  ASSERT_TRUE(db->Get(ReadOptions{}, "k", &value).ok());
  ASSERT_EQ(value, "v");

  CacheStats stats1;
  ASSERT_TRUE(db->GetCacheStats(&stats1).ok());
  EXPECT_GE(stats1.miss, stats0.miss + 1);

  ASSERT_TRUE(db->Get(ReadOptions{}, "k", &value).ok());
  ASSERT_EQ(value, "v");

  CacheStats stats2;
  ASSERT_TRUE(db->GetCacheStats(&stats2).ok());
  EXPECT_GE(stats2.hit, stats1.hit + 1);
}

TEST(CacheIntegrationTest, SnapshotReadDoesNotUseCache) {
  DBOptions options = MakeDBOptionsWithCache("snapshot_no_cache");
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());

  ASSERT_TRUE(db->Put(WriteOptions{}, "k", "v1").ok());
  const Snapshot* snap = db->GetSnapshot();
  ASSERT_NE(snap, nullptr);

  ASSERT_TRUE(db->Put(WriteOptions{}, "k", "v2").ok());

  CacheStats before;
  ASSERT_TRUE(db->GetCacheStats(&before).ok());

  ReadOptions ro;
  ro.snapshot = snap;
  std::string value;
  ASSERT_TRUE(db->Get(ro, "k", &value).ok());
  EXPECT_EQ(value, "v1");

  CacheStats after;
  ASSERT_TRUE(db->GetCacheStats(&after).ok());
  EXPECT_EQ(after.hit, before.hit);
  EXPECT_EQ(after.miss, before.miss);
  EXPECT_EQ(after.evict, before.evict);
  EXPECT_EQ(after.expire, before.expire);

  ASSERT_TRUE(db->ReleaseSnapshot(snap).ok());
}

}  // namespace
}  // namespace kv

