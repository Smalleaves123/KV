#include "kv/engine/db.h"

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
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

void RemoveDirIfExists(const std::string& dir) {
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
}

void CleanupDB(const DBOptions& options) {
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);
}

std::unique_ptr<DB> OpenDB(DBOptions options) {
  CleanupDB(options);
  std::unique_ptr<DB> db;
  Status s = DB::Open(options, &db);
  EXPECT_TRUE(s.ok()) << s.ToString();
  return db;
}

// Collect all key-value pairs from an iterator into a vector.
std::vector<std::pair<std::string, std::string>> CollectIterator(Iterator* it) {
  std::vector<std::pair<std::string, std::string>> result;
  for (; it->Valid(); it->Next()) {
    result.push_back({it->key().ToString(), it->value().ToString()});
  }
  return result;
}

TEST(IteratorTest, EmptyDB) {
  auto db = OpenDB(MakeDBOptions("empty_db"));
  auto it = db->NewIterator(ReadOptions{});
  ASSERT_NE(it, nullptr);
  it->SeekToFirst();
  EXPECT_FALSE(it->Valid());

  it->Seek("anything");
  EXPECT_FALSE(it->Valid());
}

TEST(IteratorTest, SingleKey) {
  auto db = OpenDB(MakeDBOptions("single_key"));
  ASSERT_TRUE(db->Put(WriteOptions{}, "a", "1").ok());

  auto it = db->NewIterator(ReadOptions{});
  ASSERT_NE(it, nullptr);

  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->key().ToString(), "a");
  EXPECT_EQ(it->value().ToString(), "1");

  it->Next();
  EXPECT_FALSE(it->Valid());
}

TEST(IteratorTest, MultipleKeysOrdered) {
  auto db = OpenDB(MakeDBOptions("multi_key"));
  ASSERT_TRUE(db->Put(WriteOptions{}, "c", "3").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "a", "1").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "b", "2").ok());

  auto it = db->NewIterator(ReadOptions{});
  ASSERT_NE(it, nullptr);
  it->SeekToFirst();

  auto entries = CollectIterator(it.get());
  ASSERT_EQ(entries.size(), 3U);
  EXPECT_EQ(entries[0].first, "a");
  EXPECT_EQ(entries[0].second, "1");
  EXPECT_EQ(entries[1].first, "b");
  EXPECT_EQ(entries[1].second, "2");
  EXPECT_EQ(entries[2].first, "c");
  EXPECT_EQ(entries[2].second, "3");
}

TEST(IteratorTest, SeekPositionsCorrectly) {
  auto db = OpenDB(MakeDBOptions("seek_test"));
  ASSERT_TRUE(db->Put(WriteOptions{}, "a", "1").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "c", "3").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "e", "5").ok());

  auto it = db->NewIterator(ReadOptions{});
  ASSERT_NE(it, nullptr);

  // Seek to existing key
  it->Seek("c");
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->key().ToString(), "c");
  EXPECT_EQ(it->value().ToString(), "3");

  // Seek to non-existing key (between)
  it->Seek("d");
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->key().ToString(), "e");

  // Seek past end
  it->Seek("z");
  EXPECT_FALSE(it->Valid());
}

TEST(IteratorTest, SeeksToFirstAfterSeek) {
  auto db = OpenDB(MakeDBOptions("seek_reset"));
  ASSERT_TRUE(db->Put(WriteOptions{}, "a", "1").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "b", "2").ok());

  auto it = db->NewIterator(ReadOptions{});
  ASSERT_NE(it, nullptr);

  it->Seek("b");
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->key().ToString(), "b");

  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  EXPECT_EQ(it->key().ToString(), "a");
}

TEST(IteratorTest, DeleteSkipsTombstones) {
  auto db = OpenDB(MakeDBOptions("del_skip"));
  ASSERT_TRUE(db->Put(WriteOptions{}, "a", "1").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "b", "2").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "c", "3").ok());
  ASSERT_TRUE(db->Delete(WriteOptions{}, "b").ok());

  auto it = db->NewIterator(ReadOptions{});
  ASSERT_NE(it, nullptr);
  it->SeekToFirst();

  auto entries = CollectIterator(it.get());
  ASSERT_EQ(entries.size(), 2U);
  EXPECT_EQ(entries[0].first, "a");
  EXPECT_EQ(entries[1].first, "c");
}

TEST(IteratorTest, SnapshotExcludesWritesAfterSnapshot) {
  auto db = OpenDB(MakeDBOptions("snapshot_exclude"));

  ASSERT_TRUE(db->Put(WriteOptions{}, "a", "1").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "b", "2").ok());

  const Snapshot* snap = db->GetSnapshot();
  ASSERT_NE(snap, nullptr);

  ASSERT_TRUE(db->Put(WriteOptions{}, "c", "3").ok());
  ASSERT_TRUE(db->Delete(WriteOptions{}, "a").ok());

  ReadOptions read_opts;
  read_opts.snapshot = snap;

  auto it = db->NewIterator(read_opts);
  ASSERT_NE(it, nullptr);
  it->SeekToFirst();

  auto entries = CollectIterator(it.get());
  ASSERT_EQ(entries.size(), 2U);
  EXPECT_EQ(entries[0].first, "a");
  EXPECT_EQ(entries[0].second, "1");
  EXPECT_EQ(entries[1].first, "b");
  EXPECT_EQ(entries[1].second, "2");

  ASSERT_TRUE(db->ReleaseSnapshot(snap).ok());
}

TEST(IteratorTest, OverwriteReturnsLatestValue) {
  auto db = OpenDB(MakeDBOptions("overwrite"));

  ASSERT_TRUE(db->Put(WriteOptions{}, "k", "v1").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "k", "v2").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "k", "v3").ok());

  auto it = db->NewIterator(ReadOptions{});
  ASSERT_NE(it, nullptr);
  it->SeekToFirst();

  auto entries = CollectIterator(it.get());
  ASSERT_EQ(entries.size(), 1U);
  EXPECT_EQ(entries[0].first, "k");
  EXPECT_EQ(entries[0].second, "v3");
}

TEST(IteratorTest, ScanAfterFlushAcrossSSTAndMemtable) {
  DBOptions options = MakeDBOptions("scan_after_flush");
  options.memtable_write_buffer_size = 1;  // force flush every write
  auto db = OpenDB(std::move(options));

  // This forces a flush to SST
  ASSERT_TRUE(db->Put(WriteOptions{}, "a", "1").ok());
  // This stays in memtable
  ASSERT_TRUE(db->Put(WriteOptions{}, "b", "2").ok());

  auto it = db->NewIterator(ReadOptions{});
  ASSERT_NE(it, nullptr);
  it->SeekToFirst();

  auto entries = CollectIterator(it.get());
  ASSERT_EQ(entries.size(), 2U);
  EXPECT_EQ(entries[0].first, "a");
  EXPECT_EQ(entries[0].second, "1");
  EXPECT_EQ(entries[1].first, "b");
  EXPECT_EQ(entries[1].second, "2");
}

TEST(IteratorTest, ScanWithLimit) {
  auto db = OpenDB(MakeDBOptions("scan_limit"));

  for (char ch = 'a'; ch <= 'z'; ++ch) {
    ASSERT_TRUE(
        db->Put(WriteOptions{}, std::string(1, ch), std::string(1, ch)).ok());
  }

  auto it = db->NewIterator(ReadOptions{});
  ASSERT_NE(it, nullptr);
  it->SeekToFirst();

  // manually iterate first 5
  std::vector<std::string> keys;
  for (int i = 0; i < 5 && it->Valid(); it->Next(), ++i) {
    keys.push_back(it->key().ToString());
  }
  ASSERT_EQ(keys.size(), 5U);
  EXPECT_EQ(keys[0], "a");
  EXPECT_EQ(keys[4], "e");
}

}  // namespace

class IteratorTestEnv {
 public:
  IteratorTestEnv() {
    std::error_code ec;
    std::filesystem::create_directories("test_tmp/db", ec);
  }
} g_env;

}  // namespace kv
