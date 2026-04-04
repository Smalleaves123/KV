#include "kv/engine/write_batch.h"

#include <filesystem>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "kv/engine/db.h"

namespace kv {
namespace {

class CollectingHandler final : public WriteBatch::Handler {
 public:
  Status Put(const Slice& key, const Slice& value) override {
    events_.push_back("put:" + key.ToString() + "=" + value.ToString());
    return Status::OK();
  }

  Status Delete(const Slice& key) override {
    events_.push_back("del:" + key.ToString());
    return Status::OK();
  }

  const std::vector<std::string>& events() const { return events_; }

 private:
  std::vector<std::string> events_;
};

DBOptions MakeWriteBatchDBOptions(const std::string& name) {
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

void RemoveDirIfExists(const std::string& path) {
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
}

TEST(WriteBatchTest, PutDeleteCountAndClear) {
  WriteBatch batch;
  EXPECT_TRUE(batch.Empty());
  EXPECT_EQ(batch.Count(), 0U);

  batch.Put("k1", "v1");
  batch.Delete("k2");

  EXPECT_FALSE(batch.Empty());
  EXPECT_EQ(batch.Count(), 2U);
  EXPECT_GT(batch.ApproximateSize(), 0U);

  batch.Clear();
  EXPECT_TRUE(batch.Empty());
  EXPECT_EQ(batch.Count(), 0U);
  EXPECT_EQ(batch.ApproximateSize(), 0U);
}

TEST(WriteBatchTest, IterateReplaysOperationsInOrder) {
  WriteBatch batch;
  batch.Put("a", "1");
  batch.Delete("b");
  batch.Put("c", "3");

  CollectingHandler handler;
  Status s = batch.Iterate(&handler);
  ASSERT_TRUE(s.ok());

  const auto& events = handler.events();
  ASSERT_EQ(events.size(), 3U);
  EXPECT_EQ(events[0], "put:a=1");
  EXPECT_EQ(events[1], "del:b");
  EXPECT_EQ(events[2], "put:c=3");
}

TEST(WriteBatchTest, IterateRejectsNullHandler) {
  WriteBatch batch;
  batch.Put("a", "1");
  Status s = batch.Iterate(nullptr);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST(WriteBatchTest, AppendCombinesOperations) {
  WriteBatch a;
  a.Put("a", "1");

  WriteBatch b;
  b.Delete("b");
  b.Put("c", "3");

  a.Append(b);
  EXPECT_EQ(a.Count(), 3U);

  CollectingHandler handler;
  ASSERT_TRUE(a.Iterate(&handler).ok());
  const auto& events = handler.events();
  ASSERT_EQ(events.size(), 3U);
  EXPECT_EQ(events[0], "put:a=1");
  EXPECT_EQ(events[1], "del:b");
  EXPECT_EQ(events[2], "put:c=3");
}

TEST(WriteBatchDBTest, DBWriteAppliesAllOperations) {
  DBOptions options = MakeWriteBatchDBOptions("batch_write");
  options.memtable_write_buffer_size = 1ULL << 20;
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());

  WriteBatch batch;
  batch.Put("name", "alice");
  batch.Put("lang", "cpp");
  batch.Delete("lang");
  batch.Put("name", "bob");

  ASSERT_TRUE(db->Write(WriteOptions{}, batch).ok());

  std::string value;
  Status s = db->Get(ReadOptions{}, "name", &value);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "bob");

  s = db->Get(ReadOptions{}, "lang", &value);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNotFound());
}

TEST(WriteBatchDBTest, DBWriteRejectsEmptyKey) {
  DBOptions options = MakeWriteBatchDBOptions("batch_invalid_key");
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());

  WriteBatch batch;
  batch.Put("", "value");
  batch.Put("ok", "1");

  Status s = db->Write(WriteOptions{}, batch);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

}  // namespace
}  // namespace kv
