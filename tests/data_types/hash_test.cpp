#include "kv/data_types/hash.h"
#include "kv/engine/db.h"

#include <filesystem>
#include <gtest/gtest.h>
#include <map>

namespace kv {
namespace {

std::string TestDir(const std::string& name) {
  static int counter = 0;
  ++counter;
  return "test_tmp/data_types/hash_" + name + "_" + std::to_string(counter);
}

class HashTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = TestDir("default");
    std::error_code ec;
    std::filesystem::remove_all(dir_, ec);
  }

  std::unique_ptr<DB> OpenDB() {
    DBOptions opts;
    opts.db_path = dir_;
    std::unique_ptr<DB> db;
    EXPECT_TRUE(DB::Open(opts, &db).ok());
    return db;
  }

  std::string dir_;
};

TEST_F(HashTest, HSetAndHGet) {
  auto db = OpenDB();
  int created = -1;

  ASSERT_TRUE(Hash::HSet(db.get(), "user:1", "name", "Alice", &created).ok());
  EXPECT_EQ(created, 1);

  ASSERT_TRUE(Hash::HSet(db.get(), "user:1", "age", "30", &created).ok());
  EXPECT_EQ(created, 1);

  std::string value;
  ASSERT_TRUE(Hash::HGet(db.get(), "user:1", "name", &value).ok());
  EXPECT_EQ(value, "Alice");

  ASSERT_TRUE(Hash::HGet(db.get(), "user:1", "age", &value).ok());
  EXPECT_EQ(value, "30");
}

TEST_F(HashTest, HSetOverwrite) {
  auto db = OpenDB();
  int created = -1;

  ASSERT_TRUE(Hash::HSet(db.get(), "k", "f", "v1", &created).ok());
  EXPECT_EQ(created, 1);

  ASSERT_TRUE(Hash::HSet(db.get(), "k", "f", "v2", &created).ok());
  EXPECT_EQ(created, 0);  // 覆盖

  std::string value;
  ASSERT_TRUE(Hash::HGet(db.get(), "k", "f", &value).ok());
  EXPECT_EQ(value, "v2");
}

TEST_F(HashTest, HGetNotFound) {
  auto db = OpenDB();
  std::string value;
  Status s = Hash::HGet(db.get(), "nosuch", "field", &value);
  EXPECT_TRUE(s.IsNotFound());
}

TEST_F(HashTest, HDel) {
  auto db = OpenDB();
  ASSERT_TRUE(Hash::HSet(db.get(), "k", "f1", "v1").ok());
  ASSERT_TRUE(Hash::HSet(db.get(), "k", "f2", "v2").ok());

  int deleted = -1;
  ASSERT_TRUE(Hash::HDel(db.get(), "k", "f1", &deleted).ok());
  EXPECT_EQ(deleted, 1);

  std::string value;
  Status s = Hash::HGet(db.get(), "k", "f1", &value);
  EXPECT_TRUE(s.IsNotFound());

  // f2 还在
  ASSERT_TRUE(Hash::HGet(db.get(), "k", "f2", &value).ok());
  EXPECT_EQ(value, "v2");
}

TEST_F(HashTest, HDelNonExistentField) {
  auto db = OpenDB();
  ASSERT_TRUE(Hash::HSet(db.get(), "k", "f", "v").ok());

  int deleted = -1;
  ASSERT_TRUE(Hash::HDel(db.get(), "k", "nonexist", &deleted).ok());
  EXPECT_EQ(deleted, 0);
}

TEST_F(HashTest, HExists) {
  auto db = OpenDB();
  ASSERT_TRUE(Hash::HSet(db.get(), "k", "f", "v").ok());

  bool exists = false;
  ASSERT_TRUE(Hash::HExists(db.get(), "k", "f", &exists).ok());
  EXPECT_TRUE(exists);

  ASSERT_TRUE(Hash::HExists(db.get(), "k", "missing", &exists).ok());
  EXPECT_FALSE(exists);

  ASSERT_TRUE(Hash::HExists(db.get(), "nosuch", "f", &exists).ok());
  EXPECT_FALSE(exists);
}

TEST_F(HashTest, HLen) {
  auto db = OpenDB();
  size_t len = 999;

  ASSERT_TRUE(Hash::HLen(db.get(), "k", &len).ok());
  EXPECT_EQ(len, 0u);

  ASSERT_TRUE(Hash::HSet(db.get(), "k", "a", "1").ok());
  ASSERT_TRUE(Hash::HSet(db.get(), "k", "b", "2").ok());
  ASSERT_TRUE(Hash::HSet(db.get(), "k", "c", "3").ok());

  ASSERT_TRUE(Hash::HLen(db.get(), "k", &len).ok());
  EXPECT_EQ(len, 3u);
}

TEST_F(HashTest, HGetAll) {
  auto db = OpenDB();
  ASSERT_TRUE(Hash::HSet(db.get(), "prod", "price", "99").ok());
  ASSERT_TRUE(Hash::HSet(db.get(), "prod", "stock", "200").ok());

  std::map<std::string, std::string> fields;
  ASSERT_TRUE(Hash::HGetAll(db.get(), "prod", &fields).ok());
  EXPECT_EQ(fields.size(), 2u);
  EXPECT_EQ(fields["price"], "99");
  EXPECT_EQ(fields["stock"], "200");
}

TEST_F(HashTest, HKeysAndHVals) {
  auto db = OpenDB();
  ASSERT_TRUE(Hash::HSet(db.get(), "k", "x", "10").ok());
  ASSERT_TRUE(Hash::HSet(db.get(), "k", "y", "20").ok());

  std::vector<std::string> keys;
  ASSERT_TRUE(Hash::HKeys(db.get(), "k", &keys).ok());
  EXPECT_EQ(keys.size(), 2u);

  std::vector<std::string> vals;
  ASSERT_TRUE(Hash::HVals(db.get(), "k", &vals).ok());
  EXPECT_EQ(vals.size(), 2u);
}

TEST_F(HashTest, HDelLastFieldDeletesKey) {
  auto db = OpenDB();
  ASSERT_TRUE(Hash::HSet(db.get(), "k", "only", "v").ok());

  int deleted = -1;
  ASSERT_TRUE(Hash::HDel(db.get(), "k", "only", &deleted).ok());
  EXPECT_EQ(deleted, 1);

  // key 本身应该也不存在了
  size_t len = 999;
  ASSERT_TRUE(Hash::HLen(db.get(), "k", &len).ok());
  EXPECT_EQ(len, 0u);
}

}  // namespace
}  // namespace kv
