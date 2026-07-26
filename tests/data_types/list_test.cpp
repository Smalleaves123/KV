#include "kv/data_types/list.h"
#include "kv/engine/db.h"

#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

namespace kv {
namespace {

std::string TestDir(const std::string& name) {
  static int counter = 0;
  ++counter;
  return "test_tmp/data_types/list_" + name + "_" + std::to_string(counter);
}

class ListTest : public ::testing::Test {
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

TEST_F(ListTest, LPushAndLPop) {
  auto db = OpenDB();

  size_t len = 0;
  ASSERT_TRUE(List::LPush(db.get(), "q", "c", &len).ok());
  EXPECT_EQ(len, 1u);
  ASSERT_TRUE(List::LPush(db.get(), "q", "b", &len).ok());
  EXPECT_EQ(len, 2u);
  ASSERT_TRUE(List::LPush(db.get(), "q", "a", &len).ok());
  EXPECT_EQ(len, 3u);

  std::string value;
  ASSERT_TRUE(List::LPop(db.get(), "q", &value).ok());
  EXPECT_EQ(value, "a");

  ASSERT_TRUE(List::LPop(db.get(), "q", &value).ok());
  EXPECT_EQ(value, "b");

  ASSERT_TRUE(List::LPop(db.get(), "q", &value).ok());
  EXPECT_EQ(value, "c");

  // 空列表
  Status s = List::LPop(db.get(), "q", &value);
  EXPECT_TRUE(s.IsNotFound());
}

TEST_F(ListTest, RPushAndRPop) {
  auto db = OpenDB();

  ASSERT_TRUE(List::RPush(db.get(), "q", "a").ok());
  ASSERT_TRUE(List::RPush(db.get(), "q", "b").ok());
  ASSERT_TRUE(List::RPush(db.get(), "q", "c").ok());

  std::string value;
  ASSERT_TRUE(List::RPop(db.get(), "q", &value).ok());
  EXPECT_EQ(value, "c");

  ASSERT_TRUE(List::RPop(db.get(), "q", &value).ok());
  EXPECT_EQ(value, "b");

  ASSERT_TRUE(List::RPop(db.get(), "q", &value).ok());
  EXPECT_EQ(value, "a");

  Status s = List::RPop(db.get(), "q", &value);
  EXPECT_TRUE(s.IsNotFound());
}

TEST_F(ListTest, LLen) {
  auto db = OpenDB();

  size_t len = 999;
  ASSERT_TRUE(List::LLen(db.get(), "q", &len).ok());
  EXPECT_EQ(len, 0u);

  ASSERT_TRUE(List::RPush(db.get(), "q", "a").ok());
  ASSERT_TRUE(List::RPush(db.get(), "q", "b").ok());

  ASSERT_TRUE(List::LLen(db.get(), "q", &len).ok());
  EXPECT_EQ(len, 2u);

  ASSERT_TRUE(List::LPop(db.get(), "q").ok());
  ASSERT_TRUE(List::LLen(db.get(), "q", &len).ok());
  EXPECT_EQ(len, 1u);
}

TEST_F(ListTest, LIndex) {
  auto db = OpenDB();
  ASSERT_TRUE(List::RPush(db.get(), "q", "a").ok());
  ASSERT_TRUE(List::RPush(db.get(), "q", "b").ok());
  ASSERT_TRUE(List::RPush(db.get(), "q", "c").ok());

  std::string value;
  ASSERT_TRUE(List::LIndex(db.get(), "q", 0, &value).ok());
  EXPECT_EQ(value, "a");

  ASSERT_TRUE(List::LIndex(db.get(), "q", 1, &value).ok());
  EXPECT_EQ(value, "b");

  ASSERT_TRUE(List::LIndex(db.get(), "q", -1, &value).ok());
  EXPECT_EQ(value, "c");

  ASSERT_TRUE(List::LIndex(db.get(), "q", -2, &value).ok());
  EXPECT_EQ(value, "b");

  EXPECT_TRUE(List::LIndex(db.get(), "q", 3, &value).IsNotFound());
  EXPECT_TRUE(List::LIndex(db.get(), "q", -4, &value).IsNotFound());
}

TEST_F(ListTest, LRange) {
  auto db = OpenDB();
  ASSERT_TRUE(List::RPush(db.get(), "q", "a").ok());
  ASSERT_TRUE(List::RPush(db.get(), "q", "b").ok());
  ASSERT_TRUE(List::RPush(db.get(), "q", "c").ok());
  ASSERT_TRUE(List::RPush(db.get(), "q", "d").ok());
  ASSERT_TRUE(List::RPush(db.get(), "q", "e").ok());

  std::vector<std::string> vals;
  ASSERT_TRUE(List::LRange(db.get(), "q", 1, 3, &vals).ok());
  ASSERT_EQ(vals.size(), 3u);
  EXPECT_EQ(vals[0], "b");
  EXPECT_EQ(vals[1], "c");
  EXPECT_EQ(vals[2], "d");

  // 负索引
  ASSERT_TRUE(List::LRange(db.get(), "q", -3, -1, &vals).ok());
  ASSERT_EQ(vals.size(), 3u);
  EXPECT_EQ(vals[0], "c");
  EXPECT_EQ(vals[1], "d");
  EXPECT_EQ(vals[2], "e");

  // 全范围
  ASSERT_TRUE(List::LRange(db.get(), "q", 0, -1, &vals).ok());
  EXPECT_EQ(vals.size(), 5u);

  ASSERT_TRUE(List::LRange(db.get(), "q", 10, 20, &vals).ok());
  EXPECT_TRUE(vals.empty());
  ASSERT_TRUE(List::LRange(db.get(), "q", -20, -10, &vals).ok());
  EXPECT_TRUE(vals.empty());
}

TEST_F(ListTest, QueueFIFO) {
  auto db = OpenDB();
  // RPush + LPop = 队列 (FIFO)
  ASSERT_TRUE(List::RPush(db.get(), "q", "first").ok());
  ASSERT_TRUE(List::RPush(db.get(), "q", "second").ok());
  ASSERT_TRUE(List::RPush(db.get(), "q", "third").ok());

  std::string value;
  ASSERT_TRUE(List::LPop(db.get(), "q", &value).ok());
  EXPECT_EQ(value, "first");

  ASSERT_TRUE(List::LPop(db.get(), "q", &value).ok());
  EXPECT_EQ(value, "second");

  ASSERT_TRUE(List::LPop(db.get(), "q", &value).ok());
  EXPECT_EQ(value, "third");
}

TEST_F(ListTest, StackLIFO) {
  auto db = OpenDB();
  // LPush + LPop = 栈 (LIFO)
  ASSERT_TRUE(List::LPush(db.get(), "s", "bottom").ok());
  ASSERT_TRUE(List::LPush(db.get(), "s", "middle").ok());
  ASSERT_TRUE(List::LPush(db.get(), "s", "top").ok());

  std::string value;
  ASSERT_TRUE(List::LPop(db.get(), "s", &value).ok());
  EXPECT_EQ(value, "top");

  ASSERT_TRUE(List::LPop(db.get(), "s", &value).ok());
  EXPECT_EQ(value, "middle");

  ASSERT_TRUE(List::LPop(db.get(), "s", &value).ok());
  EXPECT_EQ(value, "bottom");
}

}  // namespace
}  // namespace kv
