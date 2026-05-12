#include "kv/data_types/counter.h"
#include "kv/engine/db.h"

#include <filesystem>
#include <gtest/gtest.h>

namespace kv {
namespace {

std::string TestDir(const std::string& name) {
  static int counter = 0;
  ++counter;
  return "test_tmp/data_types/counter_" + name + "_" + std::to_string(counter);
}

TEST(CounterTest, IncrFromZero) {
  const auto dir = TestDir("incr_from_zero");
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);

  DBOptions opts;
  opts.db_path = dir;

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(opts, &db).ok());

  int64_t val = -1;
  ASSERT_TRUE(Counter::Incr(db.get(), "hits", &val).ok());
  EXPECT_EQ(val, 1);

  ASSERT_TRUE(Counter::Get(db.get(), "hits", &val).ok());
  EXPECT_EQ(val, 1);
}

TEST(CounterTest, IncrByPositive) {
  const auto dir = TestDir("incr_by");
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);

  DBOptions opts;
  opts.db_path = dir;

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(opts, &db).ok());

  int64_t val = -1;
  ASSERT_TRUE(Counter::IncrBy(db.get(), "score", 10, &val).ok());
  EXPECT_EQ(val, 10);

  ASSERT_TRUE(Counter::IncrBy(db.get(), "score", 5, &val).ok());
  EXPECT_EQ(val, 15);
}

TEST(CounterTest, DecrDecreases) {
  const auto dir = TestDir("decr");
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);

  DBOptions opts;
  opts.db_path = dir;

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(opts, &db).ok());

  int64_t val = -1;
  ASSERT_TRUE(Counter::IncrBy(db.get(), "c", 100, &val).ok());
  EXPECT_EQ(val, 100);

  ASSERT_TRUE(Counter::Decr(db.get(), "c", &val).ok());
  EXPECT_EQ(val, 99);

  ASSERT_TRUE(Counter::DecrBy(db.get(), "c", 50, &val).ok());
  EXPECT_EQ(val, 49);
}

TEST(CounterTest, GetNonExistentReturnsNotFound) {
  const auto dir = TestDir("not_found");
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);

  DBOptions opts;
  opts.db_path = dir;

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(opts, &db).ok());

  int64_t val = 0;
  Status s = Counter::Get(db.get(), "nonexist", &val);
  EXPECT_TRUE(s.IsNotFound());
}

TEST(CounterTest, MultipleKeysIndependent) {
  const auto dir = TestDir("multi_key");
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);

  DBOptions opts;
  opts.db_path = dir;

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(opts, &db).ok());

  int64_t a = -1, b = -1;
  ASSERT_TRUE(Counter::IncrBy(db.get(), "a", 10, &a).ok());
  ASSERT_TRUE(Counter::IncrBy(db.get(), "b", 20, &b).ok());
  EXPECT_EQ(a, 10);
  EXPECT_EQ(b, 20);

  ASSERT_TRUE(Counter::Incr(db.get(), "a", &a).ok());
  EXPECT_EQ(a, 11);
  ASSERT_TRUE(Counter::Get(db.get(), "b", &b).ok());
  EXPECT_EQ(b, 20);
}

}  // namespace
}  // namespace kv
