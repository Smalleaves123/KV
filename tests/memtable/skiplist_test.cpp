#include "kv/memtable/skiplist.h"

#include <vector>

#include "gtest/gtest.h"

namespace kv {
namespace {

TEST(SkipListTest, EmptyList) {
  SkipList<int> list;

  EXPECT_TRUE(list.Empty());
  EXPECT_EQ(list.Size(), 0U);

  auto it = list.Begin();
  EXPECT_FALSE(it.Valid());

  auto pos = list.FindGreaterOrEqual(10);
  EXPECT_FALSE(pos.Valid());
}

TEST(SkipListTest, InsertAndTraverseInSortedOrder) {
  SkipList<int> list;

  list.Insert(5);
  list.Insert(1);
  list.Insert(9);
  list.Insert(3);
  list.Insert(7);

  EXPECT_FALSE(list.Empty());
  EXPECT_EQ(list.Size(), 5U);

  std::vector<int> values;
  for (auto it = list.Begin(); it.Valid(); it.Next()) {
    values.push_back(it.key());
  }

  std::vector<int> expected{1, 3, 5, 7, 9};
  EXPECT_EQ(values, expected);
}

TEST(SkipListTest, DuplicateKeysAreAllowed) {
  SkipList<int> list;

  list.Insert(10);
  list.Insert(5);
  list.Insert(10);
  list.Insert(10);

  std::vector<int> values;
  for (auto it = list.Begin(); it.Valid(); it.Next()) {
    values.push_back(it.key());
  }

  std::vector<int> expected{5, 10, 10, 10};
  EXPECT_EQ(values, expected);
  EXPECT_EQ(list.Size(), 4U);
}

TEST(SkipListTest, FindGreaterOrEqualWorksForExactMatch) {
  SkipList<int> list;
  list.Insert(2);
  list.Insert(4);
  list.Insert(6);
  list.Insert(8);

  auto it = list.FindGreaterOrEqual(6);
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(it.key(), 6);
}

TEST(SkipListTest, FindGreaterOrEqualWorksForMissingKey) {
  SkipList<int> list;
  list.Insert(2);
  list.Insert(4);
  list.Insert(6);
  list.Insert(8);

  {
    auto it = list.FindGreaterOrEqual(1);
    ASSERT_TRUE(it.Valid());
    EXPECT_EQ(it.key(), 2);
  }

  {
    auto it = list.FindGreaterOrEqual(5);
    ASSERT_TRUE(it.Valid());
    EXPECT_EQ(it.key(), 6);
  }

  {
    auto it = list.FindGreaterOrEqual(8);
    ASSERT_TRUE(it.Valid());
    EXPECT_EQ(it.key(), 8);
  }

  {
    auto it = list.FindGreaterOrEqual(100);
    EXPECT_FALSE(it.Valid());
  }
}

TEST(SkipListTest, IteratorSeekAndSeekToFirst) {
  SkipList<int> list;
  list.Insert(20);
  list.Insert(10);
  list.Insert(30);

  auto it = list.Begin();
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(it.key(), 10);

  it.Seek(20);
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(it.key(), 20);

  it.Seek(25);
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(it.key(), 30);

  it.Seek(100);
  EXPECT_FALSE(it.Valid());

  it.SeekToFirst();
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(it.key(), 10);
}

TEST(SkipListTest, ClearResetsList) {
  SkipList<int> list;
  list.Insert(1);
  list.Insert(2);
  list.Insert(3);

  EXPECT_EQ(list.Size(), 3U);
  EXPECT_FALSE(list.Empty());

  list.Clear();

  EXPECT_TRUE(list.Empty());
  EXPECT_EQ(list.Size(), 0U);

  auto it = list.Begin();
  EXPECT_FALSE(it.Valid());
}

}  // namespace
}  // namespace kv