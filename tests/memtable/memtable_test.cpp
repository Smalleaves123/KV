#include "kv/memtable/memtable.h"

#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace kv {
namespace {

TEST(MemTableTest, EmptyMemTable) {
  MemTable mem;

  EXPECT_TRUE(mem.Empty());
  EXPECT_EQ(mem.Size(), 0U);
  EXPECT_EQ(mem.LatestSequence(), 0U);
  EXPECT_EQ(mem.ApproximateMemoryUsage(), 0U);

  std::string value;
  Status s = mem.Get("missing", &value);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNotFound());
}

TEST(MemTableTest, PutAndGetLatestValue) {
  MemTable mem;

  ASSERT_TRUE(mem.Put("name", "td").ok());
  ASSERT_TRUE(mem.Put("name", "tdmpc2").ok());

  std::string value;
  Status s = mem.Get("name", &value);

  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "tdmpc2");

  EXPECT_FALSE(mem.Empty());
  EXPECT_EQ(mem.Size(), 2U);
  EXPECT_EQ(mem.LatestSequence(), 2U);
  EXPECT_GT(mem.ApproximateMemoryUsage(), 0U);
}

TEST(MemTableTest, DeleteMakesKeyInvisible) {
  MemTable mem;

  ASSERT_TRUE(mem.Put("model", "v1").ok());
  ASSERT_TRUE(mem.Delete("model").ok());

  std::string value;
  Status s = mem.Get("model", &value);

  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNotFound());
  EXPECT_EQ(mem.LatestSequence(), 2U);
}

TEST(MemTableTest, PutWithExplicitSequence) {
  MemTable mem;

  ASSERT_TRUE(mem.Put(100, "k1", "v100").ok());
  ASSERT_TRUE(mem.Put(101, "k1", "v101").ok());
  ASSERT_TRUE(mem.Put(80, "k2", "v80").ok());

  std::string value;
  Status s = mem.Get("k1", &value);

  ASSERT_TRUE(s.ok());
  EXPECT_EQ(value, "v101");
  EXPECT_EQ(mem.LatestSequence(), 101U);
}

TEST(MemTableTest, DeleteWithExplicitSequence) {
  MemTable mem;

  ASSERT_TRUE(mem.Put(10, "key", "value").ok());
  ASSERT_TRUE(mem.Delete(11, "key").ok());

  std::string value;
  Status s = mem.Get("key", &value);

  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNotFound());
  EXPECT_EQ(mem.LatestSequence(), 11U);
}

TEST(MemTableTest, InvalidSequenceIsRejected) {
  MemTable mem;

  Status s1 = mem.Put(0, "a", "b");
  Status s2 = mem.Delete(0, "a");

  EXPECT_FALSE(s1.ok());
  EXPECT_TRUE(s1.IsInvalidArgument());

  EXPECT_FALSE(s2.ok());
  EXPECT_TRUE(s2.IsInvalidArgument());
}

TEST(MemTableTest, NullValuePointerIsRejected) {
  MemTable mem;
  ASSERT_TRUE(mem.Put("a", "1").ok());

  Status s = mem.Get("a", nullptr);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST(MemTableTest, IteratorTraversesAllEntriesInOrder) {
  MemTable mem;

  ASSERT_TRUE(mem.Put(1, "b", "vb1").ok());
  ASSERT_TRUE(mem.Put(2, "a", "va2").ok());
  ASSERT_TRUE(mem.Put(3, "a", "va3").ok());
  ASSERT_TRUE(mem.Delete(4, "c").ok());

  auto it = mem.NewIterator();

  std::vector<std::string> keys;
  std::vector<uint64_t> seqs;
  std::vector<int> types;

  for (; it.Valid(); it.Next()) {
    const auto& e = it.entry();
    keys.push_back(e.key);
    seqs.push_back(e.seq);
    types.push_back(static_cast<int>(e.type));
  }

  ASSERT_EQ(keys.size(), 4U);

  EXPECT_EQ(keys[0], "a");
  EXPECT_EQ(seqs[0], 3U);

  EXPECT_EQ(keys[1], "a");
  EXPECT_EQ(seqs[1], 2U);

  EXPECT_EQ(keys[2], "b");
  EXPECT_EQ(seqs[2], 1U);

  EXPECT_EQ(keys[3], "c");
  EXPECT_EQ(seqs[3], 4U);
}

TEST(MemTableTest, DebugStringContainsEntries) {
  MemTable mem;

  ASSERT_TRUE(mem.Put(1, "x", "1").ok());
  ASSERT_TRUE(mem.Delete(2, "x").ok());
  ASSERT_TRUE(mem.Put(3, "y", "2").ok());

  std::string text = mem.DebugString();

  EXPECT_NE(text.find("key=x"), std::string::npos);
  EXPECT_NE(text.find("key=y"), std::string::npos);
  EXPECT_NE(text.find("seq=1"), std::string::npos);
  EXPECT_NE(text.find("seq=2"), std::string::npos);
  EXPECT_NE(text.find("seq=3"), std::string::npos);
}

}  // namespace
}  // namespace kv