// Block and BlockIterator regression coverage.
#include "kv/sstable/block.h"
#include "kv/sstable/block_builder.h"
#include "kv/sstable/block_iterator.h"

#include "gtest/gtest.h"
#include <string>
#include <vector>

namespace kv {
namespace {

TEST(BlockTest, EncodesAndIteratesEntriesInOrder) {
  BlockBuilder builder(2);
  builder.Add("alpha", 3, 0, "one");
  builder.Add("bravo", 2, 0, "two");
  builder.Add("charlie", 1, 1, "");

  const std::string data = builder.Finish();
  ASSERT_FALSE(data.empty());

  Block block(data.data(), data.size());
  BlockIterator it(block);
  it.SeekToFirst();

  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(it.key(), "alpha");
  EXPECT_EQ(it.seq(), 3U);
  EXPECT_EQ(it.value(), "one");

  it.Next();
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(it.key(), "bravo");
  EXPECT_EQ(it.value(), "two");

  it.Next();
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(it.key(), "charlie");
  EXPECT_EQ(it.type(), 1);
  EXPECT_TRUE(it.value().empty());

  it.Next();
  EXPECT_FALSE(it.Valid());
}

} // namespace
} // namespace kv
