#include "kv/table/bloom_filter.h"

#include <string>

#include "gtest/gtest.h"

namespace kv {
namespace {

TEST(BloomFilterTest, EmptyFilterReturnsFalse) {
  BloomFilter filter(1 << 20);
  EXPECT_FALSE(filter.MayContain("alpha"));
  EXPECT_FALSE(filter.MayContain("beta"));
}

TEST(BloomFilterTest, AddedKeysAreReportedPresent) {
  BloomFilter filter(1 << 20);

  filter.Add("k1");
  filter.Add("k2");
  filter.Add("k3");

  EXPECT_TRUE(filter.MayContain("k1"));
  EXPECT_TRUE(filter.MayContain("k2"));
  EXPECT_TRUE(filter.MayContain("k3"));
}

TEST(BloomFilterTest, RepeatedAddIsStable) {
  BloomFilter filter(1 << 16);

  for (int i = 0; i < 100; ++i) {
    filter.Add("dup_key");
  }

  EXPECT_TRUE(filter.MayContain("dup_key"));
}

TEST(BloomFilterTest, ZeroBitsCtorStillWorks) {
  BloomFilter filter(0);
  filter.Add("only");
  EXPECT_TRUE(filter.MayContain("only"));
}

}  // namespace
}  // namespace kv

