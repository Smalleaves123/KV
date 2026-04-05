#include "kv/cache/lfu_cache.h"

#include <chrono>
#include <thread>

#include <gtest/gtest.h>

namespace kv {
namespace {

TEST(LFUCacheTest, EvictsLeastFrequent) {
  LFUCache cache(2, 0);

  cache.Put("a", "1");
  cache.Put("b", "2");

  std::string value;
  EXPECT_TRUE(cache.Get("a", &value));
  EXPECT_TRUE(cache.Get("a", &value));

  cache.Put("c", "3");

  EXPECT_FALSE(cache.Get("b", &value));
  EXPECT_TRUE(cache.Get("a", &value));
  EXPECT_EQ(value, "1");
  EXPECT_TRUE(cache.Get("c", &value));
  EXPECT_EQ(value, "3");

  CacheStats stats = cache.Stats();
  EXPECT_GE(stats.evict, 1u);
}

TEST(LFUCacheTest, ExpireWithDefaultTTL) {
  LFUCache cache(4, 20);

  cache.Put("ttl", "v");
  std::string value;
  EXPECT_TRUE(cache.Get("ttl", &value));
  EXPECT_EQ(value, "v");

  std::this_thread::sleep_for(std::chrono::milliseconds(35));
  EXPECT_FALSE(cache.Get("ttl", &value));

  CacheStats stats = cache.Stats();
  EXPECT_GE(stats.expire, 1u);
  EXPECT_GE(stats.miss, 1u);
}

}  // namespace
}  // namespace kv
