#include "kv/cache/shard_lru_cache.h"

#include <chrono>
#include <thread>

#include <gtest/gtest.h>

namespace kv {
namespace {

TEST(ShardLRUCacheTest, BasicPutGetAndStats) {
  ShardLRUCache cache(64, 0, 4);

  cache.Put("a", "1");
  cache.Put("b", "2");
  cache.Put("c", "3");

  std::string value;
  EXPECT_TRUE(cache.Get("a", &value));
  EXPECT_EQ(value, "1");
  EXPECT_TRUE(cache.Get("b", &value));
  EXPECT_EQ(value, "2");
  EXPECT_FALSE(cache.Get("missing", &value));

  const CacheStats st = cache.Stats();
  EXPECT_GE(st.hit, 2u);
  EXPECT_GE(st.miss, 1u);
}

TEST(ShardLRUCacheTest, SupportsTTLExpiry) {
  ShardLRUCache cache(16, 20, 2);
  cache.Put("ttl", "v");

  std::string value;
  EXPECT_TRUE(cache.Get("ttl", &value));
  EXPECT_EQ(value, "v");

  std::this_thread::sleep_for(std::chrono::milliseconds(35));
  EXPECT_FALSE(cache.Get("ttl", &value));
  EXPECT_FALSE(cache.Contains("ttl"));

  const CacheStats st = cache.Stats();
  EXPECT_GE(st.expire, 1u);
}

TEST(ShardLRUCacheTest, EvictsWhenCapacityExceeded) {
  // Use one shard to verify LRU-like eviction behavior deterministically.
  ShardLRUCache cache(2, 0, 1);

  cache.Put("a", "1");
  cache.Put("b", "2");

  std::string value;
  EXPECT_TRUE(cache.Get("a", &value));

  cache.Put("c", "3");

  EXPECT_FALSE(cache.Get("b", &value));
  EXPECT_TRUE(cache.Get("a", &value));
  EXPECT_TRUE(cache.Get("c", &value));

  const CacheStats st = cache.Stats();
  EXPECT_GE(st.evict, 1u);
}

}  // namespace
}  // namespace kv
