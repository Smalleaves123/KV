#include "kv/cache/ttl_manager.h"

#include <chrono>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace kv {
namespace {

TEST(TTLManagerTest, SetTTLAndExpire) {
  TTLManager mgr;
  mgr.SetTTL("k1", 20);

  EXPECT_TRUE(mgr.HasTTL("k1"));
  EXPECT_FALSE(mgr.IsExpired("k1"));

  std::this_thread::sleep_for(std::chrono::milliseconds(35));
  EXPECT_TRUE(mgr.IsExpired("k1"));
}

TEST(TTLManagerTest, DefaultTTLWorks) {
  TTLManager mgr(20);
  mgr.SetTTL("k1");  // 使用默认 TTL

  EXPECT_TRUE(mgr.HasTTL("k1"));
  std::this_thread::sleep_for(std::chrono::milliseconds(35));
  EXPECT_TRUE(mgr.IsExpired("k1"));
}

TEST(TTLManagerTest, NonPositiveTTLMeansNoExpiration) {
  TTLManager mgr(20);

  mgr.SetTTL("k1", 0);
  EXPECT_FALSE(mgr.HasTTL("k1"));
  EXPECT_FALSE(mgr.IsExpired("k1"));

  mgr.SetTTL("k2", -1);  // 使用默认 TTL=20
  EXPECT_TRUE(mgr.HasTTL("k2"));

  mgr.SetDefaultTTL(0);
  mgr.SetTTL("k3", -1);  // default=0 -> 不过期
  EXPECT_FALSE(mgr.HasTTL("k3"));
  EXPECT_FALSE(mgr.IsExpired("k3"));
}

TEST(TTLManagerTest, PurgeExpiredRemovesOnlyExpired) {
  TTLManager mgr;
  mgr.SetTTL("k1", 20);
  mgr.SetTTL("k2", 60);

  std::this_thread::sleep_for(std::chrono::milliseconds(35));

  std::vector<std::string> expired;
  size_t purged = mgr.PurgeExpired(&expired);

  EXPECT_EQ(purged, 1U);
  EXPECT_EQ(expired.size(), 1U);
  EXPECT_EQ(expired[0], "k1");

  EXPECT_FALSE(mgr.HasTTL("k1"));
  EXPECT_TRUE(mgr.HasTTL("k2"));
}

}  // namespace
}  // namespace kv
