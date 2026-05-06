#include "kv/txn/lock_manager.h"

#include <gtest/gtest.h>

namespace kv::txn {
namespace {

TEST(LockManagerTest, SharedAndExclusiveConflict) {
	LockManager lm;

	EXPECT_TRUE(lm.TryLockShared(1, "k"));
	EXPECT_TRUE(lm.TryLockShared(2, "k"));
	EXPECT_FALSE(lm.TryLockExclusive(3, "k"));

	lm.Unlock(1, "k");
	lm.Unlock(2, "k");
	EXPECT_TRUE(lm.TryLockExclusive(3, "k"));
}

TEST(LockManagerTest, UnlockAllReleasesOwnedKeys) {
	LockManager lm;

	EXPECT_TRUE(lm.TryLockExclusive(10, "a"));
	EXPECT_TRUE(lm.TryLockExclusive(10, "b"));
	EXPECT_EQ(lm.HeldBy(10), 2U);

	lm.UnlockAll(10);

	EXPECT_EQ(lm.HeldBy(10), 0U);
	EXPECT_FALSE(lm.IsLocked("a"));
	EXPECT_FALSE(lm.IsLocked("b"));
}

}  // namespace
}  // namespace kv::txn
