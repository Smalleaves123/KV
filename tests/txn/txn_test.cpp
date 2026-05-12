#include "kv/txn/txn_manager.h"
#include "kv/txn/txn.h"

#include <gtest/gtest.h>

namespace kv::txn {
namespace {

TEST(TxnManagerTest, CommitMakesWritesVisible) {
	TxnManager mgr;
	auto txn = mgr.Begin();
	ASSERT_NE(txn, nullptr);

	ASSERT_TRUE(txn->Put("name", "alice").ok());
	ASSERT_TRUE(txn->Commit().ok());

	std::string value;
	ASSERT_TRUE(mgr.GetCommitted("name", &value).ok());
	EXPECT_EQ(value, "alice");
}

TEST(TxnManagerTest, RollbackDropsWrites) {
	TxnManager mgr;
	auto txn = mgr.Begin();
	ASSERT_NE(txn, nullptr);

	ASSERT_TRUE(txn->Put("k", "v").ok());
	ASSERT_TRUE(txn->Rollback().ok());

	std::string value;
	Status s = mgr.GetCommitted("k", &value);
	EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST(TxnManagerTest, WriteConflictDetected) {
	TxnManager mgr;
	auto t1 = mgr.Begin();
	auto t2 = mgr.Begin();
	ASSERT_NE(t1, nullptr);
	ASSERT_NE(t2, nullptr);

	ASSERT_TRUE(t1->Put("hot", "v1").ok());
	Status s = t2->Put("hot", "v2");
	EXPECT_TRUE(s.IsAlreadyExists()) << s.ToString();
}

}  // namespace
}  // namespace kv::txn
