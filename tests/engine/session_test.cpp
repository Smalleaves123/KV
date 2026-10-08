#include "kv/engine/db.h"

#include <chrono>
#include <atomic>
#include <filesystem>
#include <functional>
#include <limits>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "kv/engine/db_impl.h"
#include "kv/engine/write_applier.h"
#include "kv/testing/failure_injection.h"
#include "kv/version/manifest.h"

namespace kv {
namespace {

class SessionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ = std::string("test_tmp/sessions/") +
            ::testing::UnitTest::GetInstance()->current_test_info()->name();
    std::filesystem::remove_all(root_);
    options_.db_path = root_ + "/db";
    options_.memtable_write_buffer_size = 0;
    options_.auto_compaction_enabled = false;
    options_.expiry_cleanup_interval_ms = 0;
    ASSERT_TRUE(DB::Open(options_, &db_).ok());
  }

  void TearDown() override {
    if (db_) (void)db_->Close();
    testing::ClearFailureInjection();
    db_.reset();
    std::filesystem::remove_all(root_);
  }

  void Reopen(size_t cleanup_interval_ms = 0) {
    ASSERT_TRUE(db_->Close().ok());
    db_.reset();
    options_.expiry_cleanup_interval_ms = cleanup_interval_ms;
    ASSERT_TRUE(DB::Open(options_, &db_).ok());
  }

  bool WaitUntil(const std::function<bool()>& predicate) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    do {
      if (predicate()) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    } while (std::chrono::steady_clock::now() < deadline);
    return predicate();
  }

  void ReadFiles(std::vector<ManifestFileMeta>* files) {
    Manifest manifest;
    ASSERT_TRUE(manifest.Open(options_.db_path + "/MANIFEST", false).ok());
    ASSERT_TRUE(manifest.Recover(files).ok());
  }

  std::string root_;
  DBOptions options_;
  std::unique_ptr<DB> db_;
};

TEST_F(SessionTest, PutAtomicallyStoresTTLAndInvalidatesPersistentCache) {
  ASSERT_TRUE(db_->Close().ok());
  options_.cache_enabled = true;
  options_.memtable_write_buffer_size = 1;
  ASSERT_TRUE(DB::Open(options_, &db_).ok());
  ASSERT_TRUE(db_->Put(WriteOptions{}, "session", "cached").ok());
  EXPECT_TRUE(db_->Compact().IsNotFound());  // Wait for the first flush.
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions{}, "session", &value).ok());
  WriteOptions write;
  write.ttl_ms = 10000;
  ASSERT_TRUE(db_->Put(write, "session", "new").ok());
  ASSERT_TRUE(db_->Get(ReadOptions{}, "session", &value).ok());
  EXPECT_EQ(value, "new");
  int64_t ttl = -2;
  ASSERT_TRUE(db_->PTTL(ReadOptions{}, "session", &ttl).ok());
  EXPECT_GT(ttl, 0);
  EXPECT_LE(ttl, 10000);
  ASSERT_NO_FATAL_FAILURE(Reopen());
  ASSERT_TRUE(db_->Get(ReadOptions{}, "session", &value).ok());
  EXPECT_EQ(value, "new");
  ASSERT_TRUE(db_->PTTL(ReadOptions{}, "session", &ttl).ok());
  EXPECT_GT(ttl, 0);
  ASSERT_TRUE(db_->Put(WriteOptions{}, "session", "persistent").ok());
  ASSERT_TRUE(db_->PTTL(ReadOptions{}, "session", &ttl).ok());
  EXPECT_EQ(ttl, -1);
}

TEST_F(SessionTest, InvalidTTLDoesNotChangeValueOrSequence) {
  ASSERT_TRUE(db_->Put(WriteOptions{}, "session", "original").ok());
  auto* impl = dynamic_cast<DBImpl*>(db_.get());
  ASSERT_NE(impl, nullptr);
  const uint64_t sequence = impl->LatestSequence();
  WriteOptions invalid;
  invalid.ttl_ms = -1;
  EXPECT_TRUE(db_->Put(invalid, "session", "invalid").IsInvalidArgument());
  EXPECT_TRUE(db_->Expire(WriteOptions{}, "session",
                          std::numeric_limits<int64_t>::max()).IsInvalidArgument());
  EXPECT_EQ(impl->LatestSequence(), sequence);
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions{}, "session", &value).ok());
  EXPECT_EQ(value, "original");
}

TEST_F(SessionTest, TTLIsRecoveredFromWALWhenFlushFailsBeforeManifest) {
  WriteOptions write;
  write.sync = true;
  write.ttl_ms = 10000;
  ASSERT_TRUE(db_->Put(write, "session", "payload").ok());
  testing::InjectFailure(testing::FailurePoint::kAfterSSTableWriteBeforeManifest,
                         Status::IOError("session flush failure"));
  EXPECT_TRUE(db_->Close().IsIOError());
  testing::ClearFailureInjection();
  db_.reset();
  ASSERT_TRUE(DB::Open(options_, &db_).ok());
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions{}, "session", &value).ok());
  EXPECT_EQ(value, "payload");
  int64_t ttl = -2;
  ASSERT_TRUE(db_->PTTL(ReadOptions{}, "session", &ttl).ok());
  EXPECT_GT(ttl, 0);
  EXPECT_LE(ttl, 10000);
}

TEST_F(SessionTest, RenewalPreservesPayloadAndPTTLSentinels) {
  WriteOptions write;
  write.ttl_ms = 10000;
  ASSERT_TRUE(db_->Put(write, "session", "payload").ok());
  ASSERT_TRUE(db_->ExpireMillis(WriteOptions{}, "session", 20000).ok());
  int64_t ttl = -2;
  ASSERT_TRUE(db_->PTTL(ReadOptions{}, "session", &ttl).ok());
  EXPECT_GT(ttl, 10000);
  EXPECT_LE(ttl, 20000);
  ASSERT_NO_FATAL_FAILURE(Reopen());
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions{}, "session", &value).ok());
  EXPECT_EQ(value, "payload");
  ASSERT_TRUE(db_->PTTL(ReadOptions{}, "session", &ttl).ok());
  EXPECT_GT(ttl, 10000);
  ASSERT_TRUE(db_->Persist(WriteOptions{}, "session").ok());
  ASSERT_TRUE(db_->PTTL(ReadOptions{}, "session", &ttl).ok());
  EXPECT_EQ(ttl, -1);
  ASSERT_TRUE(db_->ExpireMillis(WriteOptions{}, "session", 0).ok());
  ASSERT_TRUE(db_->PTTL(ReadOptions{}, "session", &ttl).ok());
  EXPECT_EQ(ttl, -2);
  EXPECT_TRUE(db_->ExpireMillis(WriteOptions{}, "session", 10000).IsNotFound());
  EXPECT_TRUE(db_->PTTL(ReadOptions{}, "session", nullptr).IsInvalidArgument());
}

TEST_F(SessionTest, ExpiredSessionCannotBeRenewed) {
  WriteOptions write;
  write.ttl_ms = 20;
  ASSERT_TRUE(db_->Put(write, "session", "payload").ok());
  ASSERT_TRUE(WaitUntil([&] {
    std::string value;
    return db_->Get(ReadOptions{}, "session", &value).IsNotFound();
  }));
  EXPECT_TRUE(db_->ExpireMillis(WriteOptions{}, "session", 1000).IsNotFound());
  int64_t ttl = 0;
  ASSERT_TRUE(db_->PTTL(ReadOptions{}, "session", &ttl).ok());
  EXPECT_EQ(ttl, -2);
}

TEST_F(SessionTest, CleanupRebuildsDeadlinesAndReclaimsAnIdleDatabase) {
  WriteOptions write;
  write.ttl_ms = 20;
  ASSERT_TRUE(db_->Put(write, "session", "payload").ok());
  ASSERT_NO_FATAL_FAILURE(Reopen(5));
  ASSERT_TRUE(WaitUntil([&] {
    CompactionStats stats;
    return db_->GetCompactionStats(&stats).ok() && stats.succeeded > 0;
  }));
  ASSERT_TRUE(db_->Close().ok());
  std::vector<ManifestFileMeta> files;
  ASSERT_NO_FATAL_FAILURE(ReadFiles(&files));
  EXPECT_TRUE(files.empty());
  EXPECT_TRUE(std::filesystem::is_empty(options_.db_path + "/sst"));
  ASSERT_TRUE(DB::Open(options_, &db_).ok());
  std::string value;
  EXPECT_TRUE(db_->Get(ReadOptions{}, "session", &value).IsNotFound());
}

TEST_F(SessionTest, CleanupProtectsRenewedPersistentAndUnrelatedKeys) {
  WriteOptions write;
  write.ttl_ms = 10000;
  ASSERT_TRUE(db_->Put(write, "renewed", "keep").ok());
  ASSERT_TRUE(db_->ExpireMillis(WriteOptions{}, "renewed", 20000).ok());
  ASSERT_TRUE(db_->Put(write, "persisted", "keep").ok());
  ASSERT_TRUE(db_->Persist(WriteOptions{}, "persisted").ok());
  ASSERT_TRUE(db_->Put(WriteOptions{}, "unrelated", "keep").ok());
  auto* applier = dynamic_cast<WriteApplier*>(db_.get());
  ASSERT_NE(applier, nullptr);
  ASSERT_TRUE(applier->ApplyPutWithExpiry("expired", "discard", 1).ok());
  ASSERT_NO_FATAL_FAILURE(Reopen(5));
  ASSERT_TRUE(WaitUntil([&] {
    CompactionStats stats;
    return db_->GetCompactionStats(&stats).ok() && stats.succeeded > 0;
  }));
  std::string value;
  for (const auto& key : {"renewed", "persisted", "unrelated"}) {
    ASSERT_TRUE(db_->Get(ReadOptions{}, key, &value).ok());
    EXPECT_EQ(value, "keep");
  }
  int64_t ttl = -2;
  ASSERT_TRUE(db_->PTTL(ReadOptions{}, "renewed", &ttl).ok());
  EXPECT_GT(ttl, 10000);
  ASSERT_TRUE(db_->PTTL(ReadOptions{}, "persisted", &ttl).ok());
  EXPECT_EQ(ttl, -1);
  EXPECT_TRUE(db_->Get(ReadOptions{}, "expired", &value).IsNotFound());
}

TEST_F(SessionTest, CleanupPreservesAWriteDuringCompactionAndItsNewDeadline) {
  auto* applier = dynamic_cast<WriteApplier*>(db_.get());
  ASSERT_NE(applier, nullptr);
  ASSERT_TRUE(applier->ApplyPutWithExpiry("session", "old", 1).ok());
  // Install the hook before starting the cleanup worker on recovery.
  ASSERT_TRUE(db_->Close().ok());
  db_.reset();
  options_.expiry_cleanup_interval_ms = 5;
  std::atomic<DB*> published{nullptr};
  testing::SetFailurePointHook(testing::FailurePoint::kDuringCompactionOutput, [&] {
    DB* db = nullptr;
    while ((db = published.load()) == nullptr) std::this_thread::yield();
    WriteOptions write;
    write.ttl_ms = 250;
    ASSERT_TRUE(db->Put(write, "session", "new").ok());
  });
  ASSERT_TRUE(DB::Open(options_, &db_).ok());
  published.store(db_.get());
  ASSERT_TRUE(WaitUntil([&] {
    CompactionStats stats;
    return db_->GetCompactionStats(&stats).ok() && stats.succeeded > 0;
  }));
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions{}, "session", &value).ok());
  EXPECT_EQ(value, "new");
  // The old cleanup pass must not remove the concurrently written deadline.
  ASSERT_TRUE(WaitUntil([&] {
    CompactionStats stats;
    return db_->GetCompactionStats(&stats).ok() && stats.succeeded >= 2;
  }));
  EXPECT_TRUE(db_->Get(ReadOptions{}, "session", &value).IsNotFound());
}

TEST_F(SessionTest, SnapshotDefersPhysicalCleanup) {
  ASSERT_NO_FATAL_FAILURE(Reopen(5));
  ASSERT_TRUE(db_->Put(WriteOptions{}, "session", "old").ok());
  const Snapshot* snapshot = db_->GetSnapshot();
  ASSERT_NE(snapshot, nullptr);
  auto* applier = dynamic_cast<WriteApplier*>(db_.get());
  ASSERT_NE(applier, nullptr);
  ASSERT_TRUE(applier->ApplyPutWithExpiry("session", "expired", 1).ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  CompactionStats stats;
  ASSERT_TRUE(db_->GetCompactionStats(&stats).ok());
  EXPECT_EQ(stats.succeeded, 0U);
  ReadOptions read;
  read.snapshot = snapshot;
  std::string value;
  ASSERT_TRUE(db_->Get(read, "session", &value).ok());
  EXPECT_EQ(value, "old");
  ASSERT_TRUE(db_->ReleaseSnapshot(snapshot).ok());
  ASSERT_TRUE(WaitUntil([&] {
    return db_->GetCompactionStats(&stats).ok() && stats.succeeded > 0;
  }));
}

TEST_F(SessionTest, DisabledCleanupKeepsExpiredSSTUntilExplicitCompaction) {
  auto* applier = dynamic_cast<WriteApplier*>(db_.get());
  ASSERT_NE(applier, nullptr);
  ASSERT_TRUE(applier->ApplyPutWithExpiry("session", "payload", 1).ok());
  ASSERT_NO_FATAL_FAILURE(Reopen());
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  CompactionStats stats;
  ASSERT_TRUE(db_->GetCompactionStats(&stats).ok());
  EXPECT_EQ(stats.trigger_attempts, 0U);
  std::string value;
  EXPECT_TRUE(db_->Get(ReadOptions{}, "session", &value).IsNotFound());
  ASSERT_TRUE(db_->Close().ok());
  std::vector<ManifestFileMeta> files;
  ASSERT_NO_FATAL_FAILURE(ReadFiles(&files));
  EXPECT_FALSE(files.empty());
}

TEST_F(SessionTest, TransactionDefersPhysicalCleanup) {
  ASSERT_NO_FATAL_FAILURE(Reopen(5));
  ASSERT_TRUE(db_->Put(WriteOptions{}, "session", "old").ok());
  std::unique_ptr<Transaction> transaction;
  ASSERT_TRUE(db_->BeginTransaction(TxnOptions{}, &transaction).ok());
  auto* applier = dynamic_cast<WriteApplier*>(db_.get());
  ASSERT_NE(applier, nullptr);
  ASSERT_TRUE(applier->ApplyPutWithExpiry("session", "expired", 1).ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  CompactionStats stats;
  ASSERT_TRUE(db_->GetCompactionStats(&stats).ok());
  EXPECT_EQ(stats.succeeded, 0U);
  std::string value;
  ASSERT_TRUE(transaction->Get("session", &value).ok());
  EXPECT_EQ(value, "old");
  ASSERT_TRUE(transaction->Rollback().ok());
  ASSERT_TRUE(WaitUntil([&] {
    return db_->GetCompactionStats(&stats).ok() && stats.succeeded > 0;
  }));
}

TEST_F(SessionTest, CleanupRetriesAfterFailedCommitWithoutLosingData) {
  ASSERT_TRUE(db_->Put(WriteOptions{}, "persistent", "keep").ok());
  auto* applier = dynamic_cast<WriteApplier*>(db_.get());
  ASSERT_NE(applier, nullptr);
  ASSERT_TRUE(applier->ApplyPutWithExpiry("session", "discard", 1).ok());
  ASSERT_TRUE(db_->Close().ok());
  db_.reset();
  options_.expiry_cleanup_interval_ms = 5;
  testing::InjectFailure(testing::FailurePoint::kAfterCompactionAddBeforeRemove,
                         Status::IOError("expiry commit failure"));
  ASSERT_TRUE(DB::Open(options_, &db_).ok());
  ASSERT_TRUE(WaitUntil([&] {
    CompactionStats stats;
    return db_->GetCompactionStats(&stats).ok() && stats.failed == 1 &&
           stats.succeeded > 0;
  }));
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions{}, "persistent", &value).ok());
  EXPECT_EQ(value, "keep");
  EXPECT_TRUE(db_->Get(ReadOptions{}, "session", &value).IsNotFound());
  ASSERT_TRUE(db_->Close().ok());
  std::vector<ManifestFileMeta> files;
  ASSERT_NO_FATAL_FAILURE(ReadFiles(&files));
  ASSERT_EQ(files.size(), 1U);
  EXPECT_EQ(files[0].smallest_key, "persistent");
  EXPECT_EQ(files[0].largest_key, "persistent");
}

TEST_F(SessionTest, CleanupReclaimsAnExpiredFileAlreadyAtL2) {
  ASSERT_TRUE(db_->Close().ok());
  options_.compaction_min_input_files = 1;
  ASSERT_TRUE(DB::Open(options_, &db_).ok());
  WriteOptions write;
  write.ttl_ms = 300;
  ASSERT_TRUE(db_->Put(write, "session", "payload").ok());
  ASSERT_TRUE(db_->Compact().ok());
  ASSERT_TRUE(db_->Compact().ok());
  std::vector<ManifestFileMeta> files;
  ASSERT_NO_FATAL_FAILURE(ReadFiles(&files));
  ASSERT_EQ(files.size(), 1U);
  EXPECT_EQ(files[0].level, 2U);
  ASSERT_NO_FATAL_FAILURE(Reopen(5));
  ASSERT_TRUE(WaitUntil([&] {
    CompactionStats stats;
    return db_->GetCompactionStats(&stats).ok() && stats.succeeded > 0;
  }));
  ASSERT_TRUE(db_->Close().ok());
  ASSERT_NO_FATAL_FAILURE(ReadFiles(&files));
  EXPECT_TRUE(files.empty());
}

TEST_F(SessionTest, CommittedExpiryForAMissingValueDoesNotBlockApplication) {
  auto* applier = dynamic_cast<WriteApplier*>(db_.get());
  ASSERT_NE(applier, nullptr);
  ASSERT_TRUE(applier->ApplyPutWithExpiry("session", "expired", 1).ok());
  EXPECT_TRUE(applier->ApplyExpireAt("session", 100000).ok());
  EXPECT_TRUE(applier->ApplyExpireAt("missing", 0).ok());
  ASSERT_TRUE(applier->ApplyPut("next", "value").ok());
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions{}, "next", &value).ok());
  EXPECT_EQ(value, "value");
  EXPECT_TRUE(db_->Get(ReadOptions{}, "session", &value).IsNotFound());
}

}  // namespace
}  // namespace kv
