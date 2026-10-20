#include "kv/engine/db.h"
#include "kv/engine/db_impl.h"
#include "kv/engine/write_applier.h"
#include "kv/testing/failure_injection.h"

#include "gtest/gtest.h"
#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

namespace kv {
namespace {

class IdempotencyTest : public ::testing::Test {
protected:
  void SetUp() override {
    options_.db_path = std::string("test_tmp/idempotency/") +
                       ::testing::UnitTest::GetInstance()->current_test_info()->name();
    options_.memtable_write_buffer_size = 0;
    options_.auto_compaction_enabled = false;
    options_.expiry_cleanup_interval_ms = 0;
    std::filesystem::remove_all(options_.db_path);
    ASSERT_TRUE(DB::Open(options_, &db_).ok());
  }

  void TearDown() override {
    testing::ClearFailureInjection();
    if (db_)
      (void)db_->Close();
    db_.reset();
    std::filesystem::remove_all(options_.db_path);
  }

  void Reopen() {
    ASSERT_TRUE(db_->Close().ok());
    db_.reset();
    ASSERT_TRUE(DB::Open(options_, &db_).ok());
  }

  DBOptions options_;
  std::unique_ptr<DB> db_;
};

TEST_F(IdempotencyTest, ConcurrentClaimsHaveOneWinnerAndKeepItsPayload) {
  constexpr size_t kClients = 16;
  std::array<Status, kClients> statuses;
  std::array<bool, kClients> inserted{};
  std::array<std::thread, kClients> threads;
  std::atomic<bool> start{false};
  for (size_t i = 0; i < kClients; ++i) {
    threads[i] = std::thread([&, i] {
      while (!start.load())
        std::this_thread::yield();
      WriteOptions write;
      write.ttl_ms = 60000;
      statuses[i] =
          db_->PutIfAbsent(write, "idem:request", std::to_string(i), &inserted[i]);
    });
  }
  start.store(true);
  for (auto& thread : threads)
    thread.join();
  size_t winners = 0;
  std::string winner;
  for (size_t i = 0; i < kClients; ++i) {
    ASSERT_TRUE(statuses[i].ok()) << statuses[i].ToString();
    if (inserted[i]) {
      ++winners;
      winner = std::to_string(i);
    }
  }
  EXPECT_EQ(winners, 1U);
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions{}, "idem:request", &value).ok());
  EXPECT_EQ(value, winner);
}

TEST_F(IdempotencyTest, DuplicatePreservesPayloadDeadlineAndSequenceAfterRestart) {
  WriteOptions write;
  write.sync = true;
  write.ttl_ms = 60000;
  bool inserted = false;
  ASSERT_TRUE(db_->PutIfAbsent(write, "idem:request", "accepted", &inserted).ok());
  ASSERT_TRUE(inserted);
  ASSERT_NO_FATAL_FAILURE(Reopen());
  auto* impl = dynamic_cast<DBImpl*>(db_.get());
  ASSERT_NE(impl, nullptr);
  const auto sequence = impl->LatestSequence();
  int64_t before = 0;
  ASSERT_TRUE(db_->PTTL(ReadOptions{}, "idem:request", &before).ok());
  write.ttl_ms = 120000;
  ASSERT_TRUE(db_->PutIfAbsent(write, "idem:request", "duplicate", &inserted).ok());
  EXPECT_FALSE(inserted);
  EXPECT_EQ(impl->LatestSequence(), sequence);
  int64_t after = 0;
  ASSERT_TRUE(db_->PTTL(ReadOptions{}, "idem:request", &after).ok());
  EXPECT_GT(after, 0);
  EXPECT_LE(after, before);
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions{}, "idem:request", &value).ok());
  EXPECT_EQ(value, "accepted");
}

TEST_F(IdempotencyTest, ExpiredAndDeletedClaimsCanBeReusedWithoutOldValueResurrection) {
  ASSERT_TRUE(db_->Put(WriteOptions{}, "idem:request", "old").ok());
  auto* applier = dynamic_cast<WriteApplier*>(db_.get());
  ASSERT_NE(applier, nullptr);
  ASSERT_TRUE(applier->ApplyPutWithExpiry("idem:request", "expired", 1).ok());
  ASSERT_NO_FATAL_FAILURE(Reopen());
  WriteOptions write;
  write.ttl_ms = 60000;
  bool inserted = false;
  ASSERT_TRUE(db_->PutIfAbsent(write, "idem:request", "new", &inserted).ok());
  EXPECT_TRUE(inserted);
  ASSERT_TRUE(db_->Delete(WriteOptions{}, "idem:request").ok());
  ASSERT_NO_FATAL_FAILURE(Reopen());
  ASSERT_TRUE(
      db_->PutIfAbsent(WriteOptions{}, "idem:request", "persistent", &inserted).ok());
  EXPECT_TRUE(inserted);
  int64_t ttl = 0;
  ASSERT_TRUE(db_->PTTL(ReadOptions{}, "idem:request", &ttl).ok());
  EXPECT_EQ(ttl, -1);
  ASSERT_NO_FATAL_FAILURE(Reopen());
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions{}, "idem:request", &value).ok());
  EXPECT_EQ(value, "persistent");
}

TEST_F(IdempotencyTest, RejectedClaimDoesNotAddExpiryToAPersistentCachedValue) {
  ASSERT_TRUE(db_->Close().ok());
  options_.cache_enabled = true;
  ASSERT_TRUE(DB::Open(options_, &db_).ok());
  ASSERT_TRUE(db_->Put(WriteOptions{}, "idem:request", "original").ok());
  ASSERT_NO_FATAL_FAILURE(Reopen());
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions{}, "idem:request", &value).ok());
  WriteOptions write;
  write.ttl_ms = 1000;
  bool inserted = true;
  ASSERT_TRUE(db_->PutIfAbsent(write, "idem:request", "duplicate", &inserted).ok());
  EXPECT_FALSE(inserted);
  ASSERT_TRUE(db_->Get(ReadOptions{}, "idem:request", &value).ok());
  EXPECT_EQ(value, "original");
  int64_t ttl = 0;
  ASSERT_TRUE(db_->PTTL(ReadOptions{}, "idem:request", &ttl).ok());
  EXPECT_EQ(ttl, -1);
}

TEST_F(IdempotencyTest, ClaimAndTTLRecoverFromWALAfterFailedFlush) {
  WriteOptions write;
  write.sync = true;
  write.ttl_ms = 60000;
  bool inserted = false;
  ASSERT_TRUE(db_->PutIfAbsent(write, "idem:request", "accepted", &inserted).ok());
  ASSERT_TRUE(inserted);
  testing::InjectFailure(testing::FailurePoint::kAfterSSTableWriteBeforeManifest,
                         Status::IOError("claim flush failure"));
  EXPECT_TRUE(db_->Close().IsIOError());
  testing::ClearFailureInjection();
  db_.reset();
  ASSERT_TRUE(DB::Open(options_, &db_).ok());
  ASSERT_TRUE(db_->PutIfAbsent(write, "idem:request", "duplicate", &inserted).ok());
  EXPECT_FALSE(inserted);
  int64_t ttl = 0;
  ASSERT_TRUE(db_->PTTL(ReadOptions{}, "idem:request", &ttl).ok());
  EXPECT_GT(ttl, 0);
}

TEST_F(IdempotencyTest, InvalidClaimsDoNotWriteAndClosedDBReturnsAnError) {
  bool inserted = true;
  WriteOptions write;
  write.ttl_ms = -1;
  EXPECT_TRUE(db_->PutIfAbsent(write, "idem:request", "invalid", &inserted)
                  .IsInvalidArgument());
  EXPECT_FALSE(inserted);
  EXPECT_TRUE(
      db_->PutIfAbsent(WriteOptions{}, "", "invalid", &inserted).IsInvalidArgument());
  EXPECT_TRUE(db_->PutIfAbsent(WriteOptions{}, "idem:request", "invalid", nullptr)
                  .IsInvalidArgument());
  std::string value;
  EXPECT_TRUE(db_->Get(ReadOptions{}, "idem:request", &value).IsNotFound());
  ASSERT_TRUE(db_->Close().ok());
  EXPECT_TRUE(db_->PutIfAbsent(WriteOptions{}, "idem:request", "invalid", &inserted)
                  .IsIOError());
}

} // namespace
} // namespace kv
