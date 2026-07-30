#include "kv/version/compaction.h"

#include <chrono>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "kv/engine/db.h"

namespace kv {
namespace {

DBOptions MakeDBOptions(const std::string& name) {
  static int counter = 0;
  ++counter;

  DBOptions options;
  std::ostringstream oss;
  oss << "test_tmp/version/" << name << "_" << counter;
  const std::string base = oss.str();

  options.wal_path = base + ".wal";
  options.sst_dir = base + "_sst";
  options.manifest_path = base + ".manifest";
  options.memtable_write_buffer_size = 1;
  options.auto_compaction_enabled = false;
  return options;
}

void RemovePathIfExists(const std::string& path) {
  std::error_code ec;
  std::filesystem::remove(path, ec);
}

void RemoveDirIfExists(const std::string& path) {
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
}

size_t CountSSTFiles(const std::string& dir_path) {
  std::error_code ec;
  if (!std::filesystem::exists(dir_path, ec)) {
    return 0;
  }
  size_t count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(dir_path, ec)) {
    if (ec) {
      break;
    }
    if (entry.is_regular_file() && entry.path().extension() == ".sst") {
      ++count;
    }
  }
  return count;
}

bool WaitForSSTFileCount(const std::string& dir_path, size_t expected_count) {
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(1);
  while (std::chrono::steady_clock::now() < deadline) {
    if (CountSSTFiles(dir_path) == expected_count) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return CountSSTFiles(dir_path) == expected_count;
}

TEST(CompactionRunnerTest, MaybeRunOnceTreatsNotFoundAsNoop) {
  DBOptions options = MakeDBOptions("runner_noop");
  options.compaction_min_input_files = 3;
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());

  ASSERT_TRUE(db->Put(WriteOptions{}, "k1", "v1").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "k2", "v2").ok());
  ASSERT_TRUE(WaitForSSTFileCount(options.sst_dir, 2U));

  CompactionRunner runner(db.get());
  Status s = runner.MaybeRunOnce();
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(CountSSTFiles(options.sst_dir), 2U);
}

TEST(CompactionRunnerTest, RunManualReturnsNotFoundWhenBelowThreshold) {
  DBOptions options = MakeDBOptions("runner_manual_not_found");
  options.compaction_min_input_files = 3;
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());

  ASSERT_TRUE(db->Put(WriteOptions{}, "k1", "v1").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "k2", "v2").ok());

  CompactionRunner runner(db.get());
  Status s = runner.RunManual();
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNotFound());
}

TEST(CompactionRunnerTest, MaybeRunOnceCompactsWhenThresholdMet) {
  DBOptions options = MakeDBOptions("runner_trigger");
  options.compaction_min_input_files = 3;
  RemovePathIfExists(options.wal_path);
  RemovePathIfExists(options.manifest_path);
  RemoveDirIfExists(options.sst_dir);

  std::unique_ptr<DB> db;
  ASSERT_TRUE(DB::Open(options, &db).ok());

  ASSERT_TRUE(db->Put(WriteOptions{}, "a", "1").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "b", "2").ok());
  ASSERT_TRUE(db->Put(WriteOptions{}, "c", "3").ok());
  ASSERT_TRUE(WaitForSSTFileCount(options.sst_dir, 3U));

  CompactionRunner runner(db.get());
  Status s = runner.MaybeRunOnce();
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(CountSSTFiles(options.sst_dir), 1U);
}

}  // namespace
}  // namespace kv
