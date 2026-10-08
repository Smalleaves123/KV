#include "server_config.h"

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "kv/engine/write_batch.h"
#include "kv/version/manifest.h"

namespace kv::app {
namespace {

class ServerConfigTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ = std::string("test_tmp/server_config/") +
            ::testing::UnitTest::GetInstance()->current_test_info()->name();
    std::filesystem::remove_all(root_);
    std::filesystem::create_directories(root_);
  }

  void TearDown() override { std::filesystem::remove_all(root_); }

  void LoadOptions(const std::string& storage, DBOptions* options) {
    const std::string path = root_ + "/server.yaml";
    {
      std::ofstream out(path);
      out << "server:\n  db_path: " << root_ << "/db\nstorage:\n" << storage;
      ASSERT_TRUE(out.good());
    }
    std::string error;
    const auto config = LoadConfigFile(path, &error);
    ASSERT_TRUE(config.has_value()) << error;
    *options = BuildDBOptions(*config);
  }

  void ReadFiles(const DBOptions& options, std::vector<ManifestFileMeta>* files) {
    Manifest manifest;
    ASSERT_TRUE(manifest.Open(options.db_path + "/MANIFEST", false).ok());
    ASSERT_TRUE(manifest.Recover(files).ok());
  }

  WriteBatch MakeBatch() {
    WriteBatch batch;
    for (int i = 0; i < 40; ++i) {
      batch.Put("key" + std::to_string(100 + i), std::string(128, 'v'));
    }
    return batch;
  }

  std::string root_;
};

TEST_F(ServerConfigTest, OmittedCompactionOptionsUseEngineDefaults) {
  DBOptions options;
  ASSERT_NO_FATAL_FAILURE(LoadOptions("  auto_compaction_enabled: false\n", &options));
  const DBOptions defaults;
  EXPECT_EQ(options.compaction_output_file_size_bytes,
            defaults.compaction_output_file_size_bytes);
  EXPECT_EQ(options.compaction_level_max_bytes, defaults.compaction_level_max_bytes);
}

TEST_F(ServerConfigTest, YamlOutputSizeControlsSplittingIncludingZero) {
  for (size_t size : {size_t{256}, size_t{0}}) {
    DBOptions options;
    ASSERT_NO_FATAL_FAILURE(LoadOptions(
        "  compaction_output_file_size_bytes: " + std::to_string(size) +
        "\n  compaction_min_input_files: 1\n"
        "  sstable_block_size_bytes: 128\n"
        "  auto_compaction_enabled: false\n", &options));
    options.db_path += "/size_" + std::to_string(size);
    EXPECT_EQ(options.compaction_output_file_size_bytes, size);
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    ASSERT_TRUE(db->Write(WriteOptions{}, MakeBatch()).ok());
    ASSERT_TRUE(db->Compact().ok());
    ASSERT_TRUE(db->Close().ok());
    std::vector<ManifestFileMeta> files;
    ASSERT_NO_FATAL_FAILURE(ReadFiles(options, &files));
    if (size == 0) {
      EXPECT_EQ(files.size(), 1U);
    } else {
      EXPECT_GT(files.size(), 1U);
    }
    for (const auto& file : files) EXPECT_EQ(file.level, 1U);
  }
}

TEST_F(ServerConfigTest, YamlLevelBudgetControlsAutomaticL2Compaction) {
  for (size_t budget : {size_t{0}, size_t{1024 * 1024}}) {
    DBOptions options;
    ASSERT_NO_FATAL_FAILURE(LoadOptions(
        "  compaction_level_max_bytes: " + std::to_string(budget) +
        "\n  compaction_output_file_size_bytes: 256\n"
        "  compaction_min_input_files: 1\n"
        "  sstable_block_size_bytes: 128\n"
        "  memtable_write_buffer_size: 1\n"
        "  auto_compaction_enabled: true\n", &options));
    options.db_path += "/budget_" + std::to_string(budget);
    EXPECT_EQ(options.compaction_level_max_bytes, budget);
    std::unique_ptr<DB> db;
    ASSERT_TRUE(DB::Open(options, &db).ok());
    ASSERT_TRUE(db->Write(WriteOptions{}, MakeBatch()).ok());
    // Closing drains the flush worker and its automatic compaction passes.
    ASSERT_TRUE(db->Close().ok());
    CompactionStats stats;
    ASSERT_TRUE(db->GetCompactionStats(&stats).ok());
    EXPECT_EQ(stats.succeeded, budget == 0 ? 2U : 1U);
    EXPECT_EQ(stats.failed, 0U);
    std::vector<ManifestFileMeta> files;
    ASSERT_NO_FATAL_FAILURE(ReadFiles(options, &files));
    ASSERT_FALSE(files.empty());
    for (const auto& file : files) EXPECT_EQ(file.level, budget == 0 ? 2U : 1U);
  }
}

}  // namespace
}  // namespace kv::app
