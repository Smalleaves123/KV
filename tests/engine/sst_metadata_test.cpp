#include "kv/engine/db.h"

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "kv/common/encoding.h"
#include "kv/engine/write_applier.h"
#include "kv/version/manifest.h"

namespace kv {
namespace {

class SSTMetadataTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ = std::string("test_tmp/sst_metadata/") +
            ::testing::UnitTest::GetInstance()->current_test_info()->name();
    std::filesystem::remove_all(root_);
    options_.db_path = root_ + "/db";
    options_.memtable_write_buffer_size = 0;
    options_.auto_compaction_enabled = false;
    ASSERT_TRUE(DB::Open(options_, &db_).ok());
  }

  void TearDown() override {
    if (db_) {
      (void)db_->Close();
      db_.reset();
    }
    std::error_code ec;
    std::filesystem::remove_all(root_, ec);
  }

  void ReadFiles(const std::string& db_path,
                 std::vector<ManifestFileMeta>* files) {
    Manifest manifest;
    ASSERT_TRUE(manifest.Open(db_path + "/MANIFEST", false).ok());
    ASSERT_TRUE(manifest.Recover(files).ok());
  }

  void ExpectFile(const ManifestFileMeta& file, const std::string& smallest,
                  const std::string& largest, uint64_t max_seq,
                  uint32_t level = 0) {
    EXPECT_EQ(file.level, level);
    EXPECT_EQ(file.smallest_key, smallest);
    EXPECT_EQ(file.largest_key, largest);
    EXPECT_EQ(file.max_seq, max_seq);
    std::error_code ec;
    const uint64_t actual_size = std::filesystem::file_size(file.file_path, ec);
    ASSERT_FALSE(ec);
    EXPECT_GT(actual_size, 0U);
    EXPECT_EQ(file.file_size, actual_size);
  }

  std::string root_;
  DBOptions options_;
  std::unique_ptr<DB> db_;
};

TEST_F(SSTMetadataTest, FlushAndCompactionKeepBoundsIncludingTombstonesAndTTL) {
  ASSERT_TRUE(db_->Put(WriteOptions{}, "z", "last").ok());
  ASSERT_TRUE(db_->Put(WriteOptions{}, "a", "first").ok());
  ASSERT_TRUE(db_->Put(WriteOptions{}, "m", "middle").ok());
  ASSERT_TRUE(db_->Close().ok());

  std::vector<ManifestFileMeta> files;
  ASSERT_NO_FATAL_FAILURE(ReadFiles(options_.db_path, &files));
  ASSERT_EQ(files.size(), 1U);
  ASSERT_NO_FATAL_FAILURE(ExpectFile(files[0], "a", "z", 3));

  ASSERT_TRUE(DB::Open(options_, &db_).ok());
  ASSERT_TRUE(db_->Delete(WriteOptions{}, "a").ok());
  ASSERT_TRUE(db_->Expire(WriteOptions{}, "z", 3600).ok());
  ASSERT_TRUE(db_->Put(WriteOptions{}, "n", "new").ok());
  ASSERT_TRUE(db_->Close().ok());
  ASSERT_NO_FATAL_FAILURE(ReadFiles(options_.db_path, &files));
  ASSERT_EQ(files.size(), 2U);
  ASSERT_NO_FATAL_FAILURE(ExpectFile(files[1], "a", "z", 6));

  ASSERT_TRUE(DB::Open(options_, &db_).ok());
  ASSERT_TRUE(db_->Compact().ok());
  ASSERT_TRUE(db_->Close().ok());
  ASSERT_NO_FATAL_FAILURE(ReadFiles(options_.db_path, &files));
  ASSERT_EQ(files.size(), 1U);
  ASSERT_NO_FATAL_FAILURE(ExpectFile(files[0], "a", "z", 6, 1));

  ASSERT_TRUE(DB::Open(options_, &db_).ok());
  std::string value;
  EXPECT_TRUE(db_->Get(ReadOptions{}, "a", &value).IsNotFound());
  ASSERT_TRUE(db_->Get(ReadOptions{}, "z", &value).ok());
  EXPECT_EQ(value, "last");
  int64_t ttl = -2;
  ASSERT_TRUE(db_->TTL(ReadOptions{}, "z", &ttl).ok());
  EXPECT_GT(ttl, 0);
}

TEST_F(SSTMetadataTest, CheckpointAndInstallationPreserveFileMetadata) {
  const std::string smallest("a\0key", 5);
  const std::string largest("z\xffkey", 5);
  ASSERT_TRUE(db_->Put(WriteOptions{}, largest, "last").ok());
  ASSERT_TRUE(db_->Put(WriteOptions{}, smallest, "first").ok());
  const std::string checkpoint = root_ + "/checkpoint";
  ASSERT_TRUE(db_->CreateCheckpoint(checkpoint).ok());

  std::vector<ManifestFileMeta> original;
  ASSERT_NO_FATAL_FAILURE(ReadFiles(options_.db_path, &original));
  ASSERT_EQ(original.size(), 1U);
  ASSERT_NO_FATAL_FAILURE(ExpectFile(original[0], smallest, largest, 2));

  DBOptions target_options;
  target_options.db_path = root_ + "/target";
  std::unique_ptr<DB> target;
  ASSERT_TRUE(DB::Open(target_options, &target).ok());
  ASSERT_TRUE(target->Put(WriteOptions{}, "old", "live").ok());
  auto* applier = dynamic_cast<WriteApplier*>(target.get());
  ASSERT_NE(applier, nullptr);
  ASSERT_TRUE(applier->InstallCheckpoint(checkpoint).ok());
  ASSERT_TRUE(target->Close().ok());

  for (const auto& path : {checkpoint, target_options.db_path}) {
    std::vector<ManifestFileMeta> files;
    ASSERT_NO_FATAL_FAILURE(ReadFiles(path, &files));
    ASSERT_EQ(files.size(), 1U);
    ASSERT_NO_FATAL_FAILURE(ExpectFile(files[0], smallest, largest, 2));
    EXPECT_EQ(files[0].file_size, original[0].file_size);
    EXPECT_EQ(files[0].file_number, original[0].file_number);
  }
  ASSERT_TRUE(DB::Open(target_options, &target).ok());
  std::string value;
  ASSERT_TRUE(target->Get(ReadOptions{}, smallest, &value).ok());
  EXPECT_EQ(value, "first");
  EXPECT_TRUE(target->Get(ReadOptions{}, "old", &value).IsNotFound());
  ASSERT_TRUE(target->Close().ok());
}

TEST_F(SSTMetadataTest, LegacyDatabaseReopensAndCanAppendMetadataRecords) {
  ASSERT_TRUE(db_->Put(WriteOptions{}, "legacy", "value").ok());
  ASSERT_TRUE(db_->Close().ok());
  std::vector<ManifestFileMeta> files;
  ASSERT_NO_FATAL_FAILURE(ReadFiles(options_.db_path, &files));
  ASSERT_EQ(files.size(), 1U);
  const auto& file = files[0];
  std::string legacy(1, '\x01');
  EncodeFixed64(&legacy, file.file_number);
  EncodeFixed64(&legacy, file.max_seq);
  EncodeFixed32(&legacy, static_cast<uint32_t>(file.file_path.size()));
  legacy.append(file.file_path);
  {
    std::ofstream out(options_.db_path + "/MANIFEST",
                       std::ios::binary | std::ios::trunc);
    out << legacy;
    ASSERT_TRUE(out.good());
  }
  // Force the reopened read to use the SST described by a legacy record.
  std::filesystem::remove_all(options_.db_path + "/wal");
  options_.create_if_missing = false;
  ASSERT_TRUE(DB::Open(options_, &db_).ok());
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions{}, "legacy", &value).ok());
  EXPECT_EQ(value, "value");
  ASSERT_TRUE(db_->Put(WriteOptions{}, "new", "value").ok());
  ASSERT_TRUE(db_->Close().ok());

  ASSERT_NO_FATAL_FAILURE(ReadFiles(options_.db_path, &files));
  ASSERT_EQ(files.size(), 2U);
  EXPECT_EQ(files[0].level, 0U);
  EXPECT_EQ(files[0].file_size, 0U);
  EXPECT_FALSE(files[0].HasKeyRange());
  ASSERT_NO_FATAL_FAILURE(ExpectFile(files[1], "new", "new", 2));

  ASSERT_TRUE(DB::Open(options_, &db_).ok());
  ASSERT_TRUE(db_->Compact().ok());
  ASSERT_TRUE(db_->Close().ok());
  ASSERT_NO_FATAL_FAILURE(ReadFiles(options_.db_path, &files));
  ASSERT_EQ(files.size(), 1U);
  ASSERT_NO_FATAL_FAILURE(ExpectFile(files[0], "legacy", "new", 2, 1));
}

TEST_F(SSTMetadataTest, L0CompactionOnlyRewritesOverlappingL1Files) {
  ASSERT_TRUE(db_->Close().ok());
  db_.reset();
  options_.compaction_min_input_files = 1;
  options_.auto_compaction_enabled = false;
  options_.memtable_write_buffer_size = 1;
  ASSERT_TRUE(DB::Open(options_, &db_).ok());
  ASSERT_TRUE(db_->Put(WriteOptions{}, "a", "one").ok());
  ASSERT_TRUE(db_->Put(WriteOptions{}, "b", "two").ok());
  ASSERT_TRUE(db_->Compact().ok());
  std::vector<ManifestFileMeta> first_files;
  ASSERT_NO_FATAL_FAILURE(ReadFiles(options_.db_path, &first_files));
  ASSERT_EQ(first_files.size(), 1U);
  ASSERT_EQ(first_files[0].level, 1U);
  ASSERT_EQ(first_files[0].smallest_key, "a");
  ASSERT_EQ(first_files[0].largest_key, "b");

  ASSERT_TRUE(db_->Put(WriteOptions{}, "z", "last").ok());
  ASSERT_TRUE(db_->Compact().ok());
  ASSERT_TRUE(db_->Close().ok());

  std::vector<ManifestFileMeta> files;
  ASSERT_NO_FATAL_FAILURE(ReadFiles(options_.db_path, &files));
  ASSERT_EQ(files.size(), 2U);
  ASSERT_EQ(files[0].level, 1U);
  ASSERT_EQ(files[0].smallest_key, "a");
  ASSERT_EQ(files[0].largest_key, "b");
  ASSERT_EQ(files[1].level, 1U);
  ASSERT_EQ(files[1].smallest_key, "z");
  ASSERT_EQ(files[1].largest_key, "z");

  ASSERT_TRUE(DB::Open(options_, &db_).ok());
  std::string value;
  ASSERT_TRUE(db_->Get(ReadOptions{}, "a", &value).ok());
  EXPECT_EQ(value, "one");
  ASSERT_TRUE(db_->Get(ReadOptions{}, "z", &value).ok());
  EXPECT_EQ(value, "last");
}

}  // namespace
}  // namespace kv
