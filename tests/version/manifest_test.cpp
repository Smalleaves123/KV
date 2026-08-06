#include "kv/version/manifest.h"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace kv {
namespace {

std::string MakeManifestPath(const std::string& name) {
  static int counter = 0;
  ++counter;
  std::ostringstream oss;
  oss << "test_tmp/version/" << name << "_" << counter << ".manifest";
  return oss.str();
}

void RemovePathIfExists(const std::string& path) {
  std::error_code ec;
  std::filesystem::remove(path, ec);
}

TEST(ManifestTest, OpenAndAppendAndRecover) {
  const std::string path = MakeManifestPath("open_append_recover");
  RemovePathIfExists(path);

  Manifest manifest;
  ASSERT_TRUE(manifest.Open(path, true).ok());
  EXPECT_TRUE(manifest.IsOpen());

  ManifestFileMeta f1;
  f1.file_number = 1;
  f1.file_path = "test_tmp/db/sst/00000000000000000001.sst";
  f1.max_seq = 100;

  ManifestFileMeta f2;
  f2.file_number = 2;
  f2.file_path = "test_tmp/db/sst/00000000000000000002.sst";
  f2.max_seq = 160;

  ASSERT_TRUE(manifest.AddFile(f1).ok());
  ASSERT_TRUE(manifest.AddFile(f2).ok());
  ASSERT_TRUE(manifest.Close().ok());

  ASSERT_TRUE(manifest.Open(path, false).ok());
  std::vector<ManifestFileMeta> files;
  ASSERT_TRUE(manifest.Recover(&files).ok());
  ASSERT_EQ(files.size(), 2U);
  EXPECT_EQ(files[0].file_number, 1U);
  EXPECT_EQ(files[0].file_path, f1.file_path);
  EXPECT_EQ(files[0].max_seq, 100U);
  EXPECT_EQ(files[1].file_number, 2U);
  EXPECT_EQ(files[1].file_path, f2.file_path);
  EXPECT_EQ(files[1].max_seq, 160U);
}

TEST(ManifestTest, SyncPersistsAppendedRecord) {
  const std::string path = MakeManifestPath("sync_appended_record");
  RemovePathIfExists(path);

  ManifestFileMeta file;
  file.file_number = 1;
  file.file_path = "test_tmp/db/sst/00000000000000000001.sst";
  file.max_seq = 100;

  Manifest manifest;
  ASSERT_TRUE(manifest.Open(path, true).ok());
  ASSERT_TRUE(manifest.AddFile(file).ok());
  ASSERT_TRUE(manifest.Sync().ok());
  ASSERT_TRUE(manifest.Close().ok());

  ASSERT_TRUE(manifest.Open(path, false).ok());
  std::vector<ManifestFileMeta> files;
  ASSERT_TRUE(manifest.Recover(&files).ok());
  ASSERT_EQ(files.size(), 1U);
  EXPECT_EQ(files[0].file_number, file.file_number);
  EXPECT_EQ(files[0].file_path, file.file_path);
  EXPECT_EQ(files[0].max_seq, file.max_seq);
}

TEST(ManifestTest, OpenMissingFileWithoutCreateFails) {
  const std::string path = MakeManifestPath("missing_open_fail");
  RemovePathIfExists(path);

  Manifest manifest;
  Status s = manifest.Open(path, false);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNotFound());
}

TEST(ManifestTest, RejectInvalidAddFileInput) {
  const std::string path = MakeManifestPath("invalid_add_file");
  RemovePathIfExists(path);

  Manifest manifest;
  ASSERT_TRUE(manifest.Open(path, true).ok());

  ManifestFileMeta invalid;
  invalid.file_number = 0;
  invalid.file_path = "a.sst";
  Status s = manifest.AddFile(invalid);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());

  invalid.file_number = 1;
  invalid.file_path.clear();
  s = manifest.AddFile(invalid);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST(ManifestTest, RecoverAppliesRemoveFileRecords) {
  const std::string path = MakeManifestPath("recover_remove_file");
  RemovePathIfExists(path);

  Manifest manifest;
  ASSERT_TRUE(manifest.Open(path, true).ok());

  ManifestFileMeta f1;
  f1.file_number = 1;
  f1.file_path = "test_tmp/db/sst/00000000000000000001.sst";
  f1.max_seq = 100;

  ManifestFileMeta f2;
  f2.file_number = 2;
  f2.file_path = "test_tmp/db/sst/00000000000000000002.sst";
  f2.max_seq = 120;

  ASSERT_TRUE(manifest.AddFile(f1).ok());
  ASSERT_TRUE(manifest.AddFile(f2).ok());
  ASSERT_TRUE(manifest.RemoveFile(f1.file_number).ok());
  ASSERT_TRUE(manifest.Close().ok());

  ASSERT_TRUE(manifest.Open(path, false).ok());
  std::vector<ManifestFileMeta> files;
  ASSERT_TRUE(manifest.Recover(&files).ok());
  ASSERT_EQ(files.size(), 1U);
  EXPECT_EQ(files[0].file_number, f2.file_number);
  EXPECT_EQ(files[0].file_path, f2.file_path);
  EXPECT_EQ(files[0].max_seq, f2.max_seq);
}

TEST(ManifestTest, RecoverIgnoresPartialTrailingAddFileRecord) {
  const std::string path = MakeManifestPath("partial_trailing_add");
  RemovePathIfExists(path);

  ManifestFileMeta f1;
  f1.file_number = 1;
  f1.file_path = "test_tmp/db/sst/00000000000000000001.sst";
  f1.max_seq = 100;

  ManifestFileMeta f2;
  f2.file_number = 2;
  f2.file_path = "test_tmp/db/sst/00000000000000000002.sst";
  f2.max_seq = 200;

  {
    Manifest manifest;
    ASSERT_TRUE(manifest.Open(path, true).ok());
    ASSERT_TRUE(manifest.AddFile(f1).ok());
    ASSERT_TRUE(manifest.AddFile(f2).ok());
    ASSERT_TRUE(manifest.Close().ok());
  }

  std::error_code ec;
  const auto size = std::filesystem::file_size(path, ec);
  ASSERT_FALSE(ec);
  ASSERT_GT(size, 4U);
  std::filesystem::resize_file(path, size - 4, ec);
  ASSERT_FALSE(ec);

  Manifest manifest;
  ASSERT_TRUE(manifest.Open(path, false).ok());

  std::vector<ManifestFileMeta> files;
  ASSERT_TRUE(manifest.Recover(&files).ok());
  ASSERT_EQ(files.size(), 1U);
  EXPECT_EQ(files[0].file_number, f1.file_number);
  EXPECT_EQ(files[0].file_path, f1.file_path);
  EXPECT_EQ(files[0].max_seq, f1.max_seq);
}

TEST(ManifestTest, RejectsOversizedPathBeforeAllocation) {
  const std::string path = MakeManifestPath("oversized_path");
  RemovePathIfExists(path);

  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(out.is_open());
  const char add_tag = 1;
  out.write(&add_tag, 1);
  auto write_u64 = [&out](uint64_t value) {
    for (size_t i = 0; i < sizeof(uint64_t); ++i) {
      const char byte = static_cast<char>((value >> (8 * i)) & 0xFF);
      out.write(&byte, 1);
    }
  };
  auto write_u32 = [&out](uint32_t value) {
    for (size_t i = 0; i < sizeof(uint32_t); ++i) {
      const char byte = static_cast<char>((value >> (8 * i)) & 0xFF);
      out.write(&byte, 1);
    }
  };
  write_u64(1);
  write_u64(1);
  write_u32(static_cast<uint32_t>(Manifest::kMaxFilePathSize + 1));
  out.close();

  Manifest manifest;
  ASSERT_TRUE(manifest.Open(path, false).ok());
  std::vector<ManifestFileMeta> files;
  Status s = manifest.Recover(&files);
  EXPECT_TRUE(s.IsCorruption()) << s.ToString();
}

}  // namespace
}  // namespace kv
