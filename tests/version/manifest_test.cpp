#include "kv/version/manifest.h"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "kv/common/encoding.h"

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
  f1.level = 2;
  f1.file_size = (uint64_t{1} << 33) + 42;
  f1.smallest_key = std::string("a\0first", 7);
  f1.largest_key = std::string("z\xfflast", 6);

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
  EXPECT_EQ(files[0].level, f1.level);
  EXPECT_EQ(files[0].file_size, f1.file_size);
  EXPECT_EQ(files[0].smallest_key, f1.smallest_key);
  EXPECT_EQ(files[0].largest_key, f1.largest_key);
  EXPECT_EQ(files[1].file_number, 2U);
  EXPECT_EQ(files[1].file_path, f2.file_path);
  EXPECT_EQ(files[1].max_seq, 160U);
  EXPECT_FALSE(files[1].HasKeyRange());
}

// Encode the original on-disk layout independently of the current writer.
std::string LegacyAddRecord(uint64_t number, const std::string& path,
                            uint64_t max_seq) {
  std::string record(1, '\x01');
  EncodeFixed64(&record, number);
  EncodeFixed64(&record, max_seq);
  EncodeFixed32(&record, static_cast<uint32_t>(path.size()));
  record.append(path);
  return record;
}

TEST(ManifestTest, LegacyAndMetadataRecordsCanCoexistAndBeRemoved) {
  const std::string path = MakeManifestPath("mixed_records");
  std::filesystem::create_directories(std::filesystem::path(path).parent_path());
  {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out << LegacyAddRecord(1, "legacy.sst", 5);
    ASSERT_TRUE(out.good());
  }

  Manifest manifest;
  ASSERT_TRUE(manifest.Open(path, false).ok());
  ManifestFileMeta file;
  file.file_number = 2;
  file.file_path = "new.sst";
  file.max_seq = 9;
  file.level = 1;
  file.file_size = 4096;
  file.smallest_key = "alpha";
  file.largest_key = "omega";
  ASSERT_TRUE(manifest.AddFile(file).ok());

  std::vector<ManifestFileMeta> files;
  ASSERT_TRUE(manifest.Recover(&files).ok());
  ASSERT_EQ(files.size(), 2U);
  EXPECT_EQ(files[0].file_number, 1U);
  EXPECT_EQ(files[0].max_seq, 5U);
  EXPECT_EQ(files[0].level, 0U);
  EXPECT_EQ(files[0].file_size, 0U);
  EXPECT_FALSE(files[0].HasKeyRange());
  EXPECT_EQ(files[1].smallest_key, "alpha");
  EXPECT_EQ(files[1].largest_key, "omega");
  EXPECT_EQ(files[1].level, 1U);
  ASSERT_TRUE(manifest.RemoveFile(1).ok());
  ASSERT_TRUE(manifest.RemoveFile(2).ok());
  ASSERT_TRUE(manifest.Recover(&files).ok());
  EXPECT_TRUE(files.empty());
}

TEST(ManifestTest, EveryTruncatedMetadataTailLeavesPriorFilesIntact) {
  const std::string path = MakeManifestPath("metadata_tail");
  RemovePathIfExists(path);
  ManifestFileMeta file;
  file.file_number = 2;
  file.file_path = "new.sst";
  file.max_seq = 20;
  file.file_size = 1024;
  file.smallest_key = "alpha";
  file.largest_key = "omega";
  {
    Manifest manifest;
    ASSERT_TRUE(manifest.Open(path, true).ok());
    ASSERT_TRUE(manifest.AddFile(file).ok());
    ASSERT_TRUE(manifest.Close().ok());
  }
  std::ifstream input(path, std::ios::binary);
  const std::string record((std::istreambuf_iterator<char>(input)), {});
  input.close();
  ASSERT_GT(record.size(), 41U);
  EXPECT_EQ(static_cast<unsigned char>(record[0]), 4U);

  for (size_t cut = 0; cut < record.size(); ++cut) {
    SCOPED_TRACE(cut);
    {
      std::ofstream out(path, std::ios::binary | std::ios::trunc);
      out << LegacyAddRecord(1, "old.sst", 10);
      out.write(record.data(), static_cast<std::streamsize>(cut));
      ASSERT_TRUE(out.good());
    }
    Manifest manifest;
    ASSERT_TRUE(manifest.Open(path, false).ok());
    std::vector<ManifestFileMeta> files;
    ASSERT_TRUE(manifest.Recover(&files).ok());
    ASSERT_EQ(files.size(), 1U);
    EXPECT_EQ(files[0].file_number, 1U);
    EXPECT_EQ(files[0].max_seq, 10U);
  }
}

TEST(ManifestTest, ChecksumsRejectCorruptedAddAndRemoveRecords) {
  const std::string path = MakeManifestPath("checksums");
  RemovePathIfExists(path);
  ManifestFileMeta file;
  file.file_number = 1;
  file.file_path = "one.sst";
  file.max_seq = 7;
  file.smallest_key = "a";
  file.largest_key = "z";

  {
    Manifest manifest;
    ASSERT_TRUE(manifest.Open(path, true).ok());
    ASSERT_TRUE(manifest.AddFile(file).ok());
    ASSERT_TRUE(manifest.RemoveFile(file.file_number).ok());
    ASSERT_TRUE(manifest.Close().ok());
  }
  std::ifstream input(path, std::ios::binary);
  std::string records((std::istreambuf_iterator<char>(input)), {});
  input.close();
  ASSERT_GT(records.size(), 4U);

  // Corrupt a byte in the add-file payload while leaving its checksum intact.
  records[records.size() / 2] ^= 0x01;
  {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out << records;
  }
  Manifest manifest;
  ASSERT_TRUE(manifest.Open(path, false).ok());
  std::vector<ManifestFileMeta> files;
  EXPECT_TRUE(manifest.Recover(&files).IsCorruption());
}

TEST(ManifestTest, InvalidKeyRangesAreRejectedBeforeAppend) {
  const std::string path = MakeManifestPath("invalid_ranges");
  RemovePathIfExists(path);
  Manifest manifest;
  ASSERT_TRUE(manifest.Open(path, true).ok());
  ManifestFileMeta file;
  file.file_number = 1;
  file.file_path = "bad.sst";
  file.smallest_key = "z";
  EXPECT_TRUE(manifest.AddFile(file).IsInvalidArgument());
  file.largest_key = "a";
  EXPECT_TRUE(manifest.AddFile(file).IsInvalidArgument());
  std::vector<ManifestFileMeta> files;
  ASSERT_TRUE(manifest.Recover(&files).ok());
  EXPECT_TRUE(files.empty());
}

TEST(ManifestTest, CorruptMetadataLengthsAndBoundsAreRejected) {
  const std::string path = MakeManifestPath("corrupt_metadata");
  std::filesystem::create_directories(std::filesystem::path(path).parent_path());
  for (int scenario = 0; scenario < 3; ++scenario) {
    SCOPED_TRACE(scenario);
    std::string record = LegacyAddRecord(1, "bad.sst", 1);
    record[0] = '\x03';
    EncodeFixed32(&record, 0);
    EncodeFixed64(&record, 128);
    // Oversized allocation, missing half of a range, and reversed bounds.
    EncodeFixed32(&record, scenario == 0 ? Manifest::kMaxKeySize + 1 : 1);
    EncodeFixed32(&record, scenario == 1 ? 0 : 1);
    record.append("za");
    {
      std::ofstream out(path, std::ios::binary | std::ios::trunc);
      out << record;
      ASSERT_TRUE(out.good());
    }
    Manifest manifest;
    ASSERT_TRUE(manifest.Open(path, false).ok());
    std::vector<ManifestFileMeta> files;
    EXPECT_TRUE(manifest.Recover(&files).IsCorruption());
    EXPECT_TRUE(files.empty());
  }
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
