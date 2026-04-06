#include "kv/version/version_set.h"

#include <filesystem>
#include <string>

#include "gtest/gtest.h"
#include "kv/version/manifest.h"

namespace kv {
namespace {

TEST(VersionTest, FindFileReturnsFalseOnMissing) {
  Version v;
  FileMeta out;
  EXPECT_FALSE(v.FindFile(42, &out));
}

TEST(VersionTest, AddAndFindReturnsExactMeta) {
  Version v;
  FileMeta meta;
  meta.file_number = 7;
  meta.file_path = "data/00000000000000000007.sst";
  meta.max_seq = 123;
  meta.file_size = 4096;

  v.AddFile(meta);
  EXPECT_EQ(v.FileCount(), 1U);

  FileMeta out;
  ASSERT_TRUE(v.FindFile(7, &out));
  EXPECT_EQ(out.file_number, 7U);
  EXPECT_EQ(out.file_path, meta.file_path);
  EXPECT_EQ(out.max_seq, 123U);
  EXPECT_EQ(out.file_size, 4096U);
}

TEST(VersionTest, RemoveFileShrinksSet) {
  Version v;
  FileMeta meta;
  meta.file_number = 1;
  meta.file_path = "a.sst";
  v.AddFile(meta);

  EXPECT_TRUE(v.RemoveFile(1));
  EXPECT_EQ(v.FileCount(), 0U);
  EXPECT_FALSE(v.RemoveFile(1));
}

TEST(VersionSetTest, ApplyAddRemoveUpdatesCurrent) {
  VersionSet vs;
  FileMeta f1;
  f1.file_number = 2;
  f1.file_path = "b.sst";
  f1.max_seq = 20;

  vs.ApplyAdd(f1);
  EXPECT_EQ(vs.current().FileCount(), 1U);

  EXPECT_TRUE(vs.ApplyRemove(2));
  EXPECT_EQ(vs.current().FileCount(), 0U);
}

TEST(VersionSetTest, RecoverFromManifestBuildsCurrent) {
  const std::string path = "test_tmp/version/recover_vs.manifest";
  std::error_code ec;
  std::filesystem::remove(path, ec);

  Manifest manifest;
  ASSERT_TRUE(manifest.Open(path, true).ok());

  ManifestFileMeta f1{1, "a.sst", 10};
  ManifestFileMeta f2{2, "b.sst", 20};
  ASSERT_TRUE(manifest.AddFile(f1).ok());
  ASSERT_TRUE(manifest.AddFile(f2).ok());

  VersionSet vs;
  ASSERT_TRUE(vs.RecoverFromManifest(&manifest).ok());
  EXPECT_EQ(vs.current().FileCount(), 2U);

  FileMeta out;
  EXPECT_TRUE(vs.current().FindFile(1, &out));
  EXPECT_TRUE(vs.current().FindFile(2, &out));
}

}  // namespace
}  // namespace kv
