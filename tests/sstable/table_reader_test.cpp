#include <filesystem>
#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "kv/sstable/table_builder.h"
#include "kv/sstable/table_reader.h"

namespace kv {
namespace {

TEST(TableReaderTest, ReturnsLatestVersionAcrossDataBlocks) {
  const std::filesystem::path dir("test_tmp/sstable");
  std::filesystem::create_directories(dir);
  const std::string file_path = (dir / "cross_block_versions.sst").string();

  std::error_code ec;
  std::filesystem::remove(file_path, ec);

  {
    TableBuilder builder(file_path, 256);
    for (int i = 39; i >= 0; --i) {
      const std::string value = "v" + std::to_string(i);
      ASSERT_TRUE(builder.Add("key", static_cast<uint64_t>(i + 1), 0, value).ok());
    }
    ASSERT_TRUE(builder.Finish().ok());
  }

  std::unique_ptr<TableReader> reader;
  ASSERT_TRUE(TableReader::Open(file_path, &reader).ok());
  ASSERT_GT(reader->NumDataBlocks(), 1U);

  uint8_t type = 0;
  std::string value;
  ASSERT_TRUE(reader->Get("key", 40, &type, &value).ok());
  EXPECT_EQ(type, 0);
  EXPECT_EQ(value, "v39");

  std::filesystem::remove(file_path, ec);
}

}  // namespace
}  // namespace kv
