// TableBuilder regression coverage.
#include "kv/sstable/table_builder.h"
#include "kv/sstable/table_reader.h"

#include "gtest/gtest.h"
#include <filesystem>
#include <memory>

namespace kv {
namespace {

TEST(TableBuilderTest, FinishedTableCanBeReopenedAndRead) {
  const std::filesystem::path path =
      std::filesystem::path("test_tmp/sstable") / "builder_reopen.sst";
  std::filesystem::create_directories(path.parent_path());
  std::error_code ec;
  std::filesystem::remove(path, ec);

  {
    TableBuilder builder(path.string(), 256, 10);
    ASSERT_TRUE(builder.Add("alpha", 1, 0, "one").ok());
    ASSERT_TRUE(builder.Add("bravo", 2, 0, "two").ok());
    ASSERT_TRUE(builder.Finish().ok());
  }

  std::unique_ptr<TableReader> reader;
  ASSERT_TRUE(TableReader::Open(path.string(), &reader).ok());
  uint8_t type = 0;
  std::string value;
  ASSERT_TRUE(reader->Get("bravo", 2, &type, &value).ok());
  EXPECT_EQ(type, 0);
  EXPECT_EQ(value, "two");

  std::filesystem::remove(path, ec);
}

} // namespace
} // namespace kv
