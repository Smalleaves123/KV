#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "kv/common/encoding.h"
#include "kv/sstable/block_builder.h"
#include "kv/sstable/filter_block.h"
#include "kv/sstable/footer.h"
#include "kv/sstable/table_builder.h"
#include "kv/sstable/table_reader.h"

namespace kv {
namespace {

Footer ReadFooter(const std::string& file_path) {
  std::ifstream file(file_path, std::ios::binary);
  EXPECT_TRUE(file.is_open());
  file.seekg(-static_cast<std::streamoff>(kFooterEncodedSize), std::ios::end);
  std::string encoded(kFooterEncodedSize, '\0');
  file.read(encoded.data(), static_cast<std::streamsize>(encoded.size()));
  EXPECT_TRUE(file.good());
  bool ok = false;
  Footer footer = Footer::DecodeFrom(encoded, &ok);
  EXPECT_TRUE(ok);
  return footer;
}

TEST(TableBuilderTest, ConfigurableBlockAndBloomSizesAffectLayout) {
  const std::filesystem::path dir("test_tmp/sstable");
  std::filesystem::create_directories(dir);
  const std::string small_path = (dir / "config_small.sst").string();
  const std::string large_path = (dir / "config_large.sst").string();
  const std::string wide_filter_path =
      (dir / "config_wide_filter.sst").string();
  std::error_code ec;
  std::filesystem::remove(small_path, ec);
  std::filesystem::remove(large_path, ec);
  std::filesystem::remove(wide_filter_path, ec);

  const auto add_entries = [](TableBuilder* builder) {
    for (int i = 0; i < 20; ++i) {
      const std::string key = std::string("key") +
                              (i < 10 ? "0" : "") + std::to_string(i);
      ASSERT_TRUE(builder->Add(key, static_cast<uint64_t>(i + 1), 0,
                               std::string(80, 'v')).ok());
    }
  };

  TableBuilder small_builder(small_path, 256, 8);
  add_entries(&small_builder);
  ASSERT_TRUE(small_builder.Finish().ok());

  TableBuilder large_builder(large_path, 4096, 8);
  add_entries(&large_builder);
  ASSERT_TRUE(large_builder.Finish().ok());

  TableBuilder wide_filter_builder(wide_filter_path, 4096, 32);
  add_entries(&wide_filter_builder);
  ASSERT_TRUE(wide_filter_builder.Finish().ok());

  std::unique_ptr<TableReader> small_reader;
  std::unique_ptr<TableReader> large_reader;
  ASSERT_TRUE(TableReader::Open(small_path, &small_reader).ok());
  ASSERT_TRUE(TableReader::Open(large_path, &large_reader).ok());
  EXPECT_GT(small_reader->NumDataBlocks(), large_reader->NumDataBlocks());

  const Footer narrow_footer = ReadFooter(large_path);
  const Footer wide_footer = ReadFooter(wide_filter_path);
  EXPECT_GT(wide_footer.filter_handle.size, narrow_footer.filter_handle.size);

  std::filesystem::remove(small_path, ec);
  std::filesystem::remove(large_path, ec);
  std::filesystem::remove(wide_filter_path, ec);
}

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

TEST(TableReaderTest, DetectsCorruptedDataBlockChecksum) {
  const std::filesystem::path dir("test_tmp/sstable");
  std::filesystem::create_directories(dir);
  const std::string file_path = (dir / "corrupt_data_block.sst").string();

  std::error_code ec;
  std::filesystem::remove(file_path, ec);

  {
    TableBuilder builder(file_path, 256);
    ASSERT_TRUE(builder.Add("alpha", 1, 0, "one").ok());
    ASSERT_TRUE(builder.Add("bravo", 2, 0, "two").ok());
    ASSERT_TRUE(builder.Finish().ok());
  }

  std::unique_ptr<TableReader> reader;
  ASSERT_TRUE(TableReader::Open(file_path, &reader).ok());
  ASSERT_FALSE(reader->index().empty());
  const uint64_t block_offset = reader->index().front().block_offset;
  reader.reset();

  {
    std::fstream file(file_path, std::ios::binary | std::ios::in | std::ios::out);
    ASSERT_TRUE(file.is_open());
    file.seekp(static_cast<std::streamoff>(block_offset));
    char byte = 0;
    file.read(&byte, 1);
    ASSERT_TRUE(file.good());
    file.seekp(static_cast<std::streamoff>(block_offset));
    byte ^= 0x01;
    file.write(&byte, 1);
    ASSERT_TRUE(file.good());
  }

  ASSERT_TRUE(TableReader::Open(file_path, &reader).ok());
  uint8_t type = 0;
  std::string value;
  Status s = reader->Get("alpha", 1, &type, &value);
  EXPECT_TRUE(s.IsCorruption()) << s.ToString();

  std::filesystem::remove(file_path, ec);
}

TEST(TableReaderTest, ReadsLegacyTableWithoutDataBlockChecksums) {
  const std::filesystem::path dir("test_tmp/sstable");
  std::filesystem::create_directories(dir);
  const std::string file_path = (dir / "legacy_no_checksums.sst").string();

  std::error_code ec;
  std::filesystem::remove(file_path, ec);

  uint64_t offset = 0;
  uint64_t max_seq = 0;
  std::vector<TableReader::IndexEntry> index_entries;
  FilterBlockBuilder filter_builder;

  std::ofstream file(file_path, std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(file.is_open());

  BlockBuilder data_block;
  data_block.Add("alpha", 1, 0, "one");
  data_block.Add("bravo", 2, 0, "two");
  filter_builder.AddKey("alpha");
  filter_builder.AddKey("bravo");
  max_seq = 2;

  std::string block_data = data_block.Finish();
  file.write(block_data.data(), static_cast<std::streamsize>(block_data.size()));
  ASSERT_TRUE(file.good());
  index_entries.push_back({"bravo", offset, block_data.size()});
  offset += block_data.size();

  const uint64_t filter_offset = offset;
  std::string filter_data = filter_builder.Finish();
  file.write(filter_data.data(), static_cast<std::streamsize>(filter_data.size()));
  ASSERT_TRUE(file.good());
  offset += filter_data.size();

  BlockBuilder index_block;
  for (const auto& entry : index_entries) {
    std::string handle;
    EncodeFixed64(&handle, entry.block_offset);
    EncodeFixed64(&handle, entry.block_size);
    index_block.Add(entry.last_key, 0, 0, handle);
  }

  const uint64_t index_offset = offset;
  std::string index_data = index_block.Finish();
  file.write(index_data.data(), static_cast<std::streamsize>(index_data.size()));
  ASSERT_TRUE(file.good());

  Footer footer;
  footer.format_version = 0;
  footer.index_handle.offset = index_offset;
  footer.index_handle.size = index_data.size();
  footer.filter_handle.offset = filter_offset;
  footer.filter_handle.size = filter_data.size();
  footer.max_seq = max_seq;
  std::string footer_data = footer.Encode();
  file.write(footer_data.data(), static_cast<std::streamsize>(footer_data.size()));
  ASSERT_TRUE(file.good());
  file.close();

  std::unique_ptr<TableReader> reader;
  ASSERT_TRUE(TableReader::Open(file_path, &reader).ok());

  uint8_t type = 0;
  std::string value;
  ASSERT_TRUE(reader->Get("alpha", 1, &type, &value).ok());
  EXPECT_EQ(type, 0);
  EXPECT_EQ(value, "one");

  std::filesystem::remove(file_path, ec);
}

}  // namespace
}  // namespace kv
