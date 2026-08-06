#include "kv/wal/log_record.h"
#include "kv/wal/wal_reader.h"
#include "kv/wal/wal_writer.h"

#include "gtest/gtest.h"
#include <cstdio>
#include <fstream>
#include <sstream>

namespace kv {
namespace {

std::string MakeTestPath(const std::string& name) {
  static int counter = 0;
  std::ostringstream oss;
  oss << "test_tmp/wal/reader_" << name << "_" << ++counter << ".log";
  return oss.str();
}

TEST(WALReaderTest, ReadsRecordsAndTracksOffset) {
  const std::string path = MakeTestPath("records");
  std::remove(path.c_str());

  {
    WALWriter writer;
    ASSERT_TRUE(writer.Open(path, false).ok());
    ASSERT_TRUE(writer.AppendPut(1, "key", "value").ok());
    ASSERT_TRUE(writer.AppendDelete(2, "key").ok());
    ASSERT_TRUE(writer.Close().ok());
  }

  WALReader reader;
  ASSERT_TRUE(reader.Open(path).ok());
  LogRecord record;
  ASSERT_TRUE(reader.ReadNext(&record).ok());
  EXPECT_EQ(record.type, LogRecordType::kPut);
  EXPECT_EQ(record.key, "key");
  EXPECT_EQ(record.value, "value");
  EXPECT_GT(reader.offset(), 0U);

  const uint64_t first_offset = reader.offset();
  ASSERT_TRUE(reader.ReadNext(&record).ok());
  EXPECT_EQ(record.type, LogRecordType::kDelete);
  EXPECT_GT(reader.offset(), first_offset);
  EXPECT_TRUE(reader.ReadNext(&record).IsNotFound());
  ASSERT_TRUE(reader.Close().ok());
}

TEST(WALReaderTest, RejectsOversizedPayloadHeaderBeforeAllocation) {
  const std::string path = MakeTestPath("oversized");
  std::remove(path.c_str());

  std::string header(LogRecordCodec::kHeaderSize, '\0');
  header[4] = static_cast<char>(LogRecordType::kPut);
  auto put32 = [&header](size_t offset, uint32_t value) {
    for (size_t i = 0; i < sizeof(uint32_t); ++i) {
      header[offset + i] = static_cast<char>((value >> (8 * i)) & 0xFF);
    }
  };
  put32(13, static_cast<uint32_t>(LogRecordCodec::kMaxPayloadSize));
  put32(17, 1);

  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  ASSERT_TRUE(out.is_open());
  out.write(header.data(), static_cast<std::streamsize>(header.size()));
  out.close();

  WALReader reader;
  ASSERT_TRUE(reader.Open(path).ok());
  LogRecord record;
  Status s = reader.ReadNext(&record);
  EXPECT_TRUE(s.IsCorruption()) << s.ToString();
}

} // namespace
} // namespace kv
