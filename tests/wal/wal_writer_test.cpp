#include "kv/wal/wal_writer.h"

#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace kv {
namespace {

std::string ReadWholeFile(const std::string& file_path) {
  std::ifstream in(file_path, std::ios::binary);
  if (!in.is_open()) {
    return {};
  }

  std::ostringstream oss;
  oss << in.rdbuf();
  return oss.str();
}

std::vector<LogRecord> DecodeAllRecords(const std::string& content) {
  std::vector<LogRecord> records;

  size_t offset = 0;
  while (offset < content.size()) {
    LogRecord record;
    size_t consumed = 0;
    Status s = LogRecordCodec::Decode(
        Slice(content.data() + offset, content.size() - offset),
        &record,
        &consumed);

    EXPECT_TRUE(s.ok());
    EXPECT_GT(consumed, 0U);

    if (!s.ok() || consumed == 0) {
      break;
    }

    records.push_back(record);
    offset += consumed;
  }

  return records;
}

std::string MakeTestPath(const std::string& file_name) {
  static int counter = 0;
  ++counter;

  std::ostringstream oss;
  oss << "test_tmp/wal/" << file_name << "_" << counter << ".log";
  return oss.str();
}

void RemoveFileIfExists(const std::string& file_path) {
  std::remove(file_path.c_str());
}

TEST(WALWriterTest, OpenWithEmptyPathIsRejected) {
  WALWriter writer;
  Status s = writer.Open("");
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST(WALWriterTest, AppendWithoutOpenFails) {
  WALWriter writer;

  Status s = writer.AppendPut(1, "a", "b");
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());
}

TEST(WALWriterTest, OpenAppendSyncCloseWorks) {
  const std::string path = MakeTestPath("open_append_sync_close");
  RemoveFileIfExists(path);

  WALWriter writer;
  Status s = writer.Open(path, false);
  ASSERT_TRUE(s.ok());
  EXPECT_TRUE(writer.IsOpen());
  EXPECT_EQ(writer.file_path(), path);
  EXPECT_EQ(writer.file_size(), 0U);

  s = writer.AppendPut(1, "name", "td");
  ASSERT_TRUE(s.ok());

  s = writer.AppendDelete(2, "old_key");
  ASSERT_TRUE(s.ok());

  EXPECT_GT(writer.file_size(), 0U);

  s = writer.Sync();
  ASSERT_TRUE(s.ok());

  s = writer.Close();
  ASSERT_TRUE(s.ok());
  EXPECT_FALSE(writer.IsOpen());

  const std::string content = ReadWholeFile(path);
  ASSERT_FALSE(content.empty());

  auto records = DecodeAllRecords(content);
  ASSERT_EQ(records.size(), 2U);

  EXPECT_EQ(records[0].type, LogRecordType::kPut);
  EXPECT_EQ(records[0].seq, 1U);
  EXPECT_EQ(records[0].key, "name");
  EXPECT_EQ(records[0].value, "td");

  EXPECT_EQ(records[1].type, LogRecordType::kDelete);
  EXPECT_EQ(records[1].seq, 2U);
  EXPECT_EQ(records[1].key, "old_key");
  EXPECT_TRUE(records[1].value.empty());
}

TEST(WALWriterTest, AppendModeKeepsExistingContent) {
  const std::string path = MakeTestPath("append_mode");
  RemoveFileIfExists(path);

  {
    WALWriter writer;
    Status s = writer.Open(path, false);
    ASSERT_TRUE(s.ok());

    s = writer.AppendPut(1, "k1", "v1");
    ASSERT_TRUE(s.ok());

    s = writer.Close();
    ASSERT_TRUE(s.ok());
  }

  const std::string first_content = ReadWholeFile(path);
  ASSERT_FALSE(first_content.empty());

  {
    WALWriter writer;
    Status s = writer.Open(path, true);
    ASSERT_TRUE(s.ok());
    EXPECT_EQ(writer.file_size(),
              static_cast<uint64_t>(first_content.size()));

    s = writer.AppendPut(2, "k2", "v2");
    ASSERT_TRUE(s.ok());

    s = writer.Close();
    ASSERT_TRUE(s.ok());
  }

  const std::string second_content = ReadWholeFile(path);
  ASSERT_GT(second_content.size(), first_content.size());

  auto records = DecodeAllRecords(second_content);
  ASSERT_EQ(records.size(), 2U);

  EXPECT_EQ(records[0].seq, 1U);
  EXPECT_EQ(records[0].key, "k1");
  EXPECT_EQ(records[0].value, "v1");

  EXPECT_EQ(records[1].seq, 2U);
  EXPECT_EQ(records[1].key, "k2");
  EXPECT_EQ(records[1].value, "v2");
}

TEST(WALWriterTest, TruncateModeReplacesOldContent) {
  const std::string path = MakeTestPath("truncate_mode");
  RemoveFileIfExists(path);

  {
    WALWriter writer;
    Status s = writer.Open(path, false);
    ASSERT_TRUE(s.ok());

    s = writer.AppendPut(1, "old", "value");
    ASSERT_TRUE(s.ok());

    s = writer.Close();
    ASSERT_TRUE(s.ok());
  }

  {
    WALWriter writer;
    Status s = writer.Open(path, false);
    ASSERT_TRUE(s.ok());
    EXPECT_EQ(writer.file_size(), 0U);

    s = writer.AppendPut(2, "new", "value2");
    ASSERT_TRUE(s.ok());

    s = writer.Close();
    ASSERT_TRUE(s.ok());
  }

  const std::string content = ReadWholeFile(path);
  auto records = DecodeAllRecords(content);

  ASSERT_EQ(records.size(), 1U);
  EXPECT_EQ(records[0].seq, 2U);
  EXPECT_EQ(records[0].key, "new");
  EXPECT_EQ(records[0].value, "value2");
}

TEST(WALWriterTest, CloseWithoutOpenIsOk) {
  WALWriter writer;
  Status s = writer.Close();
  EXPECT_TRUE(s.ok());
}

TEST(WALWriterTest, SyncWithoutOpenFails) {
  WALWriter writer;
  Status s = writer.Sync();
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsIOError());
}

TEST(WALWriterTest, EncodedBytesMatchFileSizeGrowth) {
  const std::string path = MakeTestPath("file_size_growth");
  RemoveFileIfExists(path);

  LogRecord r1;
  r1.type = LogRecordType::kPut;
  r1.seq = 1;
  r1.key = "a";
  r1.value = "1";

  LogRecord r2;
  r2.type = LogRecordType::kDelete;
  r2.seq = 2;
  r2.key = "b";

  std::string e1;
  std::string e2;
  ASSERT_TRUE(LogRecordCodec::Encode(r1, &e1).ok());
  ASSERT_TRUE(LogRecordCodec::Encode(r2, &e2).ok());

  WALWriter writer;
  Status s = writer.Open(path, false);
  ASSERT_TRUE(s.ok());

  EXPECT_EQ(writer.file_size(), 0U);

  s = writer.Append(r1);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(writer.file_size(), static_cast<uint64_t>(e1.size()));

  s = writer.Append(r2);
  ASSERT_TRUE(s.ok());
  EXPECT_EQ(writer.file_size(),
            static_cast<uint64_t>(e1.size() + e2.size()));

  s = writer.Close();
  ASSERT_TRUE(s.ok());
}

}  // namespace
}  // namespace kv