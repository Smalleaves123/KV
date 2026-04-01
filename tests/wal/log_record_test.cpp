#include "kv/wal/log_record.h"

#include <string>

#include "gtest/gtest.h"

namespace kv {
namespace {

TEST(LogRecordCodecTest, EncodeAndDecodePutRecord) {
  LogRecord input;
  input.type = LogRecordType::kPut;
  input.seq = 123;
  input.key = "name";
  input.value = "tdmpc2";

  std::string encoded;
  Status s = LogRecordCodec::Encode(input, &encoded);
  ASSERT_TRUE(s.ok());
  ASSERT_FALSE(encoded.empty());

  LogRecord output;
  size_t bytes_consumed = 0;
  s = LogRecordCodec::Decode(encoded, &output, &bytes_consumed);

  ASSERT_TRUE(s.ok());
  EXPECT_EQ(bytes_consumed, encoded.size());
  EXPECT_EQ(output.type, LogRecordType::kPut);
  EXPECT_EQ(output.seq, 123U);
  EXPECT_EQ(output.key, "name");
  EXPECT_EQ(output.value, "tdmpc2");
}

TEST(LogRecordCodecTest, EncodeAndDecodeDeleteRecord) {
  LogRecord input;
  input.type = LogRecordType::kDelete;
  input.seq = 7;
  input.key = "k1";
  input.value.clear();

  std::string encoded;
  Status s = LogRecordCodec::Encode(input, &encoded);
  ASSERT_TRUE(s.ok());

  LogRecord output;
  s = LogRecordCodec::Decode(encoded, &output);

  ASSERT_TRUE(s.ok());
  EXPECT_EQ(output.type, LogRecordType::kDelete);
  EXPECT_EQ(output.seq, 7U);
  EXPECT_EQ(output.key, "k1");
  EXPECT_TRUE(output.value.empty());
}

TEST(LogRecordCodecTest, RejectZeroSequence) {
  LogRecord input;
  input.type = LogRecordType::kPut;
  input.seq = 0;
  input.key = "a";
  input.value = "b";

  std::string encoded;
  Status s = LogRecordCodec::Encode(input, &encoded);

  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST(LogRecordCodecTest, RejectDeleteRecordWithValue) {
  LogRecord input;
  input.type = LogRecordType::kDelete;
  input.seq = 10;
  input.key = "a";
  input.value = "should_not_exist";

  std::string encoded;
  Status s = LogRecordCodec::Encode(input, &encoded);

  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST(LogRecordCodecTest, DetectChecksumCorruption) {
  LogRecord input;
  input.type = LogRecordType::kPut;
  input.seq = 11;
  input.key = "abc";
  input.value = "xyz";

  std::string encoded;
  Status s = LogRecordCodec::Encode(input, &encoded);
  ASSERT_TRUE(s.ok());
  ASSERT_GT(encoded.size(), LogRecordCodec::kHeaderSize);

  encoded.back() ^= 0x01;

  LogRecord output;
  s = LogRecordCodec::Decode(encoded, &output);

  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsCorruption());
}

TEST(LogRecordCodecTest, DetectTruncatedBuffer) {
  LogRecord input;
  input.type = LogRecordType::kPut;
  input.seq = 22;
  input.key = "hello";
  input.value = "world";

  std::string encoded;
  Status s = LogRecordCodec::Encode(input, &encoded);
  ASSERT_TRUE(s.ok());
  ASSERT_GT(encoded.size(), 3U);

  encoded.resize(encoded.size() - 3);

  LogRecord output;
  s = LogRecordCodec::Decode(encoded, &output);

  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsCorruption());
}

TEST(LogRecordCodecTest, RejectUnknownRecordType) {
  LogRecord input;
  input.type = LogRecordType::kPut;
  input.seq = 33;
  input.key = "k";
  input.value = "v";

  std::string encoded;
  Status s = LogRecordCodec::Encode(input, &encoded);
  ASSERT_TRUE(s.ok());

  // checksum 后面的第一个字节是 type
  encoded[4] = static_cast<char>(99);

  LogRecord output;
  s = LogRecordCodec::Decode(encoded, &output);

  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsCorruption());
}

TEST(LogRecordCodecTest, NullOutputBufferIsRejected) {
  LogRecord input;
  input.type = LogRecordType::kPut;
  input.seq = 1;
  input.key = "a";
  input.value = "b";

  Status s = LogRecordCodec::Encode(input, nullptr);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

TEST(LogRecordCodecTest, NullRecordOutputIsRejected) {
  LogRecord input;
  input.type = LogRecordType::kPut;
  input.seq = 1;
  input.key = "a";
  input.value = "b";

  std::string encoded;
  Status s = LogRecordCodec::Encode(input, &encoded);
  ASSERT_TRUE(s.ok());

  s = LogRecordCodec::Decode(encoded, nullptr);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsInvalidArgument());
}

}  // namespace
}  // namespace kv