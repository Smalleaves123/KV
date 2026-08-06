#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "kv/net/codec.h"

namespace kv::net {
namespace {

TEST(LineCodecTest, EncodeLineAddsCRLF) {
  EXPECT_EQ(LineCodec::EncodeLine("PING"), "PING\r\n");
}

TEST(LineCodecTest, DecodeLineFromBuffer) {
  std::string buffer = "SET k v\r\nGET k\n";
  std::string line;

  EXPECT_TRUE(LineCodec::TryDecodeLine(&buffer, &line));
  EXPECT_EQ(line, "SET k v");

  EXPECT_TRUE(LineCodec::TryDecodeLine(&buffer, &line));
  EXPECT_EQ(line, "GET k");

  EXPECT_TRUE(buffer.empty());
}

TEST(LineCodecTest, DecodeIncompleteBufferReturnsFalse) {
  std::string buffer = "SET k";
  std::string line;

  EXPECT_FALSE(LineCodec::TryDecodeLine(&buffer, &line));
  EXPECT_EQ(buffer, "SET k");
}

TEST(RequestCodecTest, DecodesLineRequest) {
  std::string buffer = "PING\r\n";
  std::vector<std::string> tokens;
  std::string error;

  ASSERT_EQ(RequestCodec::TryDecode(&buffer, &tokens, &error),
            RequestDecodeResult::kOk);
  EXPECT_TRUE(error.empty());
  ASSERT_EQ(tokens.size(), 1U);
  EXPECT_EQ(tokens[0], "PING");
  EXPECT_TRUE(buffer.empty());
}

TEST(RequestCodecTest, DecodesLineRequestIntoTokens) {
  std::string buffer = "SET k v\r\n";
  std::vector<std::string> tokens;
  std::string error;

  ASSERT_EQ(RequestCodec::TryDecode(&buffer, &tokens, &error),
            RequestDecodeResult::kOk);
  EXPECT_TRUE(error.empty());
  ASSERT_EQ(tokens.size(), 3U);
  EXPECT_EQ(tokens[0], "SET");
  EXPECT_EQ(tokens[1], "k");
  EXPECT_EQ(tokens[2], "v");
}

TEST(RequestCodecTest, DecodesRespArrayRequest) {
  std::string buffer = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$12\r\nhello world!\r\n";
  std::vector<std::string> tokens;
  std::string error;

  ASSERT_EQ(RequestCodec::TryDecode(&buffer, &tokens, &error),
            RequestDecodeResult::kOk);
  EXPECT_TRUE(error.empty());
  ASSERT_EQ(tokens.size(), 3U);
  EXPECT_EQ(tokens[0], "SET");
  EXPECT_EQ(tokens[1], "k");
  EXPECT_EQ(tokens[2], "hello world!");
  EXPECT_TRUE(buffer.empty());
}

TEST(RequestCodecTest, IncompleteRespArrayReturnsNeedMoreWithoutConsumption) {
  std::string buffer = "*2\r\n$3\r\nGET\r\n$1\r\n";
  const std::string original = buffer;
  std::vector<std::string> tokens;
  std::string error;

  EXPECT_EQ(RequestCodec::TryDecode(&buffer, &tokens, &error),
            RequestDecodeResult::kNeedMore);
  EXPECT_TRUE(error.empty());
  EXPECT_EQ(buffer, original);
}

TEST(RequestCodecTest, DecodesBulkValueContainingCRLFAndNullBytes) {
  const std::string value("a\0b\r\nc", 6);
  std::string buffer = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$6\r\n";
  buffer.append(value);
  buffer.append("\r\n");
  std::vector<std::string> tokens;
  std::string error;

  ASSERT_EQ(RequestCodec::TryDecode(&buffer, &tokens, &error),
            RequestDecodeResult::kOk);
  EXPECT_TRUE(error.empty());
  ASSERT_EQ(tokens.size(), 3U);
  EXPECT_EQ(tokens[2], value);
  EXPECT_TRUE(buffer.empty());
}

TEST(RequestCodecTest, DecodesPipelinedRequestsOneAtATime) {
  std::string buffer = "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
  std::vector<std::string> tokens;
  std::string error;

  ASSERT_EQ(RequestCodec::TryDecode(&buffer, &tokens, &error),
            RequestDecodeResult::kOk);
  ASSERT_EQ(tokens.size(), 1U);
  EXPECT_EQ(tokens[0], "PING");

  ASSERT_EQ(RequestCodec::TryDecode(&buffer, &tokens, &error),
            RequestDecodeResult::kOk);
  ASSERT_EQ(tokens.size(), 2U);
  EXPECT_EQ(tokens[0], "GET");
  EXPECT_EQ(tokens[1], "k");
  EXPECT_TRUE(buffer.empty());
}

TEST(RequestCodecTest, MalformedFramesReturnErrorWithoutConsumption) {
  const std::vector<std::string> malformed = {
      "*x\r\n", "*1\r\n$-2\r\n", "*1\r\n+4\r\nPING\r\n",
      "*1\r\n$4\r\nPINGxx",
  };

  for (const std::string& frame : malformed) {
    std::string buffer = frame;
    std::vector<std::string> tokens;
    std::string error;

    EXPECT_EQ(RequestCodec::TryDecode(&buffer, &tokens, &error),
              RequestDecodeResult::kError);
    EXPECT_FALSE(error.empty());
    EXPECT_EQ(buffer, frame);
    EXPECT_TRUE(tokens.empty());
  }
}

TEST(RequestCodecTest, RejectsOversizedAndOverlongRequests) {
  const std::vector<std::string> oversized = {
      "*" + std::to_string(RequestCodec::kMaxArguments + 1) + "\r\n",
      "*1\r\n$" +
          std::to_string(RequestCodec::kMaxBulkStringBytes + 1) + "\r\n",
      std::string(RequestCodec::kMaxLineBytes + 1, 'x')};

  for (const std::string& frame : oversized) {
    std::string buffer = frame;
    std::vector<std::string> tokens;
    std::string error;
    EXPECT_EQ(RequestCodec::TryDecode(&buffer, &tokens, &error),
              RequestDecodeResult::kError);
    EXPECT_FALSE(error.empty());
    EXPECT_TRUE(tokens.empty());
  }
}

}  // namespace
}  // namespace kv::net
