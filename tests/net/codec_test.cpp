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

  ASSERT_TRUE(RequestCodec::TryDecode(&buffer, &tokens, &error));
  EXPECT_TRUE(error.empty());
  ASSERT_EQ(tokens.size(), 1U);
  EXPECT_EQ(tokens[0], "PING");
  EXPECT_TRUE(buffer.empty());
}

TEST(RequestCodecTest, DecodesLineRequestIntoTokens) {
  std::string buffer = "SET k v\r\n";
  std::vector<std::string> tokens;
  std::string error;

  ASSERT_TRUE(RequestCodec::TryDecode(&buffer, &tokens, &error));
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

  ASSERT_TRUE(RequestCodec::TryDecode(&buffer, &tokens, &error));
  EXPECT_TRUE(error.empty());
  ASSERT_EQ(tokens.size(), 3U);
  EXPECT_EQ(tokens[0], "SET");
  EXPECT_EQ(tokens[1], "k");
  EXPECT_EQ(tokens[2], "hello world!");
  EXPECT_TRUE(buffer.empty());
}

TEST(RequestCodecTest, IncompleteRespArrayReturnsFalse) {
  std::string buffer = "*2\r\n$3\r\nGET\r\n$1\r\n";
  std::vector<std::string> tokens;
  std::string error;

  EXPECT_FALSE(RequestCodec::TryDecode(&buffer, &tokens, &error));
  EXPECT_TRUE(error.empty());
}

}  // namespace
}  // namespace kv::net
