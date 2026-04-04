#include <gtest/gtest.h>

#include <string>

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

}  // namespace
}  // namespace kv::net
