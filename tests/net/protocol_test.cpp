#include "kv/net/protocol.h"

#include <vector>

#include "gtest/gtest.h"

namespace kv::net::protocol {
namespace {

TEST(ProtocolTest, SimpleStringEncoding) {
  EXPECT_EQ(SimpleString("OK"), "+OK\r\n");
}

TEST(ProtocolTest, ErrorEncoding) {
  EXPECT_EQ(Error("bad"), "-ERRbad\r\n");
}

TEST(ProtocolTest, MovedEncoding) {
  EXPECT_EQ(Moved("127.0.0.1:9527"), "-MOVED 127.0.0.1:9527\r\n");
}

TEST(ProtocolTest, BulkStringEncoding) {
  EXPECT_EQ(BulkString("abc"), "$3\r\nabc\r\n");
}

TEST(ProtocolTest, NilEncoding) {
  EXPECT_EQ(Nil(), "$-1\r\n");
}

TEST(ProtocolTest, ArrayEncoding) {
  std::vector<std::string> items{
      BulkString("v1"),
      Nil(),
      SimpleString("OK"),
  };

  EXPECT_EQ(Array(items), "*3\r\n$2\r\nv1\r\n$-1\r\n+OK\r\n");
}

}  // namespace
}  // namespace kv::net::protocol
