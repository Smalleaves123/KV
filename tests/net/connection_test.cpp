#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "kv/common/socket_compat.h"
#include "kv/net/connection.h"
#include "test_socket_pair.h"

namespace kv::net {
namespace {

class ConnectionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_TRUE(test::CreateSocketPair(&pair_));
  }

  void TearDown() override {
    test::CloseSocketPair(&pair_);
  }

  test::SocketPair pair_;
};

TEST_F(ConnectionTest, ReadLineWorks) {
  Connection conn(pair_.first);
  pair_.first = platform::kInvalidSocket;

  const std::string req = "PING\r\n";
  ASSERT_EQ(platform::SendSocket(pair_.second, req.data(), req.size(), 0),
            static_cast<platform::SocketIoResult>(req.size()));

  std::string line;
  Status s = conn.ReadLine(&line);
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(line, "PING");
}

TEST_F(ConnectionTest, WriteAllWorks) {
  Connection conn(pair_.first);
  pair_.first = platform::kInvalidSocket;

  const std::string resp = "+PONG\r\n";
  Status s = conn.WriteAll(resp);
  EXPECT_TRUE(s.ok()) << s.ToString();

  char buf[64] = {0};
  const platform::SocketIoResult n =
      platform::ReceiveSocket(pair_.second, buf, sizeof(buf), 0);
  ASSERT_GT(n, 0);
  EXPECT_EQ(std::string(buf, static_cast<size_t>(n)), resp);
}

TEST_F(ConnectionTest, ReadLinePeerClosedReturnsNotFound) {
  Connection conn(pair_.first);
  pair_.first = platform::kInvalidSocket;

  (void)platform::CloseSocket(pair_.second);
  pair_.second = platform::kInvalidSocket;

  std::string line;
  Status s = conn.ReadLine(&line);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(ConnectionTest, ReadRequestDecodesRespArray) {
  Connection conn(pair_.first);
  pair_.first = platform::kInvalidSocket;

  const std::string req = "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
  ASSERT_EQ(platform::SendSocket(pair_.second, req.data(), req.size(), 0),
            static_cast<platform::SocketIoResult>(req.size()));

  std::vector<std::string> tokens;
  Status s = conn.ReadRequest(&tokens);
  EXPECT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(tokens.size(), 2U);
  EXPECT_EQ(tokens[0], "GET");
  EXPECT_EQ(tokens[1], "k");
}

}  // namespace
}  // namespace kv::net
