#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>
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

TEST_F(ConnectionTest, ReadRequestBuffersPartialRespFrame) {
  Connection conn(pair_.first);
  pair_.first = platform::kInvalidSocket;

  std::string first = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$6\r\na";
  first.append("\0b", 2);
  const std::string second = "\r\nc\r\n";
  ASSERT_EQ(platform::SendSocket(pair_.second, first.data(), first.size(), 0),
            static_cast<platform::SocketIoResult>(first.size()));

  std::vector<std::string> tokens;
  Status status = Status::IOError("reader did not run");
  std::thread reader([&]() { status = conn.ReadRequest(&tokens); });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));

  const platform::SocketIoResult sent =
      platform::SendSocket(pair_.second, second.data(), second.size(), 0);
  reader.join();

  EXPECT_EQ(sent, static_cast<platform::SocketIoResult>(second.size()));
  EXPECT_TRUE(status.ok()) << status.ToString();
  ASSERT_EQ(tokens.size(), 3U);
  EXPECT_EQ(tokens[2], std::string("a\0b\r\nc", 6));
}

TEST_F(ConnectionTest, ReadRequestPreservesPipelinedFrame) {
  Connection conn(pair_.first);
  pair_.first = platform::kInvalidSocket;

  const std::string requests =
      "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
  ASSERT_EQ(platform::SendSocket(pair_.second, requests.data(), requests.size(),
                                 0),
            static_cast<platform::SocketIoResult>(requests.size()));

  std::vector<std::string> tokens;
  Status status = conn.ReadRequest(&tokens);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_EQ(tokens.size(), 1U);
  EXPECT_EQ(tokens[0], "PING");

  status = conn.ReadRequest(&tokens);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_EQ(tokens.size(), 2U);
  EXPECT_EQ(tokens[0], "GET");
  EXPECT_EQ(tokens[1], "k");
}

}  // namespace
}  // namespace kv::net
