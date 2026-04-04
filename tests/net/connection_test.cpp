#include <gtest/gtest.h>

#include <string>

#include <sys/socket.h>
#include <unistd.h>

#include "kv/net/connection.h"

namespace kv::net {
namespace {

class ConnectionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds_), 0);
  }

  void TearDown() override {
    if (fds_[0] >= 0) {
      (void)::close(fds_[0]);
      fds_[0] = -1;
    }
    if (fds_[1] >= 0) {
      (void)::close(fds_[1]);
      fds_[1] = -1;
    }
  }

  int fds_[2] = {-1, -1};
};

TEST_F(ConnectionTest, ReadLineWorks) {
  Connection conn(fds_[0]);
  fds_[0] = -1;

  const std::string req = "PING\r\n";
  ASSERT_EQ(::write(fds_[1], req.data(), req.size()), static_cast<ssize_t>(req.size()));

  std::string line;
  Status s = conn.ReadLine(&line);
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(line, "PING");
}

TEST_F(ConnectionTest, WriteAllWorks) {
  Connection conn(fds_[0]);
  fds_[0] = -1;

  const std::string resp = "+PONG\r\n";
  Status s = conn.WriteAll(resp);
  EXPECT_TRUE(s.ok()) << s.ToString();

  char buf[64] = {0};
  const ssize_t n = ::read(fds_[1], buf, sizeof(buf));
  ASSERT_GT(n, 0);
  EXPECT_EQ(std::string(buf, static_cast<size_t>(n)), resp);
}

TEST_F(ConnectionTest, ReadLinePeerClosedReturnsNotFound) {
  Connection conn(fds_[0]);
  fds_[0] = -1;

  (void)::close(fds_[1]);
  fds_[1] = -1;

  std::string line;
  Status s = conn.ReadLine(&line);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

}  // namespace
}  // namespace kv::net
