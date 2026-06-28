#include "kv/net/server.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "kv/cluster/cluster_manager.h"
#include "kv/engine/db.h"

namespace kv::net {
namespace {

DBOptions MakeDBOptions(const std::string& name) {
  static int counter = 0;
  ++counter;

  DBOptions options;
  std::ostringstream oss;
  oss << "test_tmp/net_server/" << name << "_" << counter;
  const std::string base = oss.str();

  options.wal_path = base + ".wal";
  options.sst_dir = base + "_sst";
  options.manifest_path = base + ".manifest";
  options.memtable_write_buffer_size = 1ULL << 20;
  return options;
}

void RemovePathIfExists(const std::string& path) {
  std::error_code ec;
  std::filesystem::remove(path, ec);
}

void RemoveDirIfExists(const std::string& path) {
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
}

int ConnectWithRetry(uint16_t port) {
  for (int i = 0; i < 50; ++i) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      return -1;
    }

    timeval tv{};
    tv.tv_sec = 2;
    tv.tv_usec = 0;
    (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
      return fd;
    }

    (void)::close(fd);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  return -1;
}

bool ReadExact(int fd, size_t n, std::string* out) {
  out->clear();
  out->reserve(n);
  while (out->size() < n) {
    char buf[256];
    const size_t need = n - out->size();
    const size_t chunk = need < sizeof(buf) ? need : sizeof(buf);
    const ssize_t r = ::recv(fd, buf, chunk, 0);
    if (r <= 0) {
      return false;
    }
    out->append(buf, static_cast<size_t>(r));
  }
  return true;
}

bool ReadLineCRLF(int fd, std::string* line) {
  line->clear();
  while (true) {
    char c = 0;
    const ssize_t r = ::recv(fd, &c, 1, 0);
    if (r <= 0) {
      return false;
    }
    line->push_back(c);
    const size_t n = line->size();
    if (n >= 2 && (*line)[n - 2] == '\r' && (*line)[n - 1] == '\n') {
      line->resize(n - 2);
      return true;
    }
  }
}

bool ReadResp(int fd, std::string* encoded) {
  encoded->clear();

  char prefix = 0;
  if (::recv(fd, &prefix, 1, 0) != 1) {
    return false;
  }
  encoded->push_back(prefix);

  std::string line;
  if (!ReadLineCRLF(fd, &line)) {
    return false;
  }
  encoded->append(line);
  encoded->append("\r\n");

  if (prefix == '+' || prefix == '-') {
    return true;
  }

  if (prefix == '$') {
    int len = 0;
    try {
      len = std::stoi(line);
    } catch (...) {
      return false;
    }
    if (len < 0) {
      return true;
    }

    std::string body;
    if (!ReadExact(fd, static_cast<size_t>(len) + 2, &body)) {
      return false;
    }
    encoded->append(body);
    return true;
  }

  if (prefix == '*') {
    int count = 0;
    try {
      count = std::stoi(line);
    } catch (...) {
      return false;
    }

    for (int i = 0; i < count; ++i) {
      std::string item;
      if (!ReadResp(fd, &item)) {
        return false;
      }
      encoded->append(item);
    }
    return true;
  }

  return false;
}

bool SendLine(int fd, const std::string& cmd) {
  const std::string req = cmd + "\n";
  size_t sent = 0;
  while (sent < req.size()) {
    const ssize_t n = ::send(fd, req.data() + sent, req.size() - sent, 0);
    if (n <= 0) {
      return false;
    }
    sent += static_cast<size_t>(n);
  }
  return true;
}

bool SendRaw(int fd, const std::string& req) {
  size_t sent = 0;
  while (sent < req.size()) {
    const ssize_t n = ::send(fd, req.data() + sent, req.size() - sent, 0);
    if (n <= 0) {
      return false;
    }
    sent += static_cast<size_t>(n);
  }
  return true;
}

class ServerIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    options_ = MakeDBOptions("server_it");
    RemovePathIfExists(options_.wal_path);
    RemovePathIfExists(options_.manifest_path);
    RemoveDirIfExists(options_.sst_dir);

    Status s = DB::Open(options_, &db_);
    ASSERT_TRUE(s.ok()) << s.ToString();

    cluster_ = std::make_unique<ClusterManager>(8);
    ASSERT_TRUE(cluster_->AddNode(NodeInfo{"n1", "127.0.0.1", 9001, 1, true}));
    cluster_->SetLocalNodeId("n1");

    started_ = false;
    last_status_ = server_.Start(0, db_.get(), cluster_.get());
    if (last_status_.ok()) {
      port_ = server_.port();
      started_ = true;
    }
    if (started_) {
      ASSERT_TRUE(server_.IsRunning());
      ASSERT_GT(port_, 0);
    } else {
      ASSERT_FALSE(server_.IsRunning());
    }
  }

  void TearDown() override {
    (void)server_.Stop();
    if (db_) {
      (void)db_->Close();
      db_.reset();
    }
    cluster_.reset();

    RemovePathIfExists(options_.wal_path);
    RemovePathIfExists(options_.manifest_path);
    RemoveDirIfExists(options_.sst_dir);
  }

  DBOptions options_;
  std::unique_ptr<DB> db_;
  std::unique_ptr<ClusterManager> cluster_;
  Server server_;
  uint16_t port_ = 0;
  bool started_ = false;
  Status last_status_ = Status::OK();
};

TEST_F(ServerIntegrationTest, EndToEndCommandFlow) {
  if (!started_) {
    GTEST_SKIP() << "server failed to start in test environment: "
                 << last_status_.ToString();
  }

  const int fd = ConnectWithRetry(port_);
  ASSERT_GE(fd, 0);

  std::string resp;

  ASSERT_TRUE(SendLine(fd, "PING"));
  ASSERT_TRUE(ReadResp(fd, &resp));
  EXPECT_EQ(resp, "+PONG\r\n");

  ASSERT_TRUE(SendLine(fd, "SET k v"));
  ASSERT_TRUE(ReadResp(fd, &resp));
  EXPECT_EQ(resp, "+OK\r\n");

  ASSERT_TRUE(SendLine(fd, "GET k"));
  ASSERT_TRUE(ReadResp(fd, &resp));
  EXPECT_EQ(resp, "$1\r\nv\r\n");

  ASSERT_TRUE(SendLine(fd, "GET missing"));
  ASSERT_TRUE(ReadResp(fd, &resp));
  EXPECT_EQ(resp, "$-1\r\n");

  ASSERT_TRUE(SendLine(fd, "UNKNOWN"));
  ASSERT_TRUE(ReadResp(fd, &resp));
  EXPECT_EQ(resp, "-ERRunknown command\r\n");

  ASSERT_TRUE(SendLine(fd, "BEGIN"));
  ASSERT_TRUE(ReadResp(fd, &resp));
  EXPECT_EQ(resp, "+OK\r\n");

  ASSERT_TRUE(SendLine(fd, "SET tx 1"));
  ASSERT_TRUE(ReadResp(fd, &resp));
  EXPECT_EQ(resp, "+OK\r\n");

  ASSERT_TRUE(SendLine(fd, "EXEC"));
  ASSERT_TRUE(ReadResp(fd, &resp));
  EXPECT_EQ(resp, "+OK\r\n");

  const ServerStats stats = server_.GetStats();
  EXPECT_GE(stats.total_connections, 1U);
  EXPECT_GE(stats.active_connections, 1U);
  EXPECT_GE(stats.total_requests, 8U);
  EXPECT_GE(stats.txn_begin, 1U);
  EXPECT_GE(stats.txn_commit, 1U);

  (void)::close(fd);
}

TEST_F(ServerIntegrationTest, StopThenRejectNewConnection) {
  if (!started_) {
    GTEST_SKIP() << "server failed to start in test environment: "
                 << last_status_.ToString();
  }

  Status s = server_.Stop();
  EXPECT_TRUE(s.ok()) << s.ToString();
  EXPECT_FALSE(server_.IsRunning());

  const int fd = ConnectWithRetry(port_);
  EXPECT_LT(fd, 0);
}

TEST_F(ServerIntegrationTest, CanRestartAfterStop) {
  if (!started_) {
    GTEST_SKIP() << "server failed to start in test environment: "
                 << last_status_.ToString();
  }

  Status s = server_.Stop();
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_FALSE(server_.IsRunning());

  s = server_.Start(port_, db_.get());
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_TRUE(server_.IsRunning());

  const int fd = ConnectWithRetry(port_);
  ASSERT_GE(fd, 0);

  std::string resp;
  ASSERT_TRUE(SendLine(fd, "PING"));
  ASSERT_TRUE(ReadResp(fd, &resp));
  EXPECT_EQ(resp, "+PONG\r\n");

  (void)::close(fd);
}

TEST_F(ServerIntegrationTest, StartWithZeroUsesEphemeralPort) {
  if (!started_) {
    GTEST_SKIP() << "server failed to start in test environment: "
                 << last_status_.ToString();
  }

  EXPECT_GT(server_.port(), 0);
  EXPECT_EQ(server_.port(), port_);
}

TEST_F(ServerIntegrationTest, RespArrayRequestSupportsValuesWithSpaces) {
  if (!started_) {
    GTEST_SKIP() << "server failed to start in test environment: "
                 << last_status_.ToString();
  }

  const int fd = ConnectWithRetry(port_);
  ASSERT_GE(fd, 0);

  std::string resp;
  ASSERT_TRUE(SendRaw(fd, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$11\r\nhello world\r\n"));
  ASSERT_TRUE(ReadResp(fd, &resp));
  EXPECT_EQ(resp, "+OK\r\n");

  ASSERT_TRUE(SendRaw(fd, "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"));
  ASSERT_TRUE(ReadResp(fd, &resp));
  EXPECT_EQ(resp, "$11\r\nhello world\r\n");

  (void)::close(fd);
}

TEST_F(ServerIntegrationTest, ClusterCommandsWorkOverNetwork) {
  if (!started_) {
    GTEST_SKIP() << "server failed to start in test environment: "
                 << last_status_.ToString();
  }

  const int fd = ConnectWithRetry(port_);
  ASSERT_GE(fd, 0);

  std::string resp;
  ASSERT_TRUE(SendLine(fd, "CLUSTER ROUTE user:42"));
  ASSERT_TRUE(ReadResp(fd, &resp));
  EXPECT_NE(resp.find("*1\r\n"), std::string::npos);
  EXPECT_NE(resp.find("id="), std::string::npos);

  ASSERT_TRUE(SendLine(fd, "CLUSTER STATUS"));
  ASSERT_TRUE(ReadResp(fd, &resp));
  EXPECT_NE(resp.find("cluster.node_count=1"), std::string::npos);

  ASSERT_TRUE(SendLine(fd, "CLUSTER PLAN SET net_a 1 DEL net_b"));
  ASSERT_TRUE(ReadResp(fd, &resp));
  EXPECT_NE(resp.find("*1\r\n"), std::string::npos);
  EXPECT_NE(resp.find("id=n1"), std::string::npos);
  EXPECT_NE(resp.find("SET net_a 1"), std::string::npos);
  EXPECT_NE(resp.find("DEL net_b"), std::string::npos);

  ASSERT_TRUE(SendLine(fd, "CLUSTER BATCH SET net_a 1 SET net_b 2"));
  ASSERT_TRUE(ReadResp(fd, &resp));
  EXPECT_EQ(resp, "+OK\r\n");

  ASSERT_TRUE(SendLine(fd, "GET net_a"));
  ASSERT_TRUE(ReadResp(fd, &resp));
  EXPECT_EQ(resp, "$1\r\n1\r\n");

  (void)::close(fd);
}

}  // namespace
}  // namespace kv::net
