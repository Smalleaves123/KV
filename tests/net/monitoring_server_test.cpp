#include "kv/net/monitoring_server.h"

#include <chrono>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "kv/engine/db.h"
#include "kv/net/server.h"

namespace kv::net {
namespace {

DBOptions MakeDBOptions() {
  static int counter = 0;
  ++counter;
  DBOptions options;
  const std::string base =
      "test_tmp/monitoring/monitoring_" + std::to_string(counter);
  options.wal_path = base + ".wal";
  options.sst_dir = base + "_sst";
  options.manifest_path = base + ".manifest";
  return options;
}

platform::SocketHandle Connect(uint16_t port) {
  for (int attempt = 0; attempt < 50; ++attempt) {
    const platform::SocketHandle fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (!platform::IsValidSocket(fd)) return platform::kInvalidSocket;

    (void)platform::SetReceiveTimeout(fd, 2000);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&address),
                  sizeof(address)) == 0) {
      return fd;
    }
    (void)platform::CloseSocket(fd);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return platform::kInvalidSocket;
}

std::string HttpGet(uint16_t port, const std::string& path) {
  const platform::SocketHandle fd = Connect(port);
  if (!platform::IsValidSocket(fd)) return {};

  const std::string request = "GET " + path +
                              " HTTP/1.1\r\nHost: localhost\r\n"
                              "Connection: close\r\n\r\n";
  size_t written = 0;
  while (written < request.size()) {
    const platform::SocketIoResult sent = platform::SendSocket(
        fd, request.data() + written, request.size() - written, 0);
    if (sent <= 0) {
      (void)platform::CloseSocket(fd);
      return {};
    }
    written += static_cast<size_t>(sent);
  }

  std::string response;
  char buffer[1024];
  while (true) {
    const platform::SocketIoResult received =
        platform::ReceiveSocket(fd, buffer, sizeof(buffer), 0);
    if (received <= 0) break;
    response.append(buffer, static_cast<size_t>(received));
  }
  (void)platform::CloseSocket(fd);
  return response;
}

class MonitoringServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    options_ = MakeDBOptions();
    std::error_code ec;
    std::filesystem::remove(options_.wal_path, ec);
    std::filesystem::remove(options_.manifest_path, ec);
    std::filesystem::remove_all(options_.sst_dir, ec);

    ASSERT_TRUE(DB::Open(options_, &db_).ok());
    ASSERT_TRUE(server_.Start(0, db_.get()).ok());
    ASSERT_TRUE(monitoring_.Start(0, &server_, db_.get()).ok());
  }

  void TearDown() override {
    (void)monitoring_.Stop();
    (void)server_.Stop();
    if (db_ != nullptr) (void)db_->Close();
    std::error_code ec;
    std::filesystem::remove(options_.wal_path, ec);
    std::filesystem::remove(options_.manifest_path, ec);
    std::filesystem::remove_all(options_.sst_dir, ec);
  }

  DBOptions options_;
  std::unique_ptr<DB> db_;
  Server server_;
  MonitoringServer monitoring_;
};

TEST_F(MonitoringServerTest, HealthReadinessAndMetricsEndpoints) {
  const std::string health = HttpGet(monitoring_.port(), "/health");
  EXPECT_NE(health.find("200 OK"), std::string::npos);
  EXPECT_NE(health.find("\"status\":\"ok\""), std::string::npos);
  EXPECT_NE(health.find("\"db_open\":true"), std::string::npos);

  const std::string ready = HttpGet(monitoring_.port(), "/readyz");
  EXPECT_NE(ready.find("200 OK"), std::string::npos);
  EXPECT_NE(ready.find("\"ready\":true"), std::string::npos);

  const std::string metrics = HttpGet(monitoring_.port(), "/metrics");
  EXPECT_NE(metrics.find("200 OK"), std::string::npos);
  EXPECT_NE(metrics.find("kv_server_up 1"), std::string::npos);
  EXPECT_NE(metrics.find("kv_server_requests_total"), std::string::npos);
  EXPECT_NE(metrics.find("kv_server_request_errors_total"),
           std::string::npos);
  EXPECT_NE(metrics.find("kv_server_response_bytes_total"),
           std::string::npos);
  EXPECT_NE(metrics.find("kv_server_request_duration_microseconds_total"),
           std::string::npos);
  EXPECT_NE(metrics.find("kv_server_commands_total{command=\"PING\"}"),
            std::string::npos);
  EXPECT_NE(metrics.find("kv_server_commands_total{command=\"INVALID\"}"),
            std::string::npos);
  EXPECT_NE(
      metrics.find("kv_server_command_errors_total{command=\"GET\"}"),
      std::string::npos);
  EXPECT_NE(metrics.find("kv_db_up 1"), std::string::npos);
  EXPECT_NE(metrics.find("kv_db_compaction_attempts_total"),
           std::string::npos);
}

TEST_F(MonitoringServerTest, ReportsNotReadyAfterCommandServerStops) {
  ASSERT_TRUE(server_.Stop().ok());
  const std::string response = HttpGet(monitoring_.port(), "/health");
  EXPECT_NE(response.find("503 Service Unavailable"), std::string::npos);
  EXPECT_NE(response.find("\"server_running\":false"), std::string::npos);
}

TEST_F(MonitoringServerTest, UnknownPathReturnsNotFound) {
  const std::string response = HttpGet(monitoring_.port(), "/unknown");
  EXPECT_NE(response.find("404 Not Found"), std::string::npos);
}

}  // namespace
}  // namespace kv::net
