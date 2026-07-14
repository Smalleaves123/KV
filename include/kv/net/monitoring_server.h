#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "kv/common/socket_compat.h"
#include "kv/common/status.h"
#include "kv/concurrency/thread_pool.h"

namespace kv {
class DB;
}

namespace kv::net {

class Server;

// Small dependency-free HTTP endpoint for liveness, readiness, and
// Prometheus scraping. It intentionally runs on a separate port from the
// line/RESP command server.
class MonitoringServer {
 public:
  MonitoringServer();
  ~MonitoringServer();

  MonitoringServer(const MonitoringServer&) = delete;
  MonitoringServer& operator=(const MonitoringServer&) = delete;

  Status Start(uint16_t port, const Server* server, const DB* db);
  Status Stop();

  bool IsRunning() const noexcept;
  uint16_t port() const noexcept;

  static std::string RenderMetrics(const Server& server, const DB& db);
  static std::string RenderHealth(const Server& server, const DB& db,
                                  bool readiness);

 private:
  Status SetupListenSocket(uint16_t port);
  void AcceptLoop();
  static void HandleClient(platform::SocketHandle client_fd,
                           const Server* server,
                           const DB* db,
                           std::atomic<bool>* running);

  platform::SocketHandle listen_fd_;
  uint16_t port_;
  std::atomic<bool> running_;
  const Server* server_;
  const DB* db_;
  std::thread accept_thread_;
  std::unique_ptr<ThreadPool> pool_;
  platform::SocketRuntime socket_runtime_;
};

}  // namespace kv::net
