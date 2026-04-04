#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

#include "kv/common/status.h"

namespace kv {
class DB;
}

namespace kv::net {

struct ServerStats {
  uint64_t total_connections = 0;
  uint64_t active_connections = 0;
  uint64_t total_requests = 0;
};

class Server {
 public:
  Server();
  ~Server();

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  Status Start(uint16_t port, DB* db);
  Status Stop();

  bool IsRunning() const noexcept;
  uint16_t port() const noexcept;
  ServerStats GetStats() const noexcept;

 private:
  Status SetupListenSocket(uint16_t port);
  void AcceptLoop(DB* db);
  void HandleClient(int client_fd, DB* db);

  int listen_fd_;
  uint16_t port_;
  std::atomic<bool> running_;
  std::atomic<uint64_t> total_connections_;
  std::atomic<uint64_t> active_connections_;
  std::atomic<uint64_t> total_requests_;

  std::thread accept_thread_;
  std::mutex workers_mu_;
  std::vector<std::thread> workers_;
};

}  // namespace kv::net
