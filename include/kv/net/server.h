#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>

#include "kv/common/socket_compat.h"
#include "kv/common/status.h"
#include "kv/concurrency/thread_pool.h"
#include "kv/net/command.h"

namespace kv {
class DB;
class ClusterManager;
}

namespace kv::net {

struct ServerStats {
  uint64_t total_connections = 0;
  uint64_t active_connections = 0;
  uint64_t total_requests = 0;
  uint64_t request_errors = 0;
  uint64_t response_bytes = 0;
  uint64_t request_duration_us = 0;
  uint64_t txn_begin = 0;
  uint64_t txn_commit = 0;
  uint64_t txn_abort = 0;
  uint64_t txn_conflict = 0;
  std::array<uint64_t, kCommandTypeCount> command_requests{};
  std::array<uint64_t, kCommandTypeCount> command_errors{};
};

class Server {
 public:
  Server();
  ~Server();

  Server(const Server&) = delete;
  Server& operator=(const Server&) = delete;

  Status Start(uint16_t port, DB* db, ClusterManager* cluster_manager = nullptr);
  Status Stop();

  bool IsRunning() const noexcept;
  uint16_t port() const noexcept;
  ServerStats GetStats() const noexcept;

 private:
  Status SetupListenSocket(uint16_t port);
  void AcceptLoop(DB* db, ClusterManager* cluster_manager);
  static void HandleClient(platform::SocketHandle client_fd,
                           DB* db,
                           ClusterManager* cluster_manager,
                           std::atomic<bool>* running,
                           std::atomic<uint64_t>* total_requests,
                           std::atomic<uint64_t>* request_errors,
                           std::atomic<uint64_t>* response_bytes,
                           std::atomic<uint64_t>* request_duration_us,
                           std::array<std::atomic<uint64_t>,
                                      kCommandTypeCount>* command_requests,
                           std::array<std::atomic<uint64_t>,
                                      kCommandTypeCount>* command_errors,
                           std::atomic<uint64_t>* txn_begin,
                           std::atomic<uint64_t>* txn_commit,
                           std::atomic<uint64_t>* txn_abort,
                           std::atomic<uint64_t>* txn_conflict,
                           std::atomic<uint64_t>* active_connections);

  platform::SocketHandle listen_fd_;
  uint16_t port_;
  std::atomic<bool> running_;
  std::atomic<uint64_t> total_connections_;
  std::atomic<uint64_t> active_connections_;
  std::atomic<uint64_t> total_requests_;
  std::atomic<uint64_t> request_errors_;
  std::atomic<uint64_t> response_bytes_;
  std::atomic<uint64_t> request_duration_us_;
  std::array<std::atomic<uint64_t>, kCommandTypeCount> command_requests_;
  std::array<std::atomic<uint64_t>, kCommandTypeCount> command_errors_;
  std::atomic<uint64_t> txn_begin_;
  std::atomic<uint64_t> txn_commit_;
  std::atomic<uint64_t> txn_abort_;
  std::atomic<uint64_t> txn_conflict_;

  std::thread accept_thread_;
  std::unique_ptr<ThreadPool> pool_;
  platform::SocketRuntime socket_runtime_;
};

}  // namespace kv::net
