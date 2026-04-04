#include "kv/net/server.h"

#include <cerrno>
#include <cstring>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>

#include "kv/engine/db.h"
#include "kv/net/connection.h"
#include "kv/net/session.h"

namespace kv::net {

Server::Server()
    : listen_fd_(-1),
      port_(0),
      running_(false),
      total_connections_(0),
      active_connections_(0),
      total_requests_(0),
      accept_thread_(),
      workers_mu_(),
      workers_() {}

Server::~Server() {
  (void)Stop();
}

Status Server::Start(uint16_t port, DB* db) {
  if (db == nullptr) {
    return Status::InvalidArgument("db is null");
  }
  if (port == 0) {
    return Status::InvalidArgument("port must be greater than 0");
  }
  if (running_.load()) {
    return Status::AlreadyExists("server already running");
  }

  Status s = SetupListenSocket(port);
  if (!s.ok()) {
    return s;
  }

  running_.store(true);
  accept_thread_ = std::thread(&Server::AcceptLoop, this, db);
  return Status::OK();
}

Status Server::Stop() {
  running_.store(false);

  if (listen_fd_ >= 0) {
    (void)::shutdown(listen_fd_, SHUT_RDWR);
    (void)::close(listen_fd_);
    listen_fd_ = -1;
  }

  if (accept_thread_.joinable()) {
    accept_thread_.join();
  }

  {
    std::lock_guard<std::mutex> lock(workers_mu_);
    for (auto& t : workers_) {
      if (t.joinable()) {
        t.join();
      }
    }
    workers_.clear();
  }

  return Status::OK();
}

bool Server::IsRunning() const noexcept {
  return running_.load();
}

uint16_t Server::port() const noexcept {
  return port_;
}

ServerStats Server::GetStats() const noexcept {
  ServerStats stats;
  stats.total_connections = total_connections_.load();
  stats.active_connections = active_connections_.load();
  stats.total_requests = total_requests_.load();
  return stats;
}

Status Server::SetupListenSocket(uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return Status::IOError("socket failed: " + std::string(std::strerror(errno)));
  }

  int yes = 1;
  (void)::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port);

  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    const std::string err = std::strerror(errno);
    (void)::close(fd);
    return Status::IOError("bind failed: " + err);
  }

  if (::listen(fd, 128) < 0) {
    const std::string err = std::strerror(errno);
    (void)::close(fd);
    return Status::IOError("listen failed: " + err);
  }

  listen_fd_ = fd;
  port_ = port;
  return Status::OK();
}

void Server::AcceptLoop(DB* db) {
  while (running_.load()) {
    sockaddr_in peer{};
    socklen_t peer_len = sizeof(peer);
    const int client_fd = ::accept(
        listen_fd_, reinterpret_cast<sockaddr*>(&peer), &peer_len);
    if (client_fd < 0) {
      if (!running_.load()) {
        break;
      }
      if (errno == EINTR) {
        continue;
      }
      continue;
    }

    total_connections_.fetch_add(1);
    active_connections_.fetch_add(1);

    std::lock_guard<std::mutex> lock(workers_mu_);
    workers_.emplace_back(&Server::HandleClient, this, client_fd, db);
  }
}

void Server::HandleClient(int client_fd, DB* db) {
  Connection conn(client_fd);
  Session session(db);

  timeval timeout{};
  timeout.tv_sec = 0;
  timeout.tv_usec = 200 * 1000;
  (void)::setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

  while (running_.load()) {
    std::string line;
    Status s = conn.ReadLine(&line);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        break;
      }
      if (s.IsIOError()) {
        continue;
      }
      break;
    }

    total_requests_.fetch_add(1);

    const std::string resp = session.HandleLine(line);
    s = conn.WriteAll(resp);
    if (!s.ok()) {
      break;
    }
  }

  (void)conn.Close();
  active_connections_.fetch_sub(1);
}

}  // namespace kv::net
