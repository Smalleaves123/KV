#include "kv/net/server.h"

#include <chrono>

#include "kv/cluster/cluster_manager.h"
#include "kv/engine/db.h"
#include "kv/net/connection.h"
#include "kv/net/protocol.h"
#include "kv/net/session.h"

namespace kv::net {

Server::Server()
    : listen_fd_(platform::kInvalidSocket),
      port_(0),
      running_(false),
      total_connections_(0),
      active_connections_(0),
      total_requests_(0),
      txn_begin_(0),
      txn_commit_(0),
      txn_abort_(0),
      txn_conflict_(0),
      accept_thread_(),
      pool_(std::make_unique<ThreadPool>(0)),
      socket_runtime_() {}

Server::~Server() {
  (void)Stop();
}

Status Server::Start(uint16_t port, DB* db, ClusterManager* cluster_manager) {
  if (db == nullptr) {
    return Status::InvalidArgument("db is null");
  }
  if (running_.load()) {
    return Status::AlreadyExists("server already running");
  }

  if (!socket_runtime_.Start()) {
    return Status::IOError("failed to initialize socket runtime");
  }

  Status s = SetupListenSocket(port);
  if (!s.ok()) {
    socket_runtime_.Stop();
    return s;
  }

  pool_ = std::make_unique<ThreadPool>(0);
  running_.store(true);
  accept_thread_ = std::thread(&Server::AcceptLoop, this, db, cluster_manager);
  return Status::OK();
}

Status Server::Stop() {
  running_.store(false);

  if (platform::IsValidSocket(listen_fd_)) {
    (void)platform::ShutdownSocket(listen_fd_);
    (void)platform::CloseSocket(listen_fd_);
    listen_fd_ = platform::kInvalidSocket;
  }

  if (accept_thread_.joinable()) {
    accept_thread_.join();
  }

  if (pool_ != nullptr) {
    pool_->WaitAndStop();
  }

  socket_runtime_.Stop();

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
  stats.txn_begin = txn_begin_.load();
  stats.txn_commit = txn_commit_.load();
  stats.txn_abort = txn_abort_.load();
  stats.txn_conflict = txn_conflict_.load();
  return stats;
}

Status Server::SetupListenSocket(uint16_t port) {
  const platform::SocketHandle fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (!platform::IsValidSocket(fd)) {
    const int error = platform::LastSocketError();
    return Status::IOError("socket failed: " +
                           platform::SocketErrorString(error));
  }

  (void)platform::SetSocketOptionInt(fd, SOL_SOCKET, SO_REUSEADDR, 1);

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port);

  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    const std::string err =
        platform::SocketErrorString(platform::LastSocketError());
    (void)platform::CloseSocket(fd);
    return Status::IOError("bind failed: " + err);
  }

  if (::listen(fd, 128) < 0) {
    const std::string err =
        platform::SocketErrorString(platform::LastSocketError());
    (void)platform::CloseSocket(fd);
    return Status::IOError("listen failed: " + err);
  }

  sockaddr_in bound_addr{};
  platform::SocketLength bound_len = sizeof(bound_addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&bound_addr),
                    &bound_len) < 0) {
    const std::string err =
        platform::SocketErrorString(platform::LastSocketError());
    (void)platform::CloseSocket(fd);
    return Status::IOError("getsockname failed: " + err);
  }

  listen_fd_ = fd;
  port_ = ntohs(bound_addr.sin_port);
  return Status::OK();
}

void Server::AcceptLoop(DB* db, ClusterManager* cluster_manager) {
  while (running_.load()) {
    sockaddr_in peer{};
    platform::SocketLength peer_len = sizeof(peer);
    const platform::SocketHandle client_fd = ::accept(
        listen_fd_, reinterpret_cast<sockaddr*>(&peer), &peer_len);
    if (!platform::IsValidSocket(client_fd)) {
      if (!running_.load()) {
        break;
      }
      if (platform::IsInterruptedSocketError(platform::LastSocketError())) {
        continue;
      }
      continue;
    }

    total_connections_.fetch_add(1);
    active_connections_.fetch_add(1);

    pool_->Execute([client_fd, db, cluster_manager, this]() {
      HandleClient(client_fd, db, cluster_manager,
                   &running_,
                   &total_requests_,
                   &txn_begin_,
                   &txn_commit_,
                   &txn_abort_,
                   &txn_conflict_,
                   &active_connections_);
    });
  }
}

void Server::HandleClient(platform::SocketHandle client_fd,
                          DB* db,
                          ClusterManager* cluster_manager,
                          std::atomic<bool>* running,
                          std::atomic<uint64_t>* total_requests,
                          std::atomic<uint64_t>* txn_begin,
                          std::atomic<uint64_t>* txn_commit,
                          std::atomic<uint64_t>* txn_abort,
                          std::atomic<uint64_t>* txn_conflict,
                          std::atomic<uint64_t>* active_connections) {
  Connection conn(client_fd);
  Session session(db, cluster_manager);

  (void)platform::SetReceiveTimeout(client_fd, 200);

  while (running->load()) {
    std::vector<std::string> tokens;
    Status s = conn.ReadRequest(&tokens);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        break;
      }
      if (s.IsInvalidArgument()) {
        (void)conn.WriteAll(kv::net::protocol::Error(s.ToString()));
        continue;
      }
      if (s.IsIOError()) {
        continue;
      }
      break;
    }

    total_requests->fetch_add(1);

    const std::string resp = session.HandleTokens(tokens);
    switch (session.LastTxnEvent()) {
      case TxnEvent::kBegin:
        txn_begin->fetch_add(1);
        break;
      case TxnEvent::kCommit:
        txn_commit->fetch_add(1);
        break;
      case TxnEvent::kAbort:
        txn_abort->fetch_add(1);
        break;
      case TxnEvent::kConflict:
        txn_conflict->fetch_add(1);
        break;
      case TxnEvent::kNone:
      default:
        break;
    }

    s = conn.WriteAll(resp);
    if (!s.ok()) {
      break;
    }
  }

  (void)conn.Close();
  active_connections->fetch_sub(1);
}

}  // namespace kv::net
