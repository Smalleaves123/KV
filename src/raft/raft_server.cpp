#include "kv/raft/raft_server.h"

#include <chrono>
#include <condition_variable>
#include <thread>

#include "kv/raft/raft_rpc_codec.h"
#include "kv/raft/raft_storage_impl.h"

namespace kv {

// ==================== RaftServer ====================

RaftServer::RaftServer(const RaftConfig& config, WriteApplier* applier)
    : config_(config),
      applier_(applier),
      storage_(std::make_shared<raft::FileRaftStorage>(config.data_dir)),
      raft_node_(),
      raft_mu_(),
      tick_thread_(),
      rpc_thread_(),
      running_(false),
      rpc_pool_(std::make_unique<ThreadPool>(2)),
      rpc_fd_(platform::kInvalidSocket),
      conn_mu_(),
      peer_fds_(),
      hard_state_mu_(),
      apply_mu_(),
      apply_cv_(),
      applied_results_(),
      socket_runtime_(),
      last_applied_(0) {
  // Build RaftOptions and create RaftNode
  raft::RaftOptions opts;
  opts.node_id = config.node_id;
  for (const auto& [id, peer] : config.peers) {
    opts.peers.push_back(id);
  }
  bool self_in_peers = false;
  for (auto id : opts.peers) {
    if (id == config.node_id) { self_in_peers = true; break; }
  }
  if (!self_in_peers) {
    opts.peers.push_back(config.node_id);
  }
  opts.storage = storage_;
  opts.election_tick = 10;
  opts.heartbeat_tick = 3;

  raft_node_ = std::make_unique<raft::RaftNode>(opts);

  // Wire Raft callbacks to our RPC sender
  raft_node_->set_send_request_vote_fn(
      [this](uint64_t to, const raft::RequestVoteArgs& args) {
        if (!running_.load()) {
          return;
        }
        rpc_pool_->Execute([this, to, args]() { SendRequestVote(to, args); });
      });
  raft_node_->set_send_append_entries_fn(
      [this](uint64_t to, const raft::AppendEntriesArgs& args) {
        if (!running_.load()) {
          return;
        }
        rpc_pool_->Execute([this, to, args]() { SendAppendEntries(to, args); });
      });
}

RaftServer::~RaftServer() {
  (void)Stop();
}

Status RaftServer::Start() {
  if (running_.load()) {
    return Status::AlreadyExists("raft server already running");
  }

  if (!socket_runtime_.Start()) {
    return Status::IOError("failed to initialize socket runtime");
  }

  rpc_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (!platform::IsValidSocket(rpc_fd_)) {
    socket_runtime_.Stop();
    return Status::IOError("raft rpc socket: " +
                           platform::SocketErrorString(
                               platform::LastSocketError()));
  }

  (void)platform::SetSocketOptionInt(rpc_fd_, SOL_SOCKET, SO_REUSEADDR, 1);

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(config_.raft_port);

  if (::bind(rpc_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    (void)platform::CloseSocket(rpc_fd_);
    rpc_fd_ = platform::kInvalidSocket;
    socket_runtime_.Stop();
    return Status::IOError("raft rpc bind: " +
                           platform::SocketErrorString(
                               platform::LastSocketError()));
  }

  if (::listen(rpc_fd_, 32) < 0) {
    (void)platform::CloseSocket(rpc_fd_);
    rpc_fd_ = platform::kInvalidSocket;
    socket_runtime_.Stop();
    return Status::IOError("raft rpc listen: " +
                           platform::SocketErrorString(
                               platform::LastSocketError()));
  }

  {
    std::lock_guard<std::mutex> lk(raft_mu_);
    last_applied_ = storage_->InitialState().applied_index;
  }

  running_.store(true);
  PersistHardState();
  tick_thread_ = std::thread(&RaftServer::TickLoop, this);
  rpc_thread_ = std::thread(&RaftServer::RpcListenLoop, this);

  return Status::OK();
}

Status RaftServer::Stop() {
  running_.store(false);
  apply_cv_.notify_all();

  if (platform::IsValidSocket(rpc_fd_)) {
    (void)platform::ShutdownSocket(rpc_fd_);
    (void)platform::CloseSocket(rpc_fd_);
    rpc_fd_ = platform::kInvalidSocket;
  }

  {
    std::lock_guard<std::mutex> lk(conn_mu_);
    for (auto& [id, fd] : peer_fds_) {
      (void)platform::CloseSocket(fd);
    }
    peer_fds_.clear();
  }

  if (tick_thread_.joinable()) tick_thread_.join();
  if (rpc_thread_.joinable()) rpc_thread_.join();
  rpc_pool_->WaitAndStop();

  socket_runtime_.Stop();

  PersistHardState();

  return Status::OK();
}

Status RaftServer::Propose(const std::string& cmd) {
  if (!running_.load()) {
    return Status::IOError("raft server not running");
  }

  uint64_t index = 0;
  {
    std::lock_guard<std::mutex> lk(raft_mu_);
    if (raft_node_->role() != raft::RaftRole::kLeader) {
      return Status::AlreadyExists("not the leader");
    }
    index = raft_node_->Propose(cmd);
    if (index == 0) {
      return Status::AlreadyExists("not the leader");
    }
  }

  std::unique_lock<std::mutex> lk(apply_mu_);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  const bool applied = apply_cv_.wait_until(lk, deadline, [this, index]() {
    return applied_results_.find(index) != applied_results_.end() ||
           !running_.load();
  });
  if (!applied) {
    return Status::IOError("timed out waiting for raft apply");
  }
  if (!running_.load()) {
    return Status::IOError("raft server stopped before apply");
  }

  Status result = applied_results_[index];
  applied_results_.erase(index);
  return result;
}

Status RaftServer::LinearizableReadBarrier() {
  return Propose(raft::EncodeNoopCmd());
}

bool RaftServer::IsLeader() const noexcept {
  std::lock_guard<std::mutex> lk(raft_mu_);
  return raft_node_->role() == raft::RaftRole::kLeader;
}

uint64_t RaftServer::LeaderId() const noexcept {
  std::lock_guard<std::mutex> lk(raft_mu_);
  return raft_node_->leader_id();
}

uint64_t RaftServer::NodeId() const noexcept {
  return config_.node_id;
}

// ---- Tick & Apply ----

void RaftServer::TickLoop() {
  constexpr auto tick_interval = std::chrono::milliseconds(50);
  while (running_.load()) {
    {
      std::lock_guard<std::mutex> lk(raft_mu_);
      raft_node_->Tick();
      ApplyCommitted();
    }
    PersistHardState();
    std::this_thread::sleep_for(tick_interval);
  }
}

void RaftServer::ApplyCommitted() {
  uint64_t ci = raft_node_->commit_index();
  while (last_applied_ < ci) {
    ++last_applied_;
    auto entries = storage_->Entries(last_applied_, last_applied_);
    if (entries.empty()) {
      std::lock_guard<std::mutex> lk(apply_mu_);
      applied_results_[last_applied_] =
          Status::Corruption("missing committed raft log entry");
      apply_cv_.notify_all();
      continue;
    }

    const auto& entry = entries[0];
    const std::string& data = entry.data;
    if (data.empty()) {
      std::lock_guard<std::mutex> lk(apply_mu_);
      applied_results_[last_applied_] =
          Status::Corruption("empty committed raft log entry");
      apply_cv_.notify_all();
      continue;
    }

    char op = 0;
    std::string key, value;
    if (!raft::DecodeCmd(data, &op, &key, &value)) {
      std::lock_guard<std::mutex> lk(apply_mu_);
      applied_results_[last_applied_] =
          Status::Corruption("failed to decode committed raft command");
      apply_cv_.notify_all();
      continue;
    }

    Status apply_status = Status::Corruption("unsupported raft command");
    if (op == 'S') {
      apply_status = applier_->ApplyPut(key, value);
    } else if (op == 'D') {
      apply_status = applier_->ApplyDelete(key);
    } else if (op == 'N') {
      apply_status = Status::OK();
    }

    raft_node_->AdvanceApplied(last_applied_);

    {
      std::lock_guard<std::mutex> lk(apply_mu_);
      applied_results_[last_applied_] = std::move(apply_status);
    }
    apply_cv_.notify_all();
  }
}

void RaftServer::PersistHardState() {
  raft::HardState hs;
  {
    std::lock_guard<std::mutex> lk(raft_mu_);
    hs.term = raft_node_->current_term();
    hs.vote_for = raft_node_->voted_for();
    hs.commit_index = raft_node_->commit_index();
    hs.applied_index = last_applied_;
  }

  std::lock_guard<std::mutex> lk(hard_state_mu_);
  storage_->SaveHardState(hs);
}

// ---- RPC Listener ----

void RaftServer::RpcListenLoop() {
  while (running_.load()) {
    sockaddr_in peer{};
    platform::SocketLength peer_len = sizeof(peer);
    platform::SocketHandle client_fd =
        ::accept(rpc_fd_, reinterpret_cast<sockaddr*>(&peer), &peer_len);
    if (!platform::IsValidSocket(client_fd)) {
      if (!running_.load()) break;
      if (platform::IsInterruptedSocketError(platform::LastSocketError()))
        continue;
      continue;
    }
    HandleRpcConnection(client_fd);
    (void)platform::CloseSocket(client_fd);
  }
}

void RaftServer::HandleRpcConnection(platform::SocketHandle fd) {
  std::string framed = raft::ReadMessage(fd);
  if (framed.empty()) return;

  raft::RaftMsgType msg_type;
  std::string body;
  if (!raft::DecodeMessage(framed.data(), framed.size(), &msg_type, &body))
    return;

  switch (msg_type) {
    case raft::RaftMsgType::kRequestVote: {
      raft::RequestVoteArgs args;
      if (!raft::DecodeRequestVote(body.data(), body.size(), &args)) return;

      raft::RequestVoteReply reply;
      {
        std::lock_guard<std::mutex> lk(raft_mu_);
        reply = raft_node_->HandleRequestVote(args);
      }
      PersistHardState();
      std::string reply_body = raft::EncodeRequestVoteReply(reply);
      std::string reply_msg = raft::EncodeMessage(
          raft::RaftMsgType::kRequestVoteReply, reply_body);
      (void)raft::WriteFull(fd, reply_msg.data(), reply_msg.size());
      break;
    }

    case raft::RaftMsgType::kAppendEntries: {
      raft::AppendEntriesArgs args;
      if (!raft::DecodeAppendEntries(body.data(), body.size(), &args)) return;

      raft::AppendEntriesReply reply;
      {
        std::lock_guard<std::mutex> lk(raft_mu_);
        reply = raft_node_->HandleAppendEntries(args);
      }
      PersistHardState();
      std::string reply_body = raft::EncodeAppendEntriesReply(reply);
      std::string reply_msg = raft::EncodeMessage(
          raft::RaftMsgType::kAppendEntriesReply, reply_body);
      (void)raft::WriteFull(fd, reply_msg.data(), reply_msg.size());
      break;
    }

    default:
      // Reply messages are handled by the sender side (SendRequestVote /
      // SendAppendEntries), not by the listener.
      break;
  }
}

// ---- Outgoing RPCs ----

void RaftServer::SendRequestVote(uint64_t to,
                                 const raft::RequestVoteArgs& args) {
  auto it = config_.peers.find(to);
  if (it == config_.peers.end()) return;

  std::string body = raft::EncodeRequestVote(args);
  std::string msg = raft::EncodeMessage(
      raft::RaftMsgType::kRequestVote, body);

  platform::SocketHandle fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (!platform::IsValidSocket(fd)) return;

  (void)platform::SetSendTimeout(fd, 1000);
  (void)platform::SetReceiveTimeout(fd, 1000);

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr(it->second.host.c_str());
  addr.sin_port = htons(it->second.raft_port);

  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    (void)platform::CloseSocket(fd);
    return;
  }

  if (raft::WriteFull(fd, msg.data(), msg.size()) < 0) {
    (void)platform::CloseSocket(fd);
    return;
  }

  std::string reply_framed = raft::ReadMessage(fd);
  (void)platform::CloseSocket(fd);

  if (reply_framed.empty()) return;

  raft::RaftMsgType reply_type;
  std::string reply_body;
  if (!raft::DecodeMessage(reply_framed.data(), reply_framed.size(),
                           &reply_type, &reply_body))
    return;

  raft::RequestVoteReply reply;
  if (!raft::DecodeRequestVoteReply(reply_body.data(), reply_body.size(),
                                    &reply))
    return;

  {
    std::lock_guard<std::mutex> lk(raft_mu_);
    raft_node_->HandleRequestVoteReply(to, reply);
  }
  PersistHardState();
}

void RaftServer::SendAppendEntries(uint64_t to,
                                   const raft::AppendEntriesArgs& args) {
  auto it = config_.peers.find(to);
  if (it == config_.peers.end()) return;

  std::string body = raft::EncodeAppendEntries(args);
  std::string msg = raft::EncodeMessage(
      raft::RaftMsgType::kAppendEntries, body);

  platform::SocketHandle fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (!platform::IsValidSocket(fd)) return;

  (void)platform::SetSendTimeout(fd, 2000);
  (void)platform::SetReceiveTimeout(fd, 2000);

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr(it->second.host.c_str());
  addr.sin_port = htons(it->second.raft_port);

  if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    (void)platform::CloseSocket(fd);
    return;
  }

  if (raft::WriteFull(fd, msg.data(), msg.size()) < 0) {
    (void)platform::CloseSocket(fd);
    return;
  }

  std::string reply_framed = raft::ReadMessage(fd);
  (void)platform::CloseSocket(fd);

  if (reply_framed.empty()) return;

  raft::RaftMsgType reply_type;
  std::string reply_body;
  if (!raft::DecodeMessage(reply_framed.data(), reply_framed.size(),
                           &reply_type, &reply_body))
    return;

  raft::AppendEntriesReply reply;
  if (!raft::DecodeAppendEntriesReply(reply_body.data(), reply_body.size(),
                                      &reply))
    return;

  {
    std::lock_guard<std::mutex> lk(raft_mu_);
    raft_node_->HandleAppendEntriesReply(to, reply);
  }
  PersistHardState();
}

}  // namespace kv
