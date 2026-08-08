#include "kv/raft/raft_server.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <limits>
#include <thread>

#include "kv/raft/raft_rpc_codec.h"
#include "kv/raft/raft_storage_impl.h"

namespace kv {

namespace {

constexpr char kSnapshotArchiveMagic[] = "KVRAFTSNAP1";

void AppendBE32(std::string* out, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

void AppendBE64(std::string* out, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>((value >> shift) & 0xff));
  }
}

uint32_t ReadBE32(const char* data) {
  uint32_t value = 0;
  for (int i = 0; i < 4; ++i) {
    value = (value << 8) | static_cast<uint8_t>(data[i]);
  }
  return value;
}

uint64_t ReadBE64(const char* data) {
  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value = (value << 8) | static_cast<uint8_t>(data[i]);
  }
  return value;
}

bool IsSafeRelativePath(const std::filesystem::path& path) {
  if (path.empty() || path.is_absolute()) return false;
  for (const auto& component : path) {
    if (component == ".." || component == ".") return false;
  }
  return true;
}

Status ExtractSnapshotArchive(const std::string& archive,
                              const std::string& destination) {
  const size_t magic_size = sizeof(kSnapshotArchiveMagic) - 1;
  if (archive.size() < magic_size + 4 ||
      archive.compare(0, magic_size, kSnapshotArchiveMagic) != 0) {
    return Status::Corruption("invalid raft snapshot archive");
  }

  const char* data = archive.data();
  size_t offset = magic_size;
  const uint32_t file_count = ReadBE32(data + offset);
  offset += 4;
  const std::filesystem::path destination_path(destination);
  std::error_code ec;
  std::filesystem::remove_all(destination_path, ec);
  if (ec || !std::filesystem::create_directories(destination_path, ec) || ec) {
    return Status::IOError("failed to create snapshot staging directory");
  }

  for (uint32_t i = 0; i < file_count; ++i) {
    if (archive.size() - offset < 12) {
      return Status::Corruption("truncated raft snapshot archive header");
    }
    const uint32_t path_size = ReadBE32(data + offset);
    offset += 4;
    const uint64_t content_size = ReadBE64(data + offset);
    offset += 8;
    if (path_size == 0 || content_size > archive.size() - offset ||
        path_size > archive.size() - offset - content_size) {
      return Status::Corruption("invalid raft snapshot archive entry");
    }

    const std::filesystem::path relative(
        std::string(data + offset, path_size));
    offset += path_size;
    if (!IsSafeRelativePath(relative)) {
      return Status::Corruption("unsafe path in raft snapshot archive");
    }

    const std::filesystem::path output = destination_path / relative;
    std::filesystem::create_directories(output.parent_path(), ec);
    if (ec) {
      return Status::IOError("failed to create snapshot file directory");
    }
    std::ofstream file(output, std::ios::binary | std::ios::trunc);
    if (!file.is_open()) {
      return Status::IOError("failed to open staged snapshot file");
    }
    file.write(data + offset, static_cast<std::streamsize>(content_size));
    if (!file) {
      return Status::IOError("failed to write staged snapshot file");
    }
    offset += static_cast<size_t>(content_size);
  }

  if (offset != archive.size()) {
    return Status::Corruption("trailing data in raft snapshot archive");
  }
  return Status::OK();
}

}  // namespace

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
      inbound_rpc_pool_(std::make_unique<ThreadPool>(2)),
      inbound_rpc_connections_(0),
      rpc_fd_(platform::kInvalidSocket),
      conn_mu_(),
      peer_fds_(),
      hard_state_mu_(),
      apply_mu_(),
      membership_mu_(),
      apply_cv_(),
      applied_results_(),
      socket_runtime_(),
      last_applied_(0) {
  // Build RaftOptions and create RaftNode
  raft::RaftOptions opts;
  opts.node_id = config.node_id;
  const std::vector<uint64_t> persisted_members = storage_->InitialMembers();
  const bool has_persisted_members = !persisted_members.empty();
  opts.peers = persisted_members;
  if (opts.peers.empty()) {
    if (!config.members.empty()) {
      opts.peers = config.members;
    } else {
      for (const auto& [id, peer] : config.peers) {
        opts.peers.push_back(id);
      }
    }
  }
  bool self_in_peers = false;
  for (auto id : opts.peers) {
    if (id == config.node_id) { self_in_peers = true; break; }
  }
  if (!self_in_peers && !has_persisted_members && config.members.empty()) {
    opts.peers.push_back(config.node_id);
  }
  std::sort(opts.peers.begin(), opts.peers.end());
  opts.peers.erase(std::unique(opts.peers.begin(), opts.peers.end()),
                   opts.peers.end());
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
  raft_node_->set_send_install_snapshot_fn(
      [this](uint64_t to, const raft::RaftSnapshotMeta& meta) {
        if (!running_.load()) {
          return;
        }
        rpc_pool_->Execute([this, to, meta]() {
          SendInstallSnapshot(to, meta);
        });
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

  Status storage_status = storage_->InitialStatus();
  if (!storage_status.ok()) {
    socket_runtime_.Stop();
    return storage_status;
  }
  if (storage_->InitialMembers().empty()) {
    storage_status = storage_->SaveMembers(raft_node_->Members());
    if (!storage_status.ok()) {
      socket_runtime_.Stop();
      return storage_status;
    }
  }

  Status recovery_status = RecoverSnapshotOnStart();
  if (!recovery_status.ok()) {
    socket_runtime_.Stop();
    return recovery_status;
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
  if (config_.host.empty() ||
      ::inet_pton(AF_INET, config_.host.c_str(), &addr.sin_addr) != 1) {
    (void)platform::CloseSocket(rpc_fd_);
    rpc_fd_ = platform::kInvalidSocket;
    socket_runtime_.Stop();
    return Status::InvalidArgument("invalid raft bind address: " + config_.host);
  }
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

  running_.store(true);
  storage_status = PersistHardState();
  if (!storage_status.ok()) {
    running_.store(false);
    (void)platform::CloseSocket(rpc_fd_);
    rpc_fd_ = platform::kInvalidSocket;
    socket_runtime_.Stop();
    return storage_status;
  }
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
  if (inbound_rpc_pool_ != nullptr) inbound_rpc_pool_->WaitAndStop();
  rpc_pool_->WaitAndStop();

  socket_runtime_.Stop();

  return PersistHardState();
}

Status RaftServer::Propose(const std::string& cmd) {
  if (!running_.load()) {
    return Status::IOError("raft server not running");
  }

  uint64_t index = 0;
  Status propose_status;
  {
    std::lock_guard<std::mutex> lk(raft_mu_);
    if (raft_node_->role() != raft::RaftRole::kLeader) {
      return Status::AlreadyExists("not the leader");
    }
    propose_status = raft_node_->Propose(cmd, &index);
  }
  if (!propose_status.ok()) return propose_status;
  Status persist_status = PersistHardState();
  if (!persist_status.ok()) return persist_status;

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

Status RaftServer::ChangeMembership(const std::vector<uint64_t>& members) {
  std::lock_guard<std::mutex> change_lk(membership_mu_);
  if (!running_.load()) {
    return Status::IOError("raft server not running");
  }
  if (members.empty()) {
    return Status::InvalidArgument("raft membership cannot be empty");
  }

  std::vector<uint64_t> normalized = members;
  std::sort(normalized.begin(), normalized.end());
  normalized.erase(std::unique(normalized.begin(), normalized.end()),
                  normalized.end());
  if (normalized.size() != members.size() ||
      std::any_of(normalized.begin(), normalized.end(),
                  [](uint64_t id) { return id == 0; })) {
    return Status::InvalidArgument("raft membership contains duplicate/invalid ids");
  }

  {
    std::lock_guard<std::mutex> lk(raft_mu_);
    if (raft_node_->role() != raft::RaftRole::kLeader) {
      return Status::AlreadyExists("not the leader");
    }
    for (uint64_t member : normalized) {
      if (member != config_.node_id && config_.peers.find(member) ==
                                           config_.peers.end()) {
        return Status::NotFound("missing address for raft member " +
                               std::to_string(member));
      }
    }
    if (normalized == raft_node_->Members()) return Status::OK();
  }
  return Propose(raft::EncodeMembershipCmd(normalized));
}

Status RaftServer::RecoverSnapshotOnStart() {
  const raft::RaftSnapshotMeta meta = storage_->SnapshotMeta();
  const raft::HardState prior = storage_->InitialState();
  if (meta.last_included_index == 0) {
    std::lock_guard<std::mutex> lk(raft_mu_);
    last_applied_ = prior.applied_index;
    return Status::OK();
  }
  if (meta.last_included_term == 0) {
    return Status::Corruption("raft snapshot metadata has no term");
  }

  const std::string snapshot_dir = SnapshotDirectory(meta);
  std::error_code ec;
  if (!std::filesystem::is_directory(snapshot_dir, ec) || ec) {
    return Status::Corruption("raft snapshot checkpoint is missing: " +
                             snapshot_dir);
  }
  Status s = applier_->InstallCheckpoint(snapshot_dir);
  if (!s.ok()) return s;

  raft::HardState recovered = prior;
  recovered.commit_index = std::max(recovered.commit_index,
                                    meta.last_included_index);
  recovered.applied_index = meta.last_included_index;
  {
    std::lock_guard<std::mutex> lk(raft_mu_);
    last_applied_ = meta.last_included_index;
  }
  {
    std::lock_guard<std::mutex> lk(hard_state_mu_);
    s = storage_->SaveHardState(recovered);
  }
  return s;
}

Status RaftServer::ApplyMembership(const std::vector<uint64_t>& members) {
  if (!raft_node_->UpdateMembership(members)) {
    return Status::Corruption("invalid committed raft membership");
  }
  return storage_->SaveMembers(raft_node_->Members());
}

Status RaftServer::CreateSnapshot() {
  if (!running_.load()) {
    return Status::IOError("raft server not running");
  }

  std::lock_guard<std::mutex> lk(raft_mu_);
  if (last_applied_ == 0) {
    return Status::NotFound("no applied raft entries to snapshot");
  }

  const raft::RaftSnapshotMeta current_meta = storage_->SnapshotMeta();
  if (last_applied_ <= current_meta.last_included_index) {
    return Status::OK();
  }

  const uint64_t last_included_term = storage_->Term(last_applied_);
  if (last_included_term == 0) {
    return Status::Corruption("cannot find term for applied raft entry");
  }

  const std::string snapshot_dir = SnapshotDirectory(
      raft::RaftSnapshotMeta{last_applied_, last_included_term});
  Status s = applier_->CreateCheckpoint(snapshot_dir);
  if (!s.ok()) {
    return s;
  }

  const raft::RaftSnapshotMeta meta{last_applied_, last_included_term};
  s = raft_node_->CompactSnapshot(meta);
  if (!s.ok()) return s;
  const raft::RaftSnapshotMeta persisted_meta = storage_->SnapshotMeta();
  if (persisted_meta.last_included_index != meta.last_included_index ||
      persisted_meta.last_included_term != meta.last_included_term) {
    return Status::IOError("failed to persist raft snapshot metadata");
  }
  return Status::OK();
}

std::string RaftServer::SnapshotDirectory(
    const raft::RaftSnapshotMeta& meta) const {
  return (std::filesystem::path(config_.data_dir) / "snapshot" /
          std::to_string(meta.last_included_index) / "db")
      .string();
}

Status RaftServer::BuildSnapshotArchive(const std::string& snapshot_dir,
                                        std::string* archive) {
  if (archive == nullptr) {
    return Status::InvalidArgument("snapshot archive output is null");
  }
  const std::filesystem::path root(snapshot_dir);
  std::error_code ec;
  if (!std::filesystem::is_directory(root, ec) || ec) {
    return Status::NotFound("raft snapshot directory does not exist");
  }

  std::vector<std::filesystem::path> files;
  for (std::filesystem::recursive_directory_iterator it(root, ec), end;
       it != end && !ec; it.increment(ec)) {
    if (it->is_regular_file(ec) && !ec) {
      files.push_back(it->path());
    }
  }
  if (ec) {
    return Status::IOError("failed to enumerate raft snapshot files");
  }
  std::sort(files.begin(), files.end());

  const size_t magic_size = sizeof(kSnapshotArchiveMagic) - 1;
  archive->clear();
  archive->append(kSnapshotArchiveMagic, magic_size);
  if (files.size() > std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument("too many files in raft snapshot");
  }
  AppendBE32(archive, static_cast<uint32_t>(files.size()));

  for (const auto& file_path : files) {
    const std::filesystem::path relative = file_path.lexically_relative(root);
    const std::string relative_name = relative.generic_string();
    if (!IsSafeRelativePath(relative) ||
        relative_name.size() > std::numeric_limits<uint32_t>::max()) {
      return Status::Corruption("invalid raft snapshot file path");
    }

    std::ifstream input(file_path, std::ios::binary);
    if (!input.is_open()) {
      return Status::IOError("failed to open raft snapshot file");
    }
    const std::string content((std::istreambuf_iterator<char>(input)),
                              std::istreambuf_iterator<char>());
    if (content.size() > std::numeric_limits<uint64_t>::max()) {
      return Status::InvalidArgument("raft snapshot file is too large");
    }
    AppendBE32(archive, static_cast<uint32_t>(relative_name.size()));
    AppendBE64(archive, static_cast<uint64_t>(content.size()));
    archive->append(relative_name);
    archive->append(content);
    if (archive->size() > raft::kMaxRaftMsgSize - 64) {
      return Status::InvalidArgument("raft snapshot exceeds RPC size limit");
    }
  }
  return Status::OK();
}

Status RaftServer::InstallSnapshotArchive(const raft::InstallSnapshotArgs& args) {
  const std::filesystem::path staging =
      std::filesystem::path(config_.data_dir) / "snapshot" /
      ("incoming-" + std::to_string(args.meta.last_included_index));
  Status s = ExtractSnapshotArchive(args.data, staging.string());
  if (!s.ok()) {
    return s;
  }
  s = applier_->InstallCheckpoint(staging.string());
  std::error_code ec;
  std::filesystem::remove_all(staging, ec);
  return s;
}

bool RaftServer::IsLeader() const noexcept {
  std::lock_guard<std::mutex> lk(raft_mu_);
  return raft_node_->role() == raft::RaftRole::kLeader;
}

uint64_t RaftServer::LeaderId() const noexcept {
  std::lock_guard<std::mutex> lk(raft_mu_);
  return raft_node_->leader_id();
}

std::optional<NodeAddress> RaftServer::GetLeaderAddress() const {
  const uint64_t leader_id = LeaderId();
  if (leader_id == 0 || leader_id == config_.node_id) {
    return std::nullopt;
  }

  const auto it = config_.peers.find(leader_id);
  if (it == config_.peers.end()) {
    return std::nullopt;
  }

  uint16_t client_port = it->second.client_port;
  if (client_port == 0 && it->second.raft_port > 1) {
    client_port = static_cast<uint16_t>(it->second.raft_port - 1);
  }
  if (client_port == 0 || it->second.host.empty()) {
    return std::nullopt;
  }
  return NodeAddress{it->second.host, client_port};
}

RaftStats RaftServer::GetStats() const noexcept {
  std::lock_guard<std::mutex> lk(raft_mu_);
  RaftStats stats;
  stats.running = running_.load();
  stats.is_leader = raft_node_->role() == raft::RaftRole::kLeader;
  stats.term = raft_node_->current_term();
  stats.voted_for = raft_node_->voted_for();
  stats.leader_id = raft_node_->leader_id();
  stats.commit_index = raft_node_->commit_index();
  stats.applied_index = last_applied_;
  stats.last_log_index = raft_node_->last_log_index();
  stats.snapshot_last_included_index =
      storage_->SnapshotMeta().last_included_index;
  stats.members = raft_node_->Members();
  for (const auto& [peer_id, progress] : raft_node_->Progresses()) {
    stats.peers.push_back(
        RaftStats::PeerProgress{peer_id, progress.match, progress.next});
  }
  return stats;
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
    const uint64_t next_index = last_applied_ + 1;
    auto entries = storage_->Entries(next_index, next_index);
    Status apply_status = Status::Corruption("missing committed raft log entry");
    if (!entries.empty() && !entries[0].data.empty()) {
      const std::string& data = entries[0].data;
      if (data[0] == 'M') {
        std::vector<uint64_t> members;
        if (raft::DecodeMembershipCmd(data, &members)) {
          apply_status = ApplyMembership(members);
        } else {
          apply_status = Status::Corruption("failed to decode raft membership");
        }
      } else {
        char op = 0;
        std::string key, value;
        uint64_t expires_at_ms = 0;
        if (raft::DecodeCmd(data, &op, &key, &value, &expires_at_ms)) {
          apply_status = Status::Corruption("unsupported raft command");
          if (op == 'S') {
            apply_status = applier_->ApplyPut(key, value);
          } else if (op == 'T') {
            apply_status =
                applier_->ApplyPutWithExpiry(key, value, expires_at_ms);
          } else if (op == 'E') {
            apply_status = applier_->ApplyExpireAt(key, expires_at_ms);
          } else if (op == 'D') {
            apply_status = applier_->ApplyDelete(key);
          } else if (op == 'N') {
            apply_status = Status::OK();
          }
        } else {
          apply_status = Status::Corruption("failed to decode committed raft command");
        }
      }
    }

    if (!apply_status.ok()) {
      std::lock_guard<std::mutex> lk(apply_mu_);
      applied_results_[next_index] = std::move(apply_status);
      apply_cv_.notify_all();
      return;
    }

    last_applied_ = next_index;
    raft_node_->AdvanceApplied(last_applied_);

    {
      std::lock_guard<std::mutex> lk(apply_mu_);
      applied_results_[last_applied_] = Status::OK();
      constexpr size_t kMaxTrackedApplyResults = 1024;
      if (applied_results_.size() > kMaxTrackedApplyResults) {
        const uint64_t retain_after = last_applied_ - kMaxTrackedApplyResults;
        for (auto it = applied_results_.begin(); it != applied_results_.end();) {
          if (it->first < retain_after) {
            it = applied_results_.erase(it);
          } else {
            ++it;
          }
        }
      }
    }
    apply_cv_.notify_all();
  }
}

Status RaftServer::PersistHardState() {
  raft::HardState hs;
  {
    std::lock_guard<std::mutex> lk(raft_mu_);
    hs.term = raft_node_->current_term();
    hs.vote_for = raft_node_->voted_for();
    hs.commit_index = raft_node_->commit_index();
    hs.applied_index = last_applied_;
  }

  std::lock_guard<std::mutex> lk(hard_state_mu_);
  return storage_->SaveHardState(hs);
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
    constexpr uint32_t kMaxInboundRpcConnections = 64;
    const uint32_t previous = inbound_rpc_connections_.fetch_add(1);
    if (previous >= kMaxInboundRpcConnections) {
      inbound_rpc_connections_.fetch_sub(1);
      (void)platform::CloseSocket(client_fd);
      continue;
    }
    (void)platform::SetReceiveTimeout(client_fd, 1000);
    (void)platform::SetSendTimeout(client_fd, 1000);
    inbound_rpc_pool_->Execute([this, client_fd]() {
      HandleRpcConnection(client_fd);
      (void)platform::CloseSocket(client_fd);
      inbound_rpc_connections_.fetch_sub(1);
    });
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

    case raft::RaftMsgType::kInstallSnapshot: {
      raft::InstallSnapshotArgs args;
      if (!raft::DecodeInstallSnapshot(body.data(), body.size(), &args)) return;

      raft::InstallSnapshotReply reply;
      {
        std::unique_lock<std::mutex> lk(raft_mu_);
        reply.term = raft_node_->current_term();
        if (args.term < reply.term) {
          reply.success = false;
        } else if (!raft_node_->PrepareInstallSnapshot(args)) {
          reply.term = raft_node_->current_term();
          reply.success = true;
          reply.match_index = raft_node_->SnapshotMeta().last_included_index;
        } else {
          Status install_status = InstallSnapshotArchive(args);
          if (install_status.ok()) {
            install_status = raft_node_->RestoreSnapshot(args.meta);
          }
          if (install_status.ok()) {
            last_applied_ = std::max(last_applied_, args.meta.last_included_index);
            reply.success = true;
            reply.match_index = args.meta.last_included_index;
          } else {
            reply.success = false;
          }
          reply.term = raft_node_->current_term();
        }
      }
      PersistHardState();
      std::string reply_body = raft::EncodeInstallSnapshotReply(reply);
      std::string reply_msg = raft::EncodeMessage(
          raft::RaftMsgType::kInstallSnapshotReply, reply_body);
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

void RaftServer::SendInstallSnapshot(uint64_t to,
                                     const raft::RaftSnapshotMeta& meta) {
  auto it = config_.peers.find(to);
  if (it == config_.peers.end()) return;

  std::string archive;
  Status archive_status =
      BuildSnapshotArchive(SnapshotDirectory(meta), &archive);
  if (!archive_status.ok()) return;

  raft::InstallSnapshotArgs args;
  {
    std::lock_guard<std::mutex> lk(raft_mu_);
    args.term = raft_node_->current_term();
    args.leader_id = config_.node_id;
  }
  args.meta = meta;
  args.data = std::move(archive);

  const std::string body = raft::EncodeInstallSnapshot(args);
  const std::string msg = raft::EncodeMessage(
      raft::RaftMsgType::kInstallSnapshot, body);

  platform::SocketHandle fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (!platform::IsValidSocket(fd)) return;

  (void)platform::SetSendTimeout(fd, 3000);
  (void)platform::SetReceiveTimeout(fd, 3000);

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

  const std::string reply_framed = raft::ReadMessage(fd);
  (void)platform::CloseSocket(fd);
  if (reply_framed.empty()) return;

  raft::RaftMsgType reply_type;
  std::string reply_body;
  if (!raft::DecodeMessage(reply_framed.data(), reply_framed.size(),
                           &reply_type, &reply_body) ||
      reply_type != raft::RaftMsgType::kInstallSnapshotReply) {
    return;
  }

  raft::InstallSnapshotReply reply;
  if (!raft::DecodeInstallSnapshotReply(reply_body.data(), reply_body.size(),
                                        &reply)) {
    return;
  }
  {
    std::lock_guard<std::mutex> lk(raft_mu_);
    raft_node_->HandleInstallSnapshotReply(to, reply);
  }
  PersistHardState();
}

}  // namespace kv
