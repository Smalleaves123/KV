#include "kv/net/monitoring_server.h"

#include <sstream>
#include <string_view>
#include <utility>

#include "kv/engine/db.h"
#include "kv/net/command.h"
#include "kv/net/server.h"
#include "kv/raft/raft_server.h"

namespace kv::net {
namespace {

struct HttpResponse {
  int status = 200;
  const char* status_text = "OK";
  const char* content_type = "text/plain; version=0.0.4";
  std::string body;
};

HttpResponse MakeResponse(int status, const char* status_text,
                          const char* content_type, std::string body) {
  return HttpResponse{status, status_text, content_type, std::move(body)};
}

std::string EncodeHttpResponse(const HttpResponse& response) {
  std::ostringstream out;
  out << "HTTP/1.1 " << response.status << " " << response.status_text
      << "\r\nContent-Type: " << response.content_type
      << "\r\nContent-Length: " << response.body.size()
      << "\r\nConnection: close\r\n\r\n" << response.body;
  return out.str();
}

bool ParseRequestPath(const std::string& request, std::string* path) {
  if (path == nullptr) return false;
  const size_t line_end = request.find("\r\n");
  if (line_end == std::string::npos) return false;
  const std::string_view line(request.data(), line_end);
  if (line.rfind("GET ", 0) != 0) return false;
  const size_t path_begin = 4;
  const size_t path_end = line.find(' ', path_begin);
  if (path_end == std::string_view::npos || path_end == path_begin) {
    return false;
  }
  *path = std::string(line.substr(path_begin, path_end - path_begin));
  return true;
}

std::string BoolJson(bool value) { return value ? "true" : "false"; }

const char* CommandMetricName(CommandType type) {
  switch (type) {
    case CommandType::kPing:
      return "PING";
    case CommandType::kGet:
      return "GET";
    case CommandType::kSet:
      return "SET";
    case CommandType::kDel:
      return "DEL";
    case CommandType::kExpire:
      return "EXPIRE";
    case CommandType::kTTL:
      return "TTL";
    case CommandType::kPersist:
      return "PERSIST";
    case CommandType::kMGet:
      return "MGET";
    case CommandType::kInfo:
      return "INFO";
    case CommandType::kStats:
      return "STATS";
    case CommandType::kCluster:
      return "CLUSTER";
    case CommandType::kBegin:
      return "BEGIN";
    case CommandType::kExec:
      return "EXEC";
    case CommandType::kAbort:
      return "ABORT";
    case CommandType::kScan:
      return "SCAN";
    case CommandType::kIncr:
      return "INCR";
    case CommandType::kIncrBy:
      return "INCRBY";
    case CommandType::kDecr:
      return "DECR";
    case CommandType::kDecrBy:
      return "DECRBY";
    case CommandType::kHSet:
      return "HSET";
    case CommandType::kHGet:
      return "HGET";
    case CommandType::kHDel:
      return "HDEL";
    case CommandType::kHExists:
      return "HEXISTS";
    case CommandType::kHLen:
      return "HLEN";
    case CommandType::kHGetAll:
      return "HGETALL";
    case CommandType::kHKeys:
      return "HKEYS";
    case CommandType::kHVals:
      return "HVALS";
    case CommandType::kLPush:
      return "LPUSH";
    case CommandType::kRPush:
      return "RPUSH";
    case CommandType::kLPop:
      return "LPOP";
    case CommandType::kRPop:
      return "RPOP";
    case CommandType::kLLen:
      return "LLEN";
    case CommandType::kLIndex:
      return "LINDEX";
    case CommandType::kLRange:
      return "LRANGE";
    case CommandType::kAuth:
      return "AUTH";
    case CommandType::kInvalid:
      return "INVALID";
  }
  return "INVALID";
}

}  // namespace

MonitoringServer::MonitoringServer()
    : listen_fd_(platform::kInvalidSocket),
      port_(0),
      running_(false),
      server_(nullptr),
      db_(nullptr),
      accept_thread_(),
      pool_(std::make_unique<ThreadPool>(2)),
      socket_runtime_() {}

MonitoringServer::~MonitoringServer() { (void)Stop(); }

Status MonitoringServer::Start(uint16_t port, const Server* server,
                               const DB* db) {
  if (server == nullptr || db == nullptr) {
    return Status::InvalidArgument("monitoring dependencies are null");
  }
  if (running_.load()) {
    return Status::AlreadyExists("monitoring server already running");
  }
  if (!socket_runtime_.Start()) {
    return Status::IOError("failed to initialize monitoring sockets");
  }

  Status s = SetupListenSocket(port);
  if (!s.ok()) {
    socket_runtime_.Stop();
    return s;
  }

  server_ = server;
  db_ = db;
  pool_ = std::make_unique<ThreadPool>(2);
  running_.store(true);
  accept_thread_ = std::thread(&MonitoringServer::AcceptLoop, this);
  return Status::OK();
}

Status MonitoringServer::Stop() {
  running_.store(false);
  if (platform::IsValidSocket(listen_fd_)) {
    (void)platform::ShutdownSocket(listen_fd_);
    (void)platform::CloseSocket(listen_fd_);
    listen_fd_ = platform::kInvalidSocket;
  }
  if (accept_thread_.joinable()) accept_thread_.join();
  if (pool_ != nullptr) pool_->WaitAndStop();
  socket_runtime_.Stop();
  server_ = nullptr;
  db_ = nullptr;
  return Status::OK();
}

bool MonitoringServer::IsRunning() const noexcept { return running_.load(); }

uint16_t MonitoringServer::port() const noexcept { return port_; }

std::string MonitoringServer::RenderHealth(const Server& server, const DB& db,
                                           bool readiness) {
  const bool server_running = server.IsRunning();
  const bool db_open = db.IsOpen();
  const bool ready = server_running && db_open;
  std::ostringstream out;
  out << "{\"status\":\"" << ((readiness ? ready : server_running)
                                      ? "ok"
                                      : "not_ready")
      << "\",\"server_running\":" << BoolJson(server_running)
      << ",\"db_open\":" << BoolJson(db_open)
      << ",\"ready\":" << BoolJson(ready) << "}\n";
  return out.str();
}

std::string MonitoringServer::RenderMetrics(const Server& server,
                                            const DB& db) {
  const ServerStats server_stats = server.GetStats();
  CacheStats cache_stats;
  ReadPathStats read_stats;
  CompactionStats compaction_stats;
  FlushStats flush_stats;
  const bool cache_ok = db.GetCacheStats(&cache_stats).ok();
  const bool read_ok = db.GetReadPathStats(&read_stats).ok();
  const bool compaction_ok = db.GetCompactionStats(&compaction_stats).ok();
  const bool flush_ok = db.GetFlushStats(&flush_stats).ok();
  RaftStats raft_stats;
  const bool raft_ok = server.GetRaftStats(&raft_stats);

  std::ostringstream out;
  out << "# HELP kv_server_up Whether the command server is running.\n"
      << "# TYPE kv_server_up gauge\n"
      << "kv_server_up " << (server.IsRunning() ? 1 : 0) << "\n"
      << "# HELP kv_server_ready Whether the DB and command server are ready.\n"
      << "# TYPE kv_server_ready gauge\n"
      << "kv_server_ready " << (server.IsRunning() && db.IsOpen() ? 1 : 0)
      << "\n"
      << "# HELP kv_server_port The command server TCP port.\n"
      << "# TYPE kv_server_port gauge\n"
      << "kv_server_port " << server.port() << "\n"
      << "# HELP kv_server_total_connections_total Total accepted client connections.\n"
      << "# TYPE kv_server_total_connections_total counter\n"
      << "kv_server_total_connections_total " << server_stats.total_connections
      << "\n"
      << "# HELP kv_server_active_connections Current active client connections.\n"
      << "# TYPE kv_server_active_connections gauge\n"
      << "kv_server_active_connections " << server_stats.active_connections
      << "\n"
      << "# HELP kv_server_requests_total Total client requests.\n"
      << "# TYPE kv_server_requests_total counter\n"
      << "kv_server_requests_total " << server_stats.total_requests << "\n"
      << "# HELP kv_server_request_errors_total Total protocol or command errors.\n"
      << "# TYPE kv_server_request_errors_total counter\n"
      << "kv_server_request_errors_total " << server_stats.request_errors
      << "\n"
      << "# HELP kv_server_response_bytes_total Total response bytes sent.\n"
      << "# TYPE kv_server_response_bytes_total counter\n"
      << "kv_server_response_bytes_total " << server_stats.response_bytes
      << "\n"
      << "# HELP kv_server_request_duration_microseconds_total Cumulative request handling time.\n"
      << "# TYPE kv_server_request_duration_microseconds_total counter\n"
      << "kv_server_request_duration_microseconds_total "
      << server_stats.request_duration_us << "\n"
      << "# HELP kv_server_request_duration_microseconds_count Requests included in the cumulative duration.\n"
      << "# TYPE kv_server_request_duration_microseconds_count counter\n"
      << "kv_server_request_duration_microseconds_count "
      << server_stats.total_requests << "\n"
      << "# HELP kv_server_transactions_total Transaction lifecycle events.\n"
      << "# TYPE kv_server_transactions_total counter\n"
      << "kv_server_transactions_total{state=\"begin\"} "
      << server_stats.txn_begin << "\n"
      << "kv_server_transactions_total{state=\"commit\"} "
      << server_stats.txn_commit << "\n"
      << "kv_server_transactions_total{state=\"abort\"} "
      << server_stats.txn_abort << "\n"
      << "kv_server_transactions_total{state=\"conflict\"} "
      << server_stats.txn_conflict << "\n"
      << "# HELP kv_server_commands_total Requests by command.\n"
      << "# TYPE kv_server_commands_total counter\n";
  for (size_t i = 0; i < kCommandTypeCount; ++i) {
    const auto type = static_cast<CommandType>(i);
    out << "kv_server_commands_total{command=\""
        << CommandMetricName(type) << "\"} " << server_stats.command_requests[i]
        << "\n";
  }
  out << "# HELP kv_server_command_errors_total Errors by command.\n"
      << "# TYPE kv_server_command_errors_total counter\n";
  for (size_t i = 0; i < kCommandTypeCount; ++i) {
    const auto type = static_cast<CommandType>(i);
    out << "kv_server_command_errors_total{command=\""
        << CommandMetricName(type) << "\"} " << server_stats.command_errors[i]
        << "\n";
  }
  out << "# HELP kv_db_up Whether DB statistics can be collected.\n"
      << "# TYPE kv_db_up gauge\n"
      << "kv_db_up "
      << (db.IsOpen() && cache_ok && read_ok && compaction_ok && flush_ok ? 1 : 0)
      << "\n";

  if (cache_ok) {
    out << "# HELP kv_db_cache_hits_total Value cache hits.\n"
        << "# TYPE kv_db_cache_hits_total counter\n"
        << "kv_db_cache_hits_total " << cache_stats.hit << "\n"
        << "# HELP kv_db_cache_misses_total Value cache misses.\n"
        << "# TYPE kv_db_cache_misses_total counter\n"
        << "kv_db_cache_misses_total " << cache_stats.miss << "\n"
        << "# HELP kv_db_cache_evictions_total Value cache evictions.\n"
        << "# TYPE kv_db_cache_evictions_total counter\n"
        << "kv_db_cache_evictions_total " << cache_stats.evict << "\n"
        << "# HELP kv_db_cache_expirations_total Value cache expirations.\n"
        << "# TYPE kv_db_cache_expirations_total counter\n"
        << "kv_db_cache_expirations_total " << cache_stats.expire << "\n";
  }
  if (read_ok) {
    out << "# TYPE kv_db_table_cache_hits_total counter\n"
        << "kv_db_table_cache_hits_total " << read_stats.table_cache_hits
        << "\n"
        << "# TYPE kv_db_table_cache_misses_total counter\n"
        << "kv_db_table_cache_misses_total " << read_stats.table_cache_misses
        << "\n"
        << "# TYPE kv_db_table_cache_evictions_total counter\n"
        << "kv_db_table_cache_evictions_total "
        << read_stats.table_cache_evictions << "\n"
        << "# TYPE kv_db_table_cache_entries gauge\n"
        << "kv_db_table_cache_entries " << read_stats.table_cache_entries << "\n"
        << "# TYPE kv_db_bloom_queries_total counter\n"
        << "kv_db_bloom_queries_total " << read_stats.bloom_queries << "\n"
        << "# TYPE kv_db_bloom_negatives_total counter\n"
        << "kv_db_bloom_negatives_total " << read_stats.bloom_negatives << "\n";
  }
  if (compaction_ok) {
    out << "# TYPE kv_db_compaction_attempts_total counter\n"
        << "kv_db_compaction_attempts_total "
        << compaction_stats.trigger_attempts << "\n"
        << "# TYPE kv_db_compaction_success_total counter\n"
        << "kv_db_compaction_success_total " << compaction_stats.succeeded
        << "\n"
        << "# TYPE kv_db_compaction_failures_total counter\n"
        << "kv_db_compaction_failures_total " << compaction_stats.failed << "\n"
        << "# TYPE kv_db_compaction_skipped_total counter\n"
        << "kv_db_compaction_skipped_total{reason=\"snapshot\"} "
        << compaction_stats.skipped_due_snapshot << "\n"
        << "kv_db_compaction_skipped_total{reason=\"threshold\"} "
        << compaction_stats.skipped_due_threshold << "\n";
  }
  if (flush_ok) {
    out << "# TYPE kv_db_flush_completed_total counter\n"
        << "kv_db_flush_completed_total " << flush_stats.completed << "\n"
        << "# TYPE kv_db_flush_failures_total counter\n"
        << "kv_db_flush_failures_total " << flush_stats.failed << "\n"
        << "# TYPE kv_db_flush_duration_microseconds_total counter\n"
        << "kv_db_flush_duration_microseconds_total "
        << flush_stats.total_duration_us << "\n"
        << "# TYPE kv_db_flush_queue_length gauge\n"
        << "kv_db_flush_queue_length " << flush_stats.queue_length << "\n"
        << "# TYPE kv_db_write_stalls_total counter\n"
        << "kv_db_write_stalls_total " << flush_stats.write_stalls << "\n"
        << "# TYPE kv_db_write_stall_duration_microseconds_total counter\n"
        << "kv_db_write_stall_duration_microseconds_total "
        << flush_stats.write_stall_duration_us << "\n";
  }
  if (raft_ok) {
    out << "# TYPE kv_raft_role gauge\n"
        << "kv_raft_role{role=\"leader\"} "
        << (raft_stats.is_leader ? 1 : 0) << "\n"
        << "kv_raft_role{role=\"follower\"} "
        << (raft_stats.is_leader ? 0 : 1) << "\n"
        << "# TYPE kv_raft_term gauge\n"
        << "kv_raft_term " << raft_stats.term << "\n"
        << "# TYPE kv_raft_voted_for gauge\n"
        << "kv_raft_voted_for " << raft_stats.voted_for << "\n"
        << "# TYPE kv_raft_leader_id gauge\n"
        << "kv_raft_leader_id " << raft_stats.leader_id << "\n"
        << "# TYPE kv_raft_commit_index gauge\n"
        << "kv_raft_commit_index " << raft_stats.commit_index << "\n"
        << "# TYPE kv_raft_applied_index gauge\n"
        << "kv_raft_applied_index " << raft_stats.applied_index << "\n"
        << "# TYPE kv_raft_last_log_index gauge\n"
        << "kv_raft_last_log_index " << raft_stats.last_log_index << "\n"
        << "# TYPE kv_raft_snapshot_last_included_index gauge\n"
        << "kv_raft_snapshot_last_included_index "
        << raft_stats.snapshot_last_included_index << "\n";
    out << "# TYPE kv_raft_peer_match_index gauge\n"
        << "# TYPE kv_raft_peer_next_index gauge\n"
        << "# TYPE kv_raft_peer_replication_lag gauge\n";
    for (const auto& peer : raft_stats.peers) {
      const std::string peer_id = std::to_string(peer.peer_id);
      const uint64_t lag = raft_stats.last_log_index > peer.match_index
                               ? raft_stats.last_log_index - peer.match_index
                               : 0;
      out << "kv_raft_peer_match_index{peer_id=\"" << peer_id << "\"} "
          << peer.match_index << "\n"
          << "kv_raft_peer_next_index{peer_id=\"" << peer_id << "\"} "
          << peer.next_index << "\n"
          << "kv_raft_peer_replication_lag{peer_id=\"" << peer_id << "\"} "
          << lag << "\n";
    }
  }
  return out.str();
}

Status MonitoringServer::SetupListenSocket(uint16_t port) {
  const platform::SocketHandle fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (!platform::IsValidSocket(fd)) {
    return Status::IOError("monitoring socket failed: " +
                           platform::SocketErrorString(
                               platform::LastSocketError()));
  }
  (void)platform::SetSocketOptionInt(fd, SOL_SOCKET, SO_REUSEADDR, 1);

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(port);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0 ||
      ::listen(fd, 32) < 0) {
    const std::string error = platform::SocketErrorString(
        platform::LastSocketError());
    (void)platform::CloseSocket(fd);
    return Status::IOError("monitoring listen failed: " + error);
  }

  sockaddr_in bound_addr{};
  platform::SocketLength bound_len = sizeof(bound_addr);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&bound_addr), &bound_len) <
      0) {
    const std::string error = platform::SocketErrorString(
        platform::LastSocketError());
    (void)platform::CloseSocket(fd);
    return Status::IOError("monitoring getsockname failed: " + error);
  }
  listen_fd_ = fd;
  port_ = ntohs(bound_addr.sin_port);
  return Status::OK();
}

void MonitoringServer::AcceptLoop() {
  while (running_.load()) {
    sockaddr_in peer{};
    platform::SocketLength peer_len = sizeof(peer);
    const platform::SocketHandle client_fd = ::accept(
        listen_fd_, reinterpret_cast<sockaddr*>(&peer), &peer_len);
    if (!platform::IsValidSocket(client_fd)) {
      if (!running_.load()) break;
      if (platform::IsInterruptedSocketError(platform::LastSocketError())) {
        continue;
      }
      continue;
    }
    pool_->Execute([client_fd, this]() {
      HandleClient(client_fd, server_, db_, &running_);
    });
  }
}

void MonitoringServer::HandleClient(platform::SocketHandle client_fd,
                                    const Server* server, const DB* db,
                                    std::atomic<bool>* running) {
  (void)platform::SetReceiveTimeout(client_fd, 1000);
  std::string request;
  char buffer[1024];
  while (request.find("\r\n\r\n") == std::string::npos &&
         request.size() < 8192 && running->load()) {
    const platform::SocketIoResult received =
        platform::ReceiveSocket(client_fd, buffer, sizeof(buffer), 0);
    if (received <= 0) break;
    request.append(buffer, static_cast<size_t>(received));
  }

  HttpResponse response;
  std::string path;
  if (server == nullptr || db == nullptr || !ParseRequestPath(request, &path)) {
    response = MakeResponse(400, "Bad Request", "text/plain", "bad request\n");
  } else if (path == "/health" || path == "/healthz") {
    const bool ready = server->IsRunning() && db->IsOpen();
    response = MakeResponse(ready ? 200 : 503,
                            ready ? "OK" : "Service Unavailable",
                            "application/json",
                            RenderHealth(*server, *db, false));
  } else if (path == "/ready" || path == "/readyz") {
    const bool ready = server->IsRunning() && db->IsOpen();
    response = MakeResponse(ready ? 200 : 503,
                            ready ? "OK" : "Service Unavailable",
                            "application/json",
                            RenderHealth(*server, *db, true));
  } else if (path == "/metrics") {
    response = MakeResponse(200, "OK", "text/plain; version=0.0.4",
                            RenderMetrics(*server, *db));
  } else {
    response = MakeResponse(404, "Not Found", "text/plain", "not found\n");
  }

  const std::string encoded = EncodeHttpResponse(response);
  size_t written = 0;
  while (written < encoded.size()) {
    const platform::SocketIoResult sent = platform::SendSocket(
        client_fd, encoded.data() + written, encoded.size() - written, 0);
    if (sent <= 0) break;
    written += static_cast<size_t>(sent);
  }
  (void)platform::ShutdownSocket(client_fd);
  (void)platform::CloseSocket(client_fd);
}

}  // namespace kv::net
