#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "server_config.h"

#include "kv/engine/db.h"
#include "kv/engine/write_applier.h"
#include "kv/cluster/cluster_manager.h"
#include "kv/net/server.h"
#include "kv/net/monitoring_server.h"
#include "kv/raft/raft_db_adapter.h"
#include "kv/raft/raft_server.h"

namespace {

std::atomic<bool> g_stop{false};

void OnSignal(int) {
  g_stop.store(true, std::memory_order_relaxed);
}

}  // namespace

int main(int argc, char** argv) {
  std::string config_path = "config/server.yaml";
  if (argc >= 2 && std::string(argv[1]).rfind("--config=", 0) == 0) {
    config_path = std::string(argv[1]).substr(9);
  } else if (const char* v = std::getenv("KV_CONFIG"); v != nullptr &&
             !std::string(v).empty()) {
    config_path = v;
  }

  kv::app::AppConfigFile file_config;
  std::string config_error;
  if (auto loaded = kv::app::LoadConfigFile(config_path, &config_error); loaded.has_value()) {
    file_config = *loaded;
  } else if (!config_error.empty()) {
    std::cerr << "invalid config file " << config_path << ": "
              << config_error << "\n";
    return 1;
  }

  int port = file_config.server_port;
  int metrics_port = file_config.metrics_port;
  std::string db_path = file_config.db_path;
  int arg_index = 1;
  if (argc >= 2 && std::string(argv[1]).rfind("--config=", 0) == 0) {
    arg_index = 2;
  }

  if (argc > arg_index) {
    port = kv::app::ParsePort(argv[arg_index]);
    if (port < 0) {
      std::cerr << "invalid port: " << argv[arg_index] << "\n";
      return 1;
    }
  }

  if (argc > arg_index + 1) {
    db_path = argv[arg_index + 1];
  }

  if (const char* v = std::getenv("KV_METRICS_PORT"); v != nullptr) {
    const int parsed = kv::app::ParseOptionalPort(v);
    if (parsed >= 0) metrics_port = parsed;
  }

  // ---- Raft config from file/env ----
  std::unique_ptr<kv::RaftServer> raft_server;
  auto raft_config = std::make_unique<kv::RaftConfig>(file_config.raft);
  bool raft_enabled = file_config.raft_enabled;

  if (const char* v = std::getenv("KV_RAFT"); v != nullptr) {
    const std::string s(v);
    if (s == "1" || s == "true" || s == "TRUE") {
      raft_enabled = true;
    }
  }

  std::vector<kv::NodeInfo> cluster_nodes = file_config.cluster_nodes;
  std::string cluster_local_node_id = file_config.cluster_local_node_id;
  if (const char* v = std::getenv("KV_CLUSTER_NODES"); v != nullptr &&
      !std::string(v).empty()) {
    cluster_nodes = kv::app::ParseClusterNodes(v);
  }
  if (const char* v = std::getenv("KV_CLUSTER_LOCAL_NODE_ID"); v != nullptr &&
      !std::string(v).empty()) {
    cluster_local_node_id = v;
  }

  if (raft_enabled) {
    if (const char* v = std::getenv("KV_RAFT_NODE_ID"); v != nullptr) {
      raft_config->node_id = kv::app::ParseNodeId(v);
    }
    if (const char* v = std::getenv("KV_RAFT_PORT"); v != nullptr) {
      int p = kv::app::ParsePort(v);
      if (p > 0) raft_config->raft_port = static_cast<uint16_t>(p);
    }
    if (const char* v = std::getenv("KV_RAFT_PEERS"); v != nullptr) {
      raft_config->peers = kv::app::ParsePeers(v);
    }
    if (const char* v = std::getenv("KV_RAFT_DATA_DIR"); v != nullptr) {
      raft_config->data_dir = v;
    }

    raft_config->client_port = static_cast<uint16_t>(port);

    // Default raft_port = client_port + 1 if not explicitly set
    if (raft_config->raft_port == 0) {
      raft_config->raft_port = static_cast<uint16_t>(port + 1);
    }
  }

  auto cluster_manager = std::make_unique<kv::ClusterManager>(8);
  kv::app::EnsureSelfNode(&cluster_nodes, "local", "127.0.0.1",
                 static_cast<uint16_t>(port));
  for (const auto& node : cluster_nodes) {
    (void)cluster_manager->AddNode(node);
  }
  if (cluster_manager->NodeCount() == 0) {
    (void)cluster_manager->AddNode(kv::NodeInfo{
        "local", "127.0.0.1", static_cast<uint16_t>(port), 1, true});
  }
  if (!cluster_local_node_id.empty()) {
    cluster_manager->SetLocalNodeId(cluster_local_node_id);
  }

  // ---- Open DB ----
  kv::DBOptions options;
  options.db_path = db_path;
  options.sync_on_write = file_config.sync_on_write;
  options.memtable_write_buffer_size = file_config.memtable_write_buffer_size;
  options.compaction_min_input_files = file_config.compaction_min_input_files;
  options.auto_compaction_enabled = file_config.auto_compaction_enabled;
  options.cache_enabled = file_config.cache_enabled;
  options.cache_policy = file_config.cache_policy;
  options.cache_capacity = file_config.cache_capacity;
  options.cache_default_ttl_ms = file_config.cache_ttl_ms;

  // Cache config from env
  if (const char* v = std::getenv("KV_CACHE"); v != nullptr) {
    const std::string s(v);
    if (!s.empty() && s != "0" && s != "false" && s != "FALSE") {
      options.cache_enabled = true;
    }
  }
  if (const char* v = std::getenv("KV_CACHE_POLICY"); v != nullptr) {
    (void)kv::app::ParseCachePolicyValue(v, &options.cache_policy);
  }
  if (const char* v = std::getenv("KV_CACHE_CAPACITY"); v != nullptr) {
    char* end = nullptr;
    const long long cap = std::strtoll(v, &end, 10);
    if (end != v && *end == '\0' && cap >= 0) {
      options.cache_capacity = static_cast<size_t>(cap);
    }
  }
  if (const char* v = std::getenv("KV_CACHE_TTL_MS"); v != nullptr) {
    char* end = nullptr;
    const long long ttl = std::strtoll(v, &end, 10);
    if (end != v && *end == '\0') {
      options.cache_default_ttl_ms = static_cast<int64_t>(ttl);
    }
  }
  if (const char* v = std::getenv("KV_SYNC_ON_WRITE"); v != nullptr) {
    bool parsed = false;
    if (kv::app::ParseBoolValue(v, &parsed)) options.sync_on_write = parsed;
  }
  if (const char* v = std::getenv("KV_MEMTABLE_WRITE_BUFFER_SIZE");
      v != nullptr) {
    size_t parsed = 0;
    if (kv::app::ParseSizeValue(v, &parsed)) {
      options.memtable_write_buffer_size = parsed;
    }
  }
  if (const char* v = std::getenv("KV_COMPACTION_MIN_INPUT_FILES");
      v != nullptr) {
    size_t parsed = 0;
    if (kv::app::ParseSizeValue(v, &parsed)) {
      options.compaction_min_input_files = parsed;
    }
  }
  if (const char* v = std::getenv("KV_AUTO_COMPACTION"); v != nullptr) {
    bool parsed = true;
    if (kv::app::ParseBoolValue(v, &parsed)) options.auto_compaction_enabled = parsed;
  }

  std::unique_ptr<kv::DB> db;
  kv::Status s = kv::DB::Open(options, &db);
  if (!s.ok()) {
    std::cerr << "open db failed: " << s.ToString() << "\n";
    return 1;
  }

  // ---- Start Raft if enabled ----
  if (raft_enabled && raft_config) {
    auto* applier = dynamic_cast<kv::WriteApplier*>(db.get());
    if (applier == nullptr) {
      std::cerr << "database does not support replicated writes\n";
      return 1;
    }
    raft_server = std::make_unique<kv::RaftServer>(*raft_config, applier);
    s = raft_server->Start();
    if (!s.ok()) {
      std::cerr << "raft server start failed: " << s.ToString() << "\n";
      return 1;
    }
    std::cout << "raft server started: node_id=" << raft_config->node_id
              << " raft_port=" << raft_config->raft_port << "\n";
  }

  // ---- Start client server ----
  std::unique_ptr<kv::RaftDBAdapter> raft_db;
  kv::DB* server_db = db.get();

  if (raft_server) {
    raft_db = std::make_unique<kv::RaftDBAdapter>(db.get(), raft_server.get());
    server_db = raft_db.get();
  }

  kv::net::Server server;
  s = server.Start(static_cast<uint16_t>(port), server_db, cluster_manager.get());
  if (!s.ok()) {
    std::cerr << "server start failed: " << s.ToString() << "\n";
    return 1;
  }

  kv::net::MonitoringServer monitoring_server;
  if (metrics_port > 0) {
    s = monitoring_server.Start(static_cast<uint16_t>(metrics_port), &server,
                                db.get());
    if (!s.ok()) {
      std::cerr << "monitoring server start failed: " << s.ToString() << "\n";
      (void)server.Stop();
      if (raft_server) (void)raft_server->Stop();
      (void)db->Close();
      return 1;
    }
  }

  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  std::cout << "kv_server listening on 0.0.0.0:" << port
            << " db_path=" << db_path;
  if (monitoring_server.IsRunning()) {
    std::cout << " metrics_port=" << monitoring_server.port();
  }
  if (raft_enabled) {
    std::cout << " raft=yes";
  }
  std::cout << "\n";

  auto last_report = std::chrono::steady_clock::now();
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const auto now = std::chrono::steady_clock::now();
    if (now - last_report >= std::chrono::seconds(5)) {
      const kv::net::ServerStats stats = server.GetStats();
      std::cout << "[health] running=" << (server.IsRunning() ? "yes" : "no")
                << " port=" << server.port()
                << " active_connections=" << stats.active_connections
                << " total_connections=" << stats.total_connections
                << " total_requests=" << stats.total_requests
                << " request_errors=" << stats.request_errors
                << " response_bytes=" << stats.response_bytes
                << " request_duration_us=" << stats.request_duration_us
                << " txn_begin=" << stats.txn_begin
                << " txn_commit=" << stats.txn_commit
                << " txn_abort=" << stats.txn_abort
                << " txn_conflict=" << stats.txn_conflict;

      if (raft_server) {
        std::cout << " raft_role="
                  << (raft_server->IsLeader() ? "leader" : "follower")
                  << " raft_leader=" << raft_server->LeaderId();
      }

      if (options.cache_enabled) {
        kv::CacheStats cache_stats;
        if (db->GetCacheStats(&cache_stats).ok()) {
          std::cout << " cache_hit=" << cache_stats.hit
                    << " cache_miss=" << cache_stats.miss
                    << " cache_evict=" << cache_stats.evict
                    << " cache_expire=" << cache_stats.expire;
        }
      }

      const kv::ClusterStatus cluster_status = cluster_manager->GetStatus();
      std::cout << " cluster_nodes=" << cluster_status.node_count
                << " cluster_active_nodes=" << cluster_status.active_node_count;
      std::cout << "\n";
      last_report = now;
    }
  }

  (void)monitoring_server.Stop();
  (void)server.Stop();
  if (raft_server) (void)raft_server->Stop();
  (void)db->Close();
  return 0;
}
