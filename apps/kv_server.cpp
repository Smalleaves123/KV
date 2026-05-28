#include <atomic>
#include <chrono>
#include <csignal>
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

#include "kv/engine/db.h"
#include "kv/net/server.h"
#include "kv/raft/raft_rpc_codec.h"
#include "kv/raft/raft_server.h"

namespace {

std::atomic<bool> g_stop{false};

struct AppConfigFile {
  int server_port = 9527;
  std::string db_path = "data/db";
  bool raft_enabled = false;
  kv::RaftConfig raft;
  bool cache_enabled = false;
  size_t cache_capacity = 1024;
  int64_t cache_ttl_ms = 0;
};

void OnSignal(int) {
  g_stop.store(true, std::memory_order_relaxed);
}

int ParsePort(const char* s) {
  if (s == nullptr) return -1;
  char* end = nullptr;
  const long v = std::strtol(s, &end, 10);
  if (end == s || *end != '\0' || v <= 0 || v > 65535) return -1;
  return static_cast<int>(v);
}

uint64_t ParseNodeId(const char* s) {
  if (s == nullptr) return 0;
  char* end = nullptr;
  const long long v = std::strtoll(s, &end, 10);
  if (end == s || *end != '\0' || v <= 0) return 0;
  return static_cast<uint64_t>(v);
}

std::string Trim(const std::string& s) {
  const size_t begin = s.find_first_not_of(" \t\r\n");
  if (begin == std::string::npos) return {};
  const size_t end = s.find_last_not_of(" \t\r\n");
  return s.substr(begin, end - begin + 1);
}

std::string StripComment(const std::string& s) {
  const size_t pos = s.find('#');
  return Trim(pos == std::string::npos ? s : s.substr(0, pos));
}

bool ParseBoolValue(const std::string& s, bool* value) {
  if (value == nullptr) return false;
  if (s == "true" || s == "TRUE" || s == "1") {
    *value = true;
    return true;
  }
  if (s == "false" || s == "FALSE" || s == "0") {
    *value = false;
    return true;
  }
  return false;
}

bool ParseSizeValue(const std::string& s, size_t* value) {
  if (value == nullptr) return false;
  char* end = nullptr;
  const unsigned long long parsed = std::strtoull(s.c_str(), &end, 10);
  if (end == s.c_str() || *end != '\0') return false;
  *value = static_cast<size_t>(parsed);
  return true;
}

bool ParseInt64Value(const std::string& s, int64_t* value) {
  if (value == nullptr) return false;
  char* end = nullptr;
  const long long parsed = std::strtoll(s.c_str(), &end, 10);
  if (end == s.c_str() || *end != '\0') return false;
  *value = static_cast<int64_t>(parsed);
  return true;
}

bool ParseIntValue(const std::string& s, int* value) {
  if (value == nullptr) return false;
  const int parsed = ParsePort(s.c_str());
  if (parsed < 0) return false;
  *value = parsed;
  return true;
}

std::optional<AppConfigFile> LoadConfigFile(const std::string& path,
                                            std::string* error) {
  std::ifstream in(path);
  if (!in.is_open()) {
    return std::nullopt;
  }

  AppConfigFile cfg;
  enum class Section { kNone, kServer, kRaft, kCache, kRaftPeers };
  Section section = Section::kNone;
  kv::RaftConfig::Peer current_peer;
  uint64_t current_peer_id = 0;
  bool in_peer = false;

  auto flush_peer = [&]() {
    if (in_peer && current_peer_id > 0 && current_peer.raft_port > 0 &&
        !current_peer.host.empty()) {
      cfg.raft.peers[current_peer_id] = current_peer;
    }
    current_peer = kv::RaftConfig::Peer{};
    current_peer_id = 0;
    in_peer = false;
  };

  std::string line;
  int lineno = 0;
  while (std::getline(in, line)) {
    ++lineno;
    const size_t indent = line.find_first_not_of(' ');
    const std::string cleaned = StripComment(line);
    if (cleaned.empty()) continue;

    if (indent == 0) {
      flush_peer();
      if (cleaned == "server:") {
        section = Section::kServer;
      } else if (cleaned == "raft:") {
        section = Section::kRaft;
      } else if (cleaned == "cache:") {
        section = Section::kCache;
      } else {
        section = Section::kNone;
      }
      continue;
    }

    if (section == Section::kRaft && cleaned == "peers:") {
      flush_peer();
      section = Section::kRaftPeers;
      continue;
    }

    if (section == Section::kRaftPeers) {
      if (indent == 4 && cleaned.rfind("- ", 0) == 0) {
        flush_peer();
        in_peer = true;
        const std::string rest = Trim(cleaned.substr(2));
        if (!rest.empty()) {
          const size_t colon = rest.find(':');
          if (colon == std::string::npos) {
            if (error != nullptr) {
              *error = "invalid raft peer entry at line " +
                       std::to_string(lineno);
            }
            return std::nullopt;
          }
          const std::string key = Trim(rest.substr(0, colon));
          const std::string value = Trim(rest.substr(colon + 1));
          if (key == "id") {
            current_peer_id = ParseNodeId(value.c_str());
          }
        }
        continue;
      }
      if (indent >= 6 && in_peer) {
        const size_t colon = cleaned.find(':');
        if (colon == std::string::npos) continue;
        const std::string key = Trim(cleaned.substr(0, colon));
        const std::string value = Trim(cleaned.substr(colon + 1));
        if (key == "id") {
          current_peer_id = ParseNodeId(value.c_str());
        } else if (key == "host") {
          current_peer.host = value;
        } else if (key == "raft_port") {
          int port = 0;
          if (ParseIntValue(value, &port)) {
            current_peer.raft_port = static_cast<uint16_t>(port);
          }
        }
        continue;
      }
    }

    const size_t colon = cleaned.find(':');
    if (colon == std::string::npos) continue;
    const std::string key = Trim(cleaned.substr(0, colon));
    const std::string value = Trim(cleaned.substr(colon + 1));

    if (section == Section::kServer) {
      if (key == "port") {
        ParseIntValue(value, &cfg.server_port);
      } else if (key == "db_path") {
        cfg.db_path = value;
      }
      continue;
    }

    if (section == Section::kRaft) {
      if (key == "enabled") {
        ParseBoolValue(value, &cfg.raft_enabled);
      } else if (key == "node_id") {
        cfg.raft.node_id = ParseNodeId(value.c_str());
      } else if (key == "raft_port") {
        int port = 0;
        if (ParseIntValue(value, &port)) {
          cfg.raft.raft_port = static_cast<uint16_t>(port);
        }
      } else if (key == "data_dir") {
        cfg.raft.data_dir = value;
      }
      continue;
    }

    if (section == Section::kCache) {
      if (key == "enabled") {
        ParseBoolValue(value, &cfg.cache_enabled);
      } else if (key == "capacity") {
        ParseSizeValue(value, &cfg.cache_capacity);
      } else if (key == "ttl_ms") {
        ParseInt64Value(value, &cfg.cache_ttl_ms);
      }
    }
  }

  flush_peer();
  return cfg;
}

// Parse Raft peers from env: "1:127.0.0.1:9528,2:127.0.0.1:9628"
std::unordered_map<uint64_t, kv::RaftConfig::Peer> ParsePeers(
    const std::string& s) {
  std::unordered_map<uint64_t, kv::RaftConfig::Peer> peers;
  std::istringstream iss(s);
  std::string item;
  while (std::getline(iss, item, ',')) {
    if (item.empty()) continue;
    std::istringstream pis(item);
    std::string id_str, host, port_str;
    if (!std::getline(pis, id_str, ':')) continue;
    if (!std::getline(pis, host, ':')) continue;
    if (!std::getline(pis, port_str, ':')) continue;
    uint64_t id = ParseNodeId(id_str.c_str());
    int p = ParsePort(port_str.c_str());
    if (id > 0 && p > 0) {
      peers[id] = {host, static_cast<uint16_t>(p)};
    }
  }
  return peers;
}

// Wrapper DB that routes writes through Raft, reads directly from local DB.
class RaftDBWrapper final : public kv::DB {
 public:
  RaftDBWrapper(kv::DB* real_db, kv::RaftServer* raft)
      : real_db_(real_db), raft_(raft) {}

  kv::Status Put(const kv::WriteOptions&, const kv::Slice& key,
                 const kv::Slice& value) override {
    if (!raft_->IsLeader()) {
      return kv::Status::AlreadyExists(
          "not the leader, current leader is " +
          std::to_string(raft_->LeaderId()));
    }

    std::string cmd = kv::raft::EncodePutCmd(key.ToString(),
                                               value.ToString());
    return raft_->Propose(cmd);
  }

  kv::Status Get(const kv::ReadOptions& options, const kv::Slice& key,
                 std::string* value) override {
    if (!raft_->IsLeader()) {
      return kv::Status::AlreadyExists(
          "not the leader, current leader is " +
          std::to_string(raft_->LeaderId()));
    }
    kv::Status s = raft_->LinearizableReadBarrier();
    if (!s.ok()) {
      return s;
    }
    return real_db_->Get(options, key, value);
  }

  kv::Status Delete(const kv::WriteOptions&, const kv::Slice& key) override {
    if (!raft_->IsLeader()) {
      return kv::Status::AlreadyExists(
          "not the leader, current leader is " +
          std::to_string(raft_->LeaderId()));
    }

    std::string cmd = kv::raft::EncodeDelCmd(key.ToString());
    return raft_->Propose(cmd);
  }

  kv::Status Write(const kv::WriteOptions& options,
                   const kv::WriteBatch& batch) override {
    (void)options;
    (void)batch;
    return kv::Status::InvalidArgument(
        "write batch is not supported when raft is enabled");
  }

  kv::Status BeginTransaction(const kv::TxnOptions& options,
                              std::unique_ptr<kv::Transaction>* txn) override {
    (void)options;
    if (txn != nullptr) {
      txn->reset();
    }
    return kv::Status::InvalidArgument(
        "transactions are not supported when raft is enabled");
  }

  kv::Status Compact() override { return real_db_->Compact(); }

  kv::Status GetCacheStats(kv::CacheStats* stats) const override {
    return real_db_->GetCacheStats(stats);
  }

  kv::Status GetReadPathStats(kv::ReadPathStats* stats) const override {
    return real_db_->GetReadPathStats(stats);
  }

  kv::Status GetCompactionStats(kv::CompactionStats* stats) const override {
    return real_db_->GetCompactionStats(stats);
  }

  const kv::Snapshot* GetSnapshot() override {
    return real_db_->GetSnapshot();
  }

  kv::Status ReleaseSnapshot(const kv::Snapshot* snapshot) override {
    return real_db_->ReleaseSnapshot(snapshot);
  }

  kv::Status Close() override { return real_db_->Close(); }

  kv::Status ApplyPut(const std::string& key,
                      const std::string& value) override {
    return real_db_->ApplyPut(key, value);
  }

  kv::Status ApplyDelete(const std::string& key) override {
    return real_db_->ApplyDelete(key);
  }

 private:
  kv::DB* real_db_;
  kv::RaftServer* raft_;
};

}  // namespace

int main(int argc, char** argv) {
  std::string config_path = "config/server.yaml";
  if (argc >= 2 && std::string(argv[1]).rfind("--config=", 0) == 0) {
    config_path = std::string(argv[1]).substr(9);
  } else if (const char* v = std::getenv("KV_CONFIG"); v != nullptr &&
             std::string(v).size() > 0) {
    config_path = v;
  }

  AppConfigFile file_config;
  std::string config_error;
  if (auto loaded = LoadConfigFile(config_path, &config_error); loaded.has_value()) {
    file_config = *loaded;
  } else if (!config_error.empty()) {
    std::cerr << "invalid config file " << config_path << ": "
              << config_error << "\n";
    return 1;
  }

  int port = file_config.server_port;
  std::string db_path = file_config.db_path;
  int arg_index = 1;
  if (argc >= 2 && std::string(argv[1]).rfind("--config=", 0) == 0) {
    arg_index = 2;
  }

  if (argc > arg_index) {
    port = ParsePort(argv[arg_index]);
    if (port < 0) {
      std::cerr << "invalid port: " << argv[arg_index] << "\n";
      return 1;
    }
  }

  if (argc > arg_index + 1) {
    db_path = argv[arg_index + 1];
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

  if (raft_enabled) {
    if (const char* v = std::getenv("KV_RAFT_NODE_ID"); v != nullptr) {
      raft_config->node_id = ParseNodeId(v);
    }
    if (const char* v = std::getenv("KV_RAFT_PORT"); v != nullptr) {
      int p = ParsePort(v);
      if (p > 0) raft_config->raft_port = static_cast<uint16_t>(p);
    }
    if (const char* v = std::getenv("KV_RAFT_PEERS"); v != nullptr) {
      raft_config->peers = ParsePeers(v);
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

  // ---- Open DB ----
  kv::DBOptions options;
  options.db_path = db_path;
  options.cache_enabled = file_config.cache_enabled;
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
    const std::string s(v);
    if (s == "lfu" || s == "LFU") options.cache_policy = kv::CachePolicy::kLFU;
    if (s == "lru" || s == "LRU") options.cache_policy = kv::CachePolicy::kLRU;
    if (s == "shard_lru" || s == "SHARD_LRU" || s == "slru" || s == "SLRU") {
      options.cache_policy = kv::CachePolicy::kShardLRU;
    }
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

  std::unique_ptr<kv::DB> db;
  kv::Status s = kv::DB::Open(options, &db);
  if (!s.ok()) {
    std::cerr << "open db failed: " << s.ToString() << "\n";
    return 1;
  }

  // ---- Start Raft if enabled ----
  if (raft_enabled && raft_config) {
    raft_server = std::make_unique<kv::RaftServer>(*raft_config, db.get());
    s = raft_server->Start();
    if (!s.ok()) {
      std::cerr << "raft server start failed: " << s.ToString() << "\n";
      return 1;
    }
    std::cout << "raft server started: node_id=" << raft_config->node_id
              << " raft_port=" << raft_config->raft_port << "\n";
  }

  // ---- Start client server ----
  std::unique_ptr<RaftDBWrapper> raft_db;
  kv::DB* server_db = db.get();

  if (raft_server) {
    raft_db = std::make_unique<RaftDBWrapper>(db.get(), raft_server.get());
    server_db = raft_db.get();
  }

  kv::net::Server server;
  s = server.Start(static_cast<uint16_t>(port), server_db);
  if (!s.ok()) {
    std::cerr << "server start failed: " << s.ToString() << "\n";
    return 1;
  }

  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  std::cout << "kv_server listening on 0.0.0.0:" << port
            << " db_path=" << db_path;
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
      std::cout << "\n";
      last_report = now;
    }
  }

  (void)server.Stop();
  if (raft_server) (void)raft_server->Stop();
  (void)db->Close();
  return 0;
}
