#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "kv/engine/db.h"
#include "kv/net/server.h"

namespace {
std::atomic<bool> g_stop{false};

void OnSignal(int) {
  g_stop.store(true, std::memory_order_relaxed);
}

int ParsePort(const char* s) {
  if (s == nullptr) {
    return -1;
  }
  char* end = nullptr;
  const long v = std::strtol(s, &end, 10);
  if (end == s || *end != '\0' || v <= 0 || v > 65535) {
    return -1;
  }
  return static_cast<int>(v);
}
}  // namespace

int main(int argc, char** argv) {
  int port = 9527;
  std::string db_path = "data/db";

  if (argc >= 2) {
    port = ParsePort(argv[1]);
    if (port < 0) {
      std::cerr << "invalid port: " << argv[1] << "\n";
      return 1;
    }
  }

  if (argc >= 3) {
    db_path = argv[2];
  }

  kv::DBOptions options;
  options.db_path = db_path;

  // Optional cache config via env vars (keeps defaults unchanged).
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

  kv::net::Server server;
  s = server.Start(static_cast<uint16_t>(port), db.get());
  if (!s.ok()) {
    std::cerr << "server start failed: " << s.ToString() << "\n";
    (void)db->Close();
    return 1;
  }

  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);

  std::cout << "kv_server listening on 0.0.0.0:" << port
            << ", db_path=" << db_path << "\n";

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
  (void)db->Close();
  return 0;
}
