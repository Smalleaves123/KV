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
                << " total_requests=" << stats.total_requests << "\n";
      last_report = now;
    }
  }

  (void)server.Stop();
  (void)db->Close();
  return 0;
}
