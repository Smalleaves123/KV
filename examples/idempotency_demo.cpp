#include "kv/engine/db.h"

#include <array>
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

int main(int argc, char** argv) {
  kv::DBOptions options;
  options.db_path = argc > 1 ? argv[1] : "data/idempotency_demo";
  std::unique_ptr<kv::DB> db;
  const auto check = [](const kv::Status& status) {
    if (!status.ok())
      std::cerr << status.ToString() << '\n';
    return status.ok();
  };
  if (!check(kv::DB::Open(options, &db)))
    return 1;
  const auto request_id = std::chrono::system_clock::now().time_since_epoch().count();
  const std::string key = "idem:demo:" + std::to_string(request_id);

  constexpr size_t kClients = 8;
  std::array<kv::Status, kClients> statuses;
  std::array<bool, kClients> inserted{};
  std::array<std::thread, kClients> threads;
  std::atomic<bool> start{false};
  kv::WriteOptions write;
  write.sync = true;
  write.ttl_ms = 60000;
  for (size_t i = 0; i < kClients; ++i) {
    threads[i] = std::thread([&, i] {
      while (!start.load())
        std::this_thread::yield();
      statuses[i] =
          db->PutIfAbsent(write, key, "receipt-" + std::to_string(i), &inserted[i]);
    });
  }
  start.store(true);
  for (auto& thread : threads)
    thread.join();
  size_t winners = 0;
  std::string winner;
  for (size_t i = 0; i < kClients; ++i) {
    if (!check(statuses[i]))
      return 1;
    if (inserted[i]) {
      ++winners;
      winner = "receipt-" + std::to_string(i);
    }
  }
  if (winners != 1)
    return 1;
  std::cout << "Concurrent requests: " << winners << " accepted, " << kClients - winners
            << " duplicates\n";

  if (!check(db->Close()))
    return 1;
  db.reset();
  if (!check(kv::DB::Open(options, &db)))
    return 1;
  bool created = false;
  if (!check(db->PutIfAbsent(write, key, "retry", &created)) || created)
    return 1;
  std::string receipt;
  if (!check(db->Get(kv::ReadOptions{}, key, &receipt)) || receipt != winner)
    return 1;
  std::cout << "Retry after restart: replay " << receipt << '\n';
  int64_t ttl_ms = 0;
  if (!check(db->PTTL(kv::ReadOptions{}, key, &ttl_ms)))
    return 1;
  std::cout << "Deduplication window: " << ttl_ms << " ms remaining\n";
  return check(db->Close()) ? 0 : 1;
}
