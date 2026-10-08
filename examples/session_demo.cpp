#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "kv/engine/db.h"

int main(int argc, char** argv) {
  kv::DBOptions options;
  options.db_path = argc > 1 ? argv[1] : "data/session_demo";
  options.expiry_cleanup_interval_ms = 25;
  std::unique_ptr<kv::DB> db;
  const auto check = [](const kv::Status& status) {
    if (!status.ok()) std::cerr << status.ToString() << '\n';
    return status.ok();
  };
  if (!check(kv::DB::Open(options, &db))) return 1;

  const std::string key = "session:demo-token";
  kv::WriteOptions write;
  write.sync = true;
  write.ttl_ms = 1000;
  if (!check(db->Put(write, key, R"({"user_id":"alice"})"))) return 1;
  std::string value;
  if (!check(db->Get(kv::ReadOptions{}, key, &value))) return 1;
  std::cout << "Created: " << value << '\n';

  // Activity renews the TTL while preserving the session payload.
  if (!check(db->ExpireMillis(write, key, 2000))) return 1;
  int64_t remaining = -2;
  if (!check(db->PTTL(kv::ReadOptions{}, key, &remaining))) return 1;
  std::cout << "Renewed: " << remaining << " ms remaining\n";

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  do {
    if (!check(db->PTTL(kv::ReadOptions{}, key, &remaining))) return 1;
    if (remaining == -2) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(25));
  } while (std::chrono::steady_clock::now() < deadline);
  if (remaining != -2) return 1;
  std::cout << "Expired: lookup no longer returns the session\n";

  // A new login gets a fresh TTL; logout revokes it immediately.
  if (!check(db->Put(write, key, R"({"user_id":"alice"})"))) return 1;
  if (!check(db->Delete(kv::WriteOptions{}, key))) return 1;
  if (!db->Get(kv::ReadOptions{}, key, &value).IsNotFound()) return 1;
  std::cout << "Revoked: session removed\n";
  return check(db->Close()) ? 0 : 1;
}
