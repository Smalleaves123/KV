#include "kv/cache/ttl_manager.h"

#include <algorithm>

namespace kv {

TTLManager::TTLManager(int64_t default_ttl_ms)
    : default_ttl_ms_(default_ttl_ms) {}

void TTLManager::SetDefaultTTL(int64_t default_ttl_ms) {
  std::lock_guard<std::mutex> lk(mu_);
  default_ttl_ms_ = default_ttl_ms;
}

void TTLManager::SetTTL(const std::string& key, int64_t ttl_ms) {
  std::lock_guard<std::mutex> lk(mu_);

  int64_t effective_ttl = ttl_ms;
  if (ttl_ms < 0) {
    effective_ttl = default_ttl_ms_;
  }

  if (effective_ttl <= 0) {
    expires_.erase(key);
    return;
  }

  expires_[key] = Now() + std::chrono::milliseconds(effective_ttl);
}

bool TTLManager::IsExpired(const std::string& key) const {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = expires_.find(key);
  if (it == expires_.end()) {
    return false;
  }
  return Now() >= it->second;
}

bool TTLManager::HasTTL(const std::string& key) const {
  std::lock_guard<std::mutex> lk(mu_);
  return expires_.find(key) != expires_.end();
}

void TTLManager::Erase(const std::string& key) {
  std::lock_guard<std::mutex> lk(mu_);
  expires_.erase(key);
}

void TTLManager::Clear() {
  std::lock_guard<std::mutex> lk(mu_);
  expires_.clear();
}

size_t TTLManager::Size() const {
  std::lock_guard<std::mutex> lk(mu_);
  return expires_.size();
}

size_t TTLManager::PurgeExpired(std::vector<std::string>* expired_keys) {
  std::lock_guard<std::mutex> lk(mu_);
  const TimePoint now = Now();

  std::vector<std::string> expired;
  size_t purged = 0;
  for (auto it = expires_.begin(); it != expires_.end();) {
    if (now >= it->second) {
      expired.push_back(it->first);
      it = expires_.erase(it);
      ++purged;
    } else {
      ++it;
    }
  }

  if (!expired.empty()) {
    std::sort(expired.begin(), expired.end());
    if (expired_keys != nullptr) {
      expired_keys->insert(expired_keys->end(), expired.begin(), expired.end());
    }
  }

  return purged;
}

TTLManager::TimePoint TTLManager::Now() {
  return std::chrono::steady_clock::now();
}

}  // namespace kv
