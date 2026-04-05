#include "kv/cache/lru_cache.h"

#include <utility>

namespace kv {

LRUCache::LRUCache(size_t capacity, int64_t default_ttl_ms)
    : capacity_(capacity),
      default_ttl_ms_(default_ttl_ms),
      mu_(),
      lru_(),
      map_(),
      stats_() {}

bool LRUCache::IsExpired(const Entry& entry) const {
  if (!entry.has_expire) {
    return false;
  }
  return std::chrono::steady_clock::now() >= entry.expire_at;
}

void LRUCache::Touch(std::unordered_map<std::string, Entry>::iterator it) {
  lru_.erase(it->second.it);
  lru_.push_front(it->first);
  it->second.it = lru_.begin();
}

void LRUCache::EraseInternal(std::unordered_map<std::string, Entry>::iterator it) {
  lru_.erase(it->second.it);
  map_.erase(it);
}

void LRUCache::EvictIfNeeded() {
  while (capacity_ > 0 && map_.size() > capacity_) {
    const std::string key = lru_.back();
    lru_.pop_back();
    auto it = map_.find(key);
    if (it != map_.end()) {
      map_.erase(it);
      ++stats_.evict;
    }
  }
}

bool LRUCache::Get(const std::string& key, std::string* value) {
  std::lock_guard<std::mutex> lk(mu_);

  auto it = map_.find(key);
  if (it == map_.end()) {
    ++stats_.miss;
    return false;
  }

  if (IsExpired(it->second)) {
    EraseInternal(it);
    ++stats_.expire;
    ++stats_.miss;
    return false;
  }

  Touch(it);
  if (value != nullptr) {
    *value = it->second.value;
  }
  ++stats_.hit;
  return true;
}

void LRUCache::Put(const std::string& key,
                   const std::string& value,
                   int64_t ttl_ms) {
  std::lock_guard<std::mutex> lk(mu_);

  if (capacity_ == 0) {
    return;
  }

  const int64_t effective_ttl = ttl_ms >= 0 ? ttl_ms : default_ttl_ms_;

  auto found = map_.find(key);
  if (found != map_.end()) {
    found->second.value = value;
    if (effective_ttl > 0) {
      found->second.has_expire = true;
      found->second.expire_at =
          std::chrono::steady_clock::now() + std::chrono::milliseconds(effective_ttl);
    } else {
      found->second.has_expire = false;
    }
    Touch(found);
    return;
  }

  lru_.push_front(key);

  Entry entry;
  entry.value = value;
  entry.it = lru_.begin();
  if (effective_ttl > 0) {
    entry.has_expire = true;
    entry.expire_at =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(effective_ttl);
  }

  map_.emplace(key, std::move(entry));
  EvictIfNeeded();
}

void LRUCache::Erase(const std::string& key) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = map_.find(key);
  if (it != map_.end()) {
    EraseInternal(it);
  }
}

bool LRUCache::Contains(const std::string& key) {
  std::lock_guard<std::mutex> lk(mu_);

  auto it = map_.find(key);
  if (it == map_.end()) {
    return false;
  }
  if (IsExpired(it->second)) {
    EraseInternal(it);
    ++stats_.expire;
    return false;
  }
  return true;
}

size_t LRUCache::Size() const {
  std::lock_guard<std::mutex> lk(mu_);
  return map_.size();
}

CacheStats LRUCache::Stats() const {
  std::lock_guard<std::mutex> lk(mu_);
  return stats_;
}

}  // namespace kv
