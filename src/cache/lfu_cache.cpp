#include "kv/cache/lfu_cache.h"

#include <utility>

namespace kv {

LFUCache::LFUCache(size_t capacity, int64_t default_ttl_ms)
    : capacity_(capacity),
      default_ttl_ms_(default_ttl_ms),
      mu_(),
      entries_(),
      buckets_(),
      min_freq_(0),
      stats_() {}

bool LFUCache::IsExpired(const Entry& entry) const {
  if (!entry.has_expire) {
    return false;
  }
  return std::chrono::steady_clock::now() >= entry.expire_at;
}

void LFUCache::Touch(std::unordered_map<std::string, Entry>::iterator it) {
  const uint64_t old_freq = it->second.freq;
  auto bucket_it = buckets_.find(old_freq);
  if (bucket_it != buckets_.end()) {
    bucket_it->second.erase(it->second.it);
    if (bucket_it->second.empty()) {
      buckets_.erase(bucket_it);
      if (min_freq_ == old_freq) {
        ++min_freq_;
      }
    }
  }

  const uint64_t new_freq = old_freq + 1;
  auto& bucket = buckets_[new_freq];
  bucket.push_front(it->first);
  it->second.freq = new_freq;
  it->second.it = bucket.begin();
}

void LFUCache::EraseInternal(std::unordered_map<std::string, Entry>::iterator it) {
  const uint64_t freq = it->second.freq;
  auto bucket_it = buckets_.find(freq);
  if (bucket_it != buckets_.end()) {
    bucket_it->second.erase(it->second.it);
    if (bucket_it->second.empty()) {
      buckets_.erase(bucket_it);
      if (min_freq_ == freq && buckets_.empty()) {
        min_freq_ = 0;
      }
    }
  }
  entries_.erase(it);
}

void LFUCache::EvictIfNeeded() {
  while (capacity_ > 0 && entries_.size() > capacity_) {
    auto bucket_it = buckets_.find(min_freq_);
    if (bucket_it == buckets_.end() || bucket_it->second.empty()) {
      if (buckets_.empty()) {
        min_freq_ = 0;
        return;
      }
      min_freq_ = buckets_.begin()->first;
      continue;
    }

    const std::string key = bucket_it->second.back();
    bucket_it->second.pop_back();
    if (bucket_it->second.empty()) {
      buckets_.erase(bucket_it);
    }

    auto it = entries_.find(key);
    if (it != entries_.end()) {
      entries_.erase(it);
      ++stats_.evict;
    }
  }
}

bool LFUCache::Get(const std::string& key, std::string* value) {
  std::lock_guard<std::mutex> lk(mu_);

  auto it = entries_.find(key);
  if (it == entries_.end()) {
    ++stats_.miss;
    return false;
  }

  if (IsExpired(it->second)) {
    EraseInternal(it);
    ++stats_.expire;
    ++stats_.miss;
    return false;
  }

  if (value != nullptr) {
    *value = it->second.value;
  }
  Touch(it);
  ++stats_.hit;
  return true;
}

void LFUCache::Put(const std::string& key,
                   const std::string& value,
                   int64_t ttl_ms) {
  std::lock_guard<std::mutex> lk(mu_);

  if (capacity_ == 0) {
    return;
  }

  const int64_t effective_ttl = ttl_ms >= 0 ? ttl_ms : default_ttl_ms_;

  auto it = entries_.find(key);
  if (it != entries_.end()) {
    it->second.value = value;
    if (effective_ttl > 0) {
      it->second.has_expire = true;
      it->second.expire_at =
          std::chrono::steady_clock::now() + std::chrono::milliseconds(effective_ttl);
    } else {
      it->second.has_expire = false;
    }
    Touch(it);
    return;
  }

  auto& bucket = buckets_[1];
  bucket.push_front(key);

  Entry entry;
  entry.value = value;
  entry.freq = 1;
  entry.it = bucket.begin();
  if (effective_ttl > 0) {
    entry.has_expire = true;
    entry.expire_at =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(effective_ttl);
  }

  entries_.emplace(key, std::move(entry));
  min_freq_ = 1;
  EvictIfNeeded();
}

void LFUCache::Erase(const std::string& key) {
  std::lock_guard<std::mutex> lk(mu_);

  auto it = entries_.find(key);
  if (it != entries_.end()) {
    EraseInternal(it);
  }
}

bool LFUCache::Contains(const std::string& key) {
  std::lock_guard<std::mutex> lk(mu_);

  auto it = entries_.find(key);
  if (it == entries_.end()) {
    return false;
  }
  if (IsExpired(it->second)) {
    EraseInternal(it);
    ++stats_.expire;
    return false;
  }
  return true;
}

size_t LFUCache::Size() const {
  std::lock_guard<std::mutex> lk(mu_);
  return entries_.size();
}

CacheStats LFUCache::Stats() const {
  std::lock_guard<std::mutex> lk(mu_);
  return stats_;
}

}  // namespace kv
