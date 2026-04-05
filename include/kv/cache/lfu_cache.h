#pragma once

#include <chrono>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>

#include "kv/cache/cache.h"

namespace kv {

class LFUCache final : public Cache {
 public:
  LFUCache(size_t capacity, int64_t default_ttl_ms);

  bool Get(const std::string& key, std::string* value) override;
  void Put(const std::string& key,
           const std::string& value,
           int64_t ttl_ms = -1) override;
  void Erase(const std::string& key) override;
  bool Contains(const std::string& key) override;

  size_t Size() const override;
  CacheStats Stats() const override;

 private:
  struct Entry {
    std::string value;
    uint64_t freq = 1;
    std::chrono::steady_clock::time_point expire_at;
    bool has_expire = false;
    std::list<std::string>::iterator it;
  };

  bool IsExpired(const Entry& entry) const;
  void Touch(std::unordered_map<std::string, Entry>::iterator it);
  void EraseInternal(std::unordered_map<std::string, Entry>::iterator it);
  void EvictIfNeeded();

  const size_t capacity_;
  const int64_t default_ttl_ms_;

  mutable std::mutex mu_;
  std::unordered_map<std::string, Entry> entries_;
  std::unordered_map<uint64_t, std::list<std::string>> buckets_;
  uint64_t min_freq_;
  CacheStats stats_;
};

}  // namespace kv
