#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace kv {

enum class CachePolicy {
  kLRU = 0,
  kLFU = 1,
};

struct CacheStats {
  uint64_t hit = 0;
  uint64_t miss = 0;
  uint64_t evict = 0;
  uint64_t expire = 0;
};

class Cache {
 public:
  virtual ~Cache() = default;

  virtual bool Get(const std::string& key, std::string* value) = 0;
  virtual void Put(const std::string& key,
                   const std::string& value,
                   int64_t ttl_ms = -1) = 0;
  virtual void Erase(const std::string& key) = 0;
  virtual bool Contains(const std::string& key) = 0;

  virtual size_t Size() const = 0;
  virtual CacheStats Stats() const = 0;
};

std::unique_ptr<Cache> CreateCache(CachePolicy policy,
                                   size_t capacity,
                                   int64_t default_ttl_ms);

}  // namespace kv
