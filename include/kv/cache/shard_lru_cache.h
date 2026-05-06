#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "kv/cache/cache.h"
#include "kv/cache/lru_cache.h"

namespace kv {

class ShardLRUCache final : public Cache {
 public:
  ShardLRUCache(size_t capacity,
                int64_t default_ttl_ms,
                size_t shard_count = 0);

  bool Get(const std::string& key, std::string* value) override;
  void Put(const std::string& key,
           const std::string& value,
           int64_t ttl_ms = -1) override;
  void Erase(const std::string& key) override;
  bool Contains(const std::string& key) override;

  size_t Size() const override;
  CacheStats Stats() const override;

 private:
  size_t ShardIndex(const std::string& key) const;

  std::vector<std::unique_ptr<LRUCache>> shards_;
  size_t shard_count_ = 1;
};

}  // namespace kv
