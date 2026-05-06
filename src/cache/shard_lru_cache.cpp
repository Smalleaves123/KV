#include "kv/cache/shard_lru_cache.h"

#include <algorithm>
#include <functional>
#include <thread>

namespace kv {

namespace {
size_t DefaultShardCount() {
  const size_t hc = static_cast<size_t>(std::thread::hardware_concurrency());
  if (hc == 0) {
    return 4;
  }
  return std::min<size_t>(16, hc);
}
}  // namespace

ShardLRUCache::ShardLRUCache(size_t capacity,
                             int64_t default_ttl_ms,
                             size_t shard_count) {
  if (shard_count == 0) {
    shard_count = DefaultShardCount();
  }
  if (shard_count == 0) {
    shard_count = 1;
  }
  if (capacity > 0) {
    shard_count = std::min(shard_count, capacity);
  }

  shard_count_ = shard_count;
  shards_.reserve(shard_count_);

  const size_t base_cap = shard_count_ == 0 ? 0 : capacity / shard_count_;
  const size_t rem = shard_count_ == 0 ? 0 : capacity % shard_count_;
  for (size_t i = 0; i < shard_count_; ++i) {
    const size_t shard_capacity = base_cap + (i < rem ? 1 : 0);
    shards_.push_back(std::make_unique<LRUCache>(shard_capacity, default_ttl_ms));
  }
}

size_t ShardLRUCache::ShardIndex(const std::string& key) const {
  const size_t h = std::hash<std::string>{}(key);
  return h % shard_count_;
}

bool ShardLRUCache::Get(const std::string& key, std::string* value) {
  return shards_[ShardIndex(key)]->Get(key, value);
}

void ShardLRUCache::Put(const std::string& key,
                        const std::string& value,
                        int64_t ttl_ms) {
  shards_[ShardIndex(key)]->Put(key, value, ttl_ms);
}

void ShardLRUCache::Erase(const std::string& key) {
  shards_[ShardIndex(key)]->Erase(key);
}

bool ShardLRUCache::Contains(const std::string& key) {
  return shards_[ShardIndex(key)]->Contains(key);
}

size_t ShardLRUCache::Size() const {
  size_t total = 0;
  for (const auto& shard : shards_) {
    total += shard->Size();
  }
  return total;
}

CacheStats ShardLRUCache::Stats() const {
  CacheStats out;
  for (const auto& shard : shards_) {
    const CacheStats st = shard->Stats();
    out.hit += st.hit;
    out.miss += st.miss;
    out.evict += st.evict;
    out.expire += st.expire;
  }
  return out;
}

}  // namespace kv
