#include "kv/cache/cache.h"

#include "kv/cache/lfu_cache.h"
#include "kv/cache/lru_cache.h"
#include "kv/cache/shard_lru_cache.h"

namespace kv {

std::unique_ptr<Cache> CreateCache(CachePolicy policy,
                                   size_t capacity,
                                   int64_t default_ttl_ms) {
  if (policy == CachePolicy::kLFU) {
    return std::make_unique<LFUCache>(capacity, default_ttl_ms);
  }
  if (policy == CachePolicy::kShardLRU) {
    return std::make_unique<ShardLRUCache>(capacity, default_ttl_ms);
  }
  return std::make_unique<LRUCache>(capacity, default_ttl_ms);
}

}  // namespace kv
