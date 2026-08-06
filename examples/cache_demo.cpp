#include "kv/cache/cache.h"

#include <iostream>
#include <memory>
#include <string>

int main() {
  auto cache = kv::CreateCache(kv::CachePolicy::kLRU, 2, 0);
  cache->Put("a", "one");
  cache->Put("b", "two");

  std::string value;
  std::cout << "a hit = " << (cache->Get("a", &value) ? "yes" : "no")
            << ", value = " << value << "\n";

  cache->Put("c", "three");
  const kv::CacheStats stats = cache->Stats();
  std::cout << "size = " << cache->Size() << ", evictions = " << stats.evict << "\n";
  return 0;
}
