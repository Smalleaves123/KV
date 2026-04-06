#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace kv {

class BloomFilter {
 public:
  explicit BloomFilter(size_t bits = 8192);
  void Add(const std::string& key);
  bool MayContain(const std::string& key) const;

 private:
  uint64_t Hash64(const std::string& key, uint64_t seed) const;

  size_t bits_;
  std::vector<uint8_t> data_;
};

}  // namespace kv
