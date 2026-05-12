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

  const std::vector<uint8_t>& data() const noexcept { return data_; }
  std::vector<uint8_t>& data() noexcept { return data_; }
  size_t bits() const noexcept { return bits_; }

  static BloomFilter FromRaw(size_t bits, const std::vector<uint8_t>& raw);

 private:
  uint64_t Hash64(const std::string& key, uint64_t seed) const;

  size_t bits_;
  std::vector<uint8_t> data_;
};

}  // namespace kv
