#include "kv/table/bloom_filter.h"

namespace kv {

BloomFilter::BloomFilter(size_t bits)
    : bits_(bits == 0 ? 1 : bits),
      data_((bits_ + 7) / 8, 0) {}

uint64_t BloomFilter::Hash64(const std::string& key, uint64_t seed) const {
  uint64_t h = seed ^ 0x9e3779b97f4a7c15ULL;
  for (unsigned char c : key) {
    h ^= static_cast<uint64_t>(c) + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
  }
  return h;
}

void BloomFilter::Add(const std::string& key) {
  const uint64_t h1 = Hash64(key, 0x12345678);
  const uint64_t h2 = Hash64(key, 0x87654321);
  for (int i = 0; i < 4; ++i) {
    const uint64_t h = h1 + static_cast<uint64_t>(i) * h2;
    const size_t bit = static_cast<size_t>(h % bits_);
    data_[bit / 8] |= static_cast<uint8_t>(1u << (bit % 8));
  }
}

bool BloomFilter::MayContain(const std::string& key) const {
  const uint64_t h1 = Hash64(key, 0x12345678);
  const uint64_t h2 = Hash64(key, 0x87654321);
  for (int i = 0; i < 4; ++i) {
    const uint64_t h = h1 + static_cast<uint64_t>(i) * h2;
    const size_t bit = static_cast<size_t>(h % bits_);
    if ((data_[bit / 8] & static_cast<uint8_t>(1u << (bit % 8))) == 0) {
      return false;
    }
  }
  return true;
}

BloomFilter BloomFilter::FromRaw(size_t bits, const std::vector<uint8_t>& raw) {
  BloomFilter bf(bits);
  if (raw.size() <= bf.data_.size()) {
    std::copy(raw.begin(), raw.end(), bf.data_.begin());
  }
  return bf;
}

}  // namespace kv

