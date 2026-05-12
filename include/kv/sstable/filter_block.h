#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace kv {

class BloomFilter;

// Builder for the filter block in an SST file.
// Uses a single Bloom filter for all keys.
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(size_t bits_per_key = 10);

  void AddKey(const std::string& key);
  std::string Finish();
  void Reset();
  bool Empty() const noexcept;

 private:
  std::vector<std::string> keys_;
  size_t bits_per_key_;
};

// Reader for the filter block.
class FilterBlockReader {
 public:
  FilterBlockReader() : data_(nullptr), size_(0) {}
  FilterBlockReader(const char* data, size_t size);

  bool MayMatch(const std::string& key) const;

 private:
  const char* data_;
  size_t size_;
};

}  // namespace kv
