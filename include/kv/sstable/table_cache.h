#pragma once

#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>

#include "kv/common/status.h"

namespace kv {

class TableReader;

// LRU cache of open TableReader instances, keyed by file path.
// Avoids re-opening and re-parsing SST index blocks on every read.
class TableCache {
 public:
  explicit TableCache(size_t max_open_files = 64);
  ~TableCache() = default;

  TableCache(const TableCache&) = delete;
  TableCache& operator=(const TableCache&) = delete;

  // Get a cached TableReader, or open + cache it.
  Status Get(const std::string& file_path,
             std::shared_ptr<const TableReader>* out);

  // Evict a specific file from the cache.
  void Evict(const std::string& file_path);

  // Remove all entries.
  void Clear();

  size_t Size() const noexcept;

 private:
  void EnforceCapacity();

  struct Entry {
    std::string file_path;
    std::shared_ptr<const TableReader> reader;
  };

  size_t max_open_files_;
  std::list<Entry> lru_;
  std::unordered_map<std::string, decltype(lru_.begin())> map_;
};

}  // namespace kv
