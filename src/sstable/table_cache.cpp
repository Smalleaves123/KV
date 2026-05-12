#include "kv/sstable/table_cache.h"

#include "kv/sstable/table_reader.h"

namespace kv {

TableCache::TableCache(size_t max_open_files)
    : max_open_files_(max_open_files == 0 ? 1 : max_open_files),
      lru_(),
      map_() {}

Status TableCache::Get(const std::string& file_path,
                       std::shared_ptr<const TableReader>* out) {
  auto it = map_.find(file_path);
  if (it != map_.end()) {
    // Move to front of LRU
    lru_.splice(lru_.begin(), lru_, it->second);
    *out = it->second->reader;
    return Status::OK();
  }

  std::unique_ptr<TableReader> reader;
  Status s = TableReader::Open(file_path, &reader);
  if (!s.ok()) {
    return s;
  }

  auto shared = std::shared_ptr<const TableReader>(reader.release());
  Entry entry;
  entry.file_path = file_path;
  entry.reader = shared;
  lru_.push_front(std::move(entry));
  map_[file_path] = lru_.begin();

  EnforceCapacity();
  *out = shared;
  return Status::OK();
}

void TableCache::Evict(const std::string& file_path) {
  auto it = map_.find(file_path);
  if (it == map_.end()) return;

  lru_.erase(it->second);
  map_.erase(it);
}

void TableCache::Clear() {
  lru_.clear();
  map_.clear();
}

size_t TableCache::Size() const noexcept {
  return map_.size();
}

void TableCache::EnforceCapacity() {
  while (lru_.size() > max_open_files_) {
    const auto& last = lru_.back();
    map_.erase(last.file_path);
    lru_.pop_back();
  }
}

}  // namespace kv
