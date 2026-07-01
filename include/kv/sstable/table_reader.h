#pragma once

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "kv/common/status.h"
#include "kv/sstable/filter_block.h"

namespace kv {

class TableIterator;

struct TableReadStatsDelta {
  uint64_t bloom_queries = 0;
  uint64_t bloom_negatives = 0;
};

// Reads a block-based SST file.
// Supports point lookups (Get) via index binary search + bloom filter.
class TableReader {
 public:
  // Open an SST file for reading.
  static Status Open(const std::string& file_path,
                     std::unique_ptr<TableReader>* out);

  ~TableReader();

  // Look up a key. Returns the first entry with key == target and seq <=
  // read_seq. On success, *type (0=value, 1=deletion) and *value are set.
  Status Get(const std::string& target, uint64_t read_seq, uint8_t* type,
             std::string* value) const;
  Status Get(const std::string& target, uint64_t read_seq, uint8_t* type,
             std::string* value, TableReadStatsDelta* stats) const;

  // Maximum sequence number in this file.
  uint64_t MaxSequence() const noexcept { return max_seq_; }

  // Total number of data blocks.
  size_t NumDataBlocks() const noexcept { return index_.size(); }

  // Expose the index for external iteration / compaction.
  struct IndexEntry {
    std::string last_key;
    uint64_t block_offset;
    uint64_t block_size;
  };
  const std::vector<IndexEntry>& index() const noexcept { return index_; }

  // Read an entire data block into memory. Returns the block data.
  Status ReadBlock(uint64_t offset, uint64_t size,
                   std::string* block_data) const;

  const std::string& file_path() const noexcept { return file_path_; }

 private:
  TableReader() = default;
  Status Init();

  std::string file_path_;
  mutable std::ifstream file_;
  std::vector<IndexEntry> index_;
  std::string filter_data_;
  FilterBlockReader filter_;
  uint64_t max_seq_;
};

// Iterator over all entries in an SST file.
// Walks through each data block sequentially.
class TableIterator {
 public:
  TableIterator(const TableReader& reader);

  void SeekToFirst();
  // Position at the first entry with key >= target.
  void Seek(const std::string& target);
  void Next();
  bool Valid() const noexcept;

  const std::string& key() const noexcept;
  uint64_t seq() const noexcept;
  uint8_t type() const noexcept;
  const std::string& value() const noexcept;

 private:
  void LoadNextBlock();

  const TableReader& reader_;
  size_t block_idx_;
  std::string block_data_;
  // Current position within current block (offset-based tracking)
  const char* block_ptr_;
  const char* block_end_;

  // Current entry
  std::string current_key_;
  uint64_t current_seq_;
  uint8_t current_type_;
  std::string current_value_;
  bool valid_;
};

}  // namespace kv
