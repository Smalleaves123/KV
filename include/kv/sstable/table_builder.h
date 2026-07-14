#pragma once

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string>

#include "kv/common/status.h"
#include "kv/sstable/filter_block.h"

namespace kv {

class BlockBuilder;

// Builds a block-based SST file.
//
// File layout:
//   [Data Block 1]
//   [Data Block 2]
//   ...
//   [Filter Block]
//   [Index Block]    (BlockHandle per data block using BlockBuilder format)
//   [Footer]         (48 bytes, fixed size)
class TableBuilder {
 public:
  // Construct with target file path. Blocks are ~block_size bytes.
  TableBuilder(const std::string& file_path, size_t block_size = 4096);
  ~TableBuilder();

  TableBuilder(const TableBuilder&) = delete;
  TableBuilder& operator=(const TableBuilder&) = delete;

  // Add a key-value entry. Keys must be added in ascending order.
  // type: 0 = value (put), 1 = deletion (delete)
  Status Add(const std::string& key, uint64_t seq, uint8_t type,
             const std::string& value);
  Status Add(const std::string& key, uint64_t seq, uint8_t type,
             uint64_t expires_at_ms, const std::string& value);

  // Finalize the file: flush last block, write filter/index/footer.
  Status Finish();

  // Current file size (approximate, before Finish).
  uint64_t FileSize() const noexcept;

  // Abandon the build (removes the partial file on destruction if not finished).
  void Abandon();

 private:
  Status FlushDataBlock();

  std::string file_path_;
  std::ofstream file_;
  size_t block_size_;
  uint64_t offset_;
  uint64_t max_seq_;
  bool finished_;
  bool abandoned_;

  // Current data block
  std::unique_ptr<BlockBuilder> data_block_;
  std::string last_key_;

  // Index entries: (last_key, block_handle)
  struct IndexEntry {
    std::string last_key;
    uint64_t block_offset;
    uint64_t block_size;
  };
  std::vector<IndexEntry> index_entries_;

  // Filter
  FilterBlockBuilder filter_builder_;
};

}  // namespace kv
