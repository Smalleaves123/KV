#include "kv/sstable/table_reader.h"

#include <cstring>

#include "kv/common/checksum.h"
#include "kv/common/encoding.h"
#include "kv/sstable/block.h"
#include "kv/sstable/block_iterator.h"
#include "kv/sstable/footer.h"
#include "kv/sstable/value_codec.h"

namespace kv {

// ==================== TableReader ====================

Status TableReader::Open(const std::string& file_path,
                         std::unique_ptr<TableReader>* out) {
  auto reader = std::unique_ptr<TableReader>(new TableReader());
  reader->file_path_ = file_path;
  reader->file_.open(file_path, std::ios::binary);
  if (!reader->file_.is_open()) {
    return Status::IOError("failed to open sst file: " + file_path);
  }
  Status s = reader->Init();
  if (!s.ok()) {
    return s;
  }
  *out = std::move(reader);
  return Status::OK();
}

TableReader::~TableReader() {
  if (file_.is_open()) {
    file_.close();
  }
}

Status TableReader::Init() {
  // Read the last kFooterEncodedSize bytes
  file_.seekg(0, std::ios::end);
  const auto file_size = file_.tellg();
  if (file_size < static_cast<std::streamoff>(kFooterEncodedSize)) {
    return Status::Corruption("sst file too small: " + file_path_);
  }

  std::string footer_data(kFooterEncodedSize, '\0');
  file_.seekg(file_size - static_cast<std::streamoff>(kFooterEncodedSize));
  file_.read(footer_data.data(), kFooterEncodedSize);
  if (!file_) {
    return Status::Corruption("failed to read footer: " + file_path_);
  }

  bool ok = false;
  Footer footer = Footer::DecodeFrom(
      std::string_view(footer_data.data(), kFooterEncodedSize), &ok);
  if (!ok) {
    return Status::Corruption("invalid footer magic: " + file_path_);
  }

  max_seq_ = footer.max_seq;
  has_data_block_checksums_ = footer.format_version >= 1;

  // Read index block
  std::string index_data(footer.index_handle.size, '\0');
  file_.seekg(static_cast<std::streamoff>(footer.index_handle.offset));
  file_.read(index_data.data(),
             static_cast<std::streamsize>(footer.index_handle.size));
  if (!file_) {
    return Status::Corruption("failed to read index block: " + file_path_);
  }

  Block index_block(index_data.data(), index_data.size());
  BlockIterator it(index_block);

  index_.clear();
  for (it.SeekToFirst(); it.Valid(); it.Next()) {
    std::string_view val = it.value();
    if (val.size() < 16) {
      return Status::Corruption("invalid index entry value: " + file_path_);
    }

    IndexEntry ie;
    ie.last_key = std::string(it.key());
    ie.block_offset = DecodeFixed64(val.data());
    ie.block_size = DecodeFixed64(val.data() + 8);
    index_.push_back(std::move(ie));
  }

  // Read filter block (if present)
  if (footer.filter_handle.size > 0) {
    filter_data_.assign(footer.filter_handle.size, '\0');
    file_.seekg(static_cast<std::streamoff>(footer.filter_handle.offset));
    file_.read(filter_data_.data(),
               static_cast<std::streamsize>(footer.filter_handle.size));
    if (file_) {
      filter_ = FilterBlockReader(filter_data_.data(), filter_data_.size());
    }
  }

  return Status::OK();
}

Status TableReader::Get(const std::string& target, uint64_t read_seq,
                        uint8_t* type, std::string* value) const {
  return Get(target, read_seq, type, value, nullptr, nullptr);
}

Status TableReader::Get(const std::string& target, uint64_t read_seq,
                        uint8_t* type, std::string* value,
                        TableReadStatsDelta* stats) const {
  return Get(target, read_seq, type, value, nullptr, stats);
}

Status TableReader::Get(const std::string& target, uint64_t read_seq,
                        uint8_t* type, std::string* value,
                        uint64_t* expires_at_ms,
                        TableReadStatsDelta* stats) const {
  if (type == nullptr || value == nullptr) {
    return Status::InvalidArgument("output pointer is null");
  }
  if (expires_at_ms != nullptr) {
    *expires_at_ms = 0;
  }

  // Bloom filter check
  if (stats != nullptr) {
    ++stats->bloom_queries;
  }
  if (!filter_.MayMatch(target)) {
    if (stats != nullptr) {
      ++stats->bloom_negatives;
    }
    return Status::NotFound("bloom filter rejected");
  }

  // Binary search index to find the right data block
  size_t lo = 0;
  size_t hi = index_.size();
  while (lo < hi) {
    const size_t mid = lo + (hi - lo) / 2;
    if (index_[mid].last_key < target) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }

  if (lo >= index_.size()) {
    return Status::NotFound("key not found in index");
  }

  for (size_t i = lo; i < index_.size(); ++i) {
    std::string block_data;
    Status s = ReadBlock(index_[i].block_offset, index_[i].block_size,
                         &block_data);
    if (!s.ok()) return s;

    Block block(block_data.data(), block_data.size());
    BlockIterator it(block);

    for (it.SeekToFirst(); it.Valid(); it.Next()) {
      const std::string_view entry_key = it.key();
      if (entry_key < target) {
        continue;
      }
      if (entry_key > target) {
        break;
      }

      if (it.seq() <= read_seq) {
        *type = it.type();
        if (it.type() == 0) {
          const std::string encoded_value(it.value());
          DecodeSSTValue(encoded_value, value, expires_at_ms);
          return Status::OK();
        }
        // type == 1: deletion tombstone
        return Status::NotFound("key deleted");
      }
    }
  }

  return Status::NotFound("key not found");
}

Status TableReader::ReadBlock(uint64_t offset, uint64_t size,
                              std::string* block_data) const {
  if (block_data == nullptr) {
    return Status::InvalidArgument("block data output is null");
  }
  const uint64_t bytes_to_read = size + (has_data_block_checksums_ ? 4 : 0);
  block_data->assign(static_cast<size_t>(bytes_to_read), '\0');
  file_.seekg(static_cast<std::streamoff>(offset));
  file_.read(block_data->data(), static_cast<std::streamsize>(bytes_to_read));
  if (!file_) {
    return Status::IOError("failed to read block");
  }
  if (has_data_block_checksums_) {
    const char* trailer = block_data->data() + size;
    const uint32_t expected = DecodeFixed32(trailer);
    const uint32_t actual = CRC32(block_data->data(), static_cast<size_t>(size));
    if (actual != expected) {
      return Status::Corruption("data block checksum mismatch: " + file_path_);
    }
    block_data->resize(static_cast<size_t>(size));
  }
  return Status::OK();
}

// ==================== TableIterator ====================

TableIterator::TableIterator(const TableReader& reader)
    : reader_(reader),
      block_idx_(0),
      block_data_(),
      block_ptr_(nullptr),
      block_end_(nullptr),
      current_key_(),
      current_seq_(0),
      current_type_(0),
      current_value_(),
      current_expires_at_ms_(0),
      valid_(false) {}

void TableIterator::SeekToFirst() {
  block_idx_ = 0;
  block_data_.clear();
  block_ptr_ = nullptr;
  block_end_ = nullptr;
  valid_ = false;

  LoadNextBlock();
}

void TableIterator::Seek(const std::string& target) {
  valid_ = false;
  block_data_.clear();
  block_ptr_ = nullptr;
  block_end_ = nullptr;

  const auto& idx = reader_.index();
  if (idx.empty()) {
    return;
  }

  // Binary search the index for the block that may contain target.
  size_t lo = 0;
  size_t hi = idx.size();
  while (lo < hi) {
    const size_t mid = lo + (hi - lo) / 2;
    if (idx[mid].last_key < target) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }

  if (lo >= idx.size()) {
    // target is past the last index entry's last_key — nothing to find.
    return;
  }

  block_idx_ = lo;
  const size_t data_end = idx.size();

  // Load the first candidate block and seek within it.
  do {
    Status s = reader_.ReadBlock(idx[block_idx_].block_offset,
                                  idx[block_idx_].block_size, &block_data_);
    if (!s.ok()) {
      valid_ = false;
      return;
    }

    Block block(block_data_.data(), block_data_.size());
    BlockIterator it(block);
    it.Seek(target);

    if (it.Valid()) {
      current_key_ = std::string(it.key());
      current_seq_ = it.seq();
      current_type_ = it.type();
      const std::string encoded_value(it.value());
      DecodeSSTValue(encoded_value, &current_value_,
                     &current_expires_at_ms_);
      block_ptr_ = block_data_.data();
      block_end_ = block_data_.data() + block_data_.size();
      valid_ = true;
      return;
    }

    ++block_idx_;
  } while (block_idx_ < data_end);

  // Exhausted all blocks — not found.
  valid_ = false;
}

void TableIterator::LoadNextBlock() {
  const auto& idx = reader_.index();
  if (block_idx_ >= idx.size()) {
    valid_ = false;
    return;
  }

  Status s = reader_.ReadBlock(idx[block_idx_].block_offset,
                                idx[block_idx_].block_size, &block_data_);
  if (!s.ok()) {
    valid_ = false;
    return;
  }

  Block block(block_data_.data(), block_data_.size());
  BlockIterator it(block);
  it.SeekToFirst();

  if (!it.Valid()) {
    ++block_idx_;
    LoadNextBlock();
    return;
  }

  current_key_ = std::string(it.key());
  current_seq_ = it.seq();
  current_type_ = it.type();
  const std::string encoded_value(it.value());
  DecodeSSTValue(encoded_value, &current_value_, &current_expires_at_ms_);

  // Set up for Next() to find the next entry
  block_ptr_ = block_data_.data();
  block_end_ = block_data_.data() + block_data_.size();
  valid_ = true;
}

void TableIterator::Next() {
  if (!valid_) return;

  // We need to find the next entry in the current block.
  // Re-parse from current position + size of current entry.
  // Simpler approach: re-parse the block from scratch and track position.
  //
  // Implementation: use Block + BlockIterator to find current position,
  // then advance to next entry.

  Block block(block_data_.data(), block_data_.size());
  BlockIterator it(block);
  it.Seek(current_key_);

  // Advance past current entry (may have multiple versions)
  while (it.Valid() && it.key() == current_key_) {
    if (it.seq() == current_seq_ && it.type() == current_type_) {
      it.Next();
      break;
    }
    it.Next();
  }

  if (it.Valid()) {
    current_key_ = std::string(it.key());
    current_seq_ = it.seq();
    current_type_ = it.type();
    const std::string encoded_value(it.value());
    DecodeSSTValue(encoded_value, &current_value_,
                   &current_expires_at_ms_);
    return;
  }

  // Move to next block
  ++block_idx_;
  LoadNextBlock();
}

bool TableIterator::Valid() const noexcept {
  return valid_;
}

const std::string& TableIterator::key() const noexcept {
  return current_key_;
}

uint64_t TableIterator::seq() const noexcept {
  return current_seq_;
}

uint8_t TableIterator::type() const noexcept {
  return current_type_;
}

const std::string& TableIterator::value() const noexcept {
  return current_value_;
}

uint64_t TableIterator::expires_at_ms() const noexcept {
  return current_expires_at_ms_;
}

}  // namespace kv
