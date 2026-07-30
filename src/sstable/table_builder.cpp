#include "kv/sstable/table_builder.h"

#include <algorithm>

#include "kv/common/checksum.h"
#include "kv/common/encoding.h"
#include "kv/common/file_compat.h"
#include "kv/sstable/block_builder.h"
#include "kv/sstable/footer.h"
#include "kv/sstable/value_codec.h"

namespace kv {

static void PutFixed64(std::string* out, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    out->push_back(static_cast<char>(v & 0xFF));
    v >>= 8;
  }
}

TableBuilder::TableBuilder(const std::string& file_path,
                           size_t block_size,
                           size_t bloom_bits_per_key)
    : file_path_(file_path),
      file_(),
      block_size_(std::max<size_t>(block_size, 256)),
      offset_(0),
      max_seq_(0),
      finished_(false),
      abandoned_(false),
      data_block_(std::make_unique<BlockBuilder>()),
      last_key_(),
      index_entries_(),
      filter_builder_(bloom_bits_per_key) {
  file_.open(file_path_, std::ios::binary | std::ios::trunc);
}

TableBuilder::~TableBuilder() {
  if (!finished_ && !abandoned_ && file_.is_open()) {
    file_.close();
    std::remove(file_path_.c_str());
  }
}

Status TableBuilder::Add(const std::string& key, uint64_t seq, uint8_t type,
                         const std::string& value) {
  return Add(key, seq, type, 0, value);
}

Status TableBuilder::Add(const std::string& key, uint64_t seq, uint8_t type,
                         uint64_t expires_at_ms,
                         const std::string& value) {
  if (finished_) {
    return Status::AlreadyExists("table builder already finished");
  }
  if (!file_.is_open()) {
    return Status::IOError("table builder file is not open");
  }
  if (!last_key_.empty() && key < last_key_) {
    return Status::InvalidArgument("keys must be added in ascending order");
  }

  data_block_->Add(key, seq, type, EncodeSSTValue(value, expires_at_ms));
  filter_builder_.AddKey(key);
  max_seq_ = std::max(max_seq_, seq);
  last_key_ = key;

  if (data_block_->Size() >= block_size_) {
    return FlushDataBlock();
  }

  return Status::OK();
}

Status TableBuilder::FlushDataBlock() {
  if (data_block_->Empty()) return Status::OK();

  std::string block_data = data_block_->Finish();

  file_.write(block_data.data(), static_cast<std::streamsize>(block_data.size()));
  if (!file_) {
    return Status::IOError("failed to write data block");
  }
  std::string trailer;
  EncodeFixed32(&trailer, CRC32(block_data.data(), block_data.size()));
  file_.write(trailer.data(), static_cast<std::streamsize>(trailer.size()));
  if (!file_) {
    return Status::IOError("failed to write data block checksum");
  }

  index_entries_.push_back({last_key_, offset_, block_data.size()});

  offset_ += block_data.size() + trailer.size();
  data_block_->Reset();
  last_key_.clear();

  return Status::OK();
}

Status TableBuilder::Finish() {
  if (finished_) return Status::OK();
  if (!file_.is_open()) {
    return Status::IOError("table builder file is not open");
  }

  // Flush final data block
  Status s = FlushDataBlock();
  if (!s.ok()) return s;

  // Write filter block
  const uint64_t filter_offset = offset_;
  std::string filter_data = filter_builder_.Finish();
  if (!filter_data.empty()) {
    file_.write(filter_data.data(),
                static_cast<std::streamsize>(filter_data.size()));
    if (!file_) {
      return Status::IOError("failed to write filter block");
    }
    offset_ += filter_data.size();
  }

  // Build and write index block
  BlockBuilder index_block;
  for (const auto& ie : index_entries_) {
    std::string handle_value;
    PutFixed64(&handle_value, ie.block_offset);
    PutFixed64(&handle_value, ie.block_size);
    index_block.Add(ie.last_key, 0, 0, handle_value);
  }

  const uint64_t index_offset = offset_;
  std::string index_data = index_block.Finish();
  file_.write(index_data.data(),
              static_cast<std::streamsize>(index_data.size()));
  if (!file_) {
    return Status::IOError("failed to write index block");
  }
  offset_ += index_data.size();

  // Write footer
  Footer footer;
  footer.index_handle.offset = index_offset;
  footer.index_handle.size = index_data.size();
  footer.filter_handle.offset = filter_offset;
  footer.filter_handle.size = filter_data.size();
  footer.max_seq = max_seq_;

  std::string footer_data = footer.Encode();
  file_.write(footer_data.data(),
              static_cast<std::streamsize>(footer_data.size()));
  if (!file_) {
    return Status::IOError("failed to write footer");
  }

  file_.flush();
  if (!file_) {
    return Status::IOError("failed to flush table file");
  }

  const int sync_fd = platform::OpenSyncFile(file_path_, true);
  if (sync_fd < 0) {
    return Status::IOError("failed to open table fd for sync: " + file_path_ +
                           ": " + platform::FileErrorString());
  }
  const int sync_result = platform::SyncFile(sync_fd);
  const std::string sync_error = sync_result == 0 ? "" : platform::FileErrorString();
  const int close_result = platform::CloseFile(sync_fd);
  if (sync_result != 0) {
    return Status::IOError("failed to sync table file: " + file_path_ + ": " +
                           sync_error);
  }
  if (close_result != 0) {
    return Status::IOError("failed to close table sync fd: " + file_path_ + ": " +
                           platform::FileErrorString());
  }

  file_.close();
  if (file_.fail()) {
    return Status::IOError("failed to close table file");
  }
  finished_ = true;
  return Status::OK();
}

uint64_t TableBuilder::FileSize() const noexcept {
  return offset_;
}

void TableBuilder::Abandon() {
  if (!finished_ && file_.is_open()) {
    file_.close();
    std::remove(file_path_.c_str());
    abandoned_ = true;
  }
}

}  // namespace kv
