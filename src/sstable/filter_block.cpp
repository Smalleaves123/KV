#include "kv/sstable/filter_block.h"

#include "kv/table/bloom_filter.h"

namespace kv {

namespace {

void PutFixed32(std::string* out, uint32_t v) {
  out->push_back(static_cast<char>(v & 0xFF));
  out->push_back(static_cast<char>((v >> 8) & 0xFF));
  out->push_back(static_cast<char>((v >> 16) & 0xFF));
  out->push_back(static_cast<char>((v >> 24) & 0xFF));
}

uint32_t DecodeFixed32(const char* p) {
  uint32_t v = 0;
  v |= static_cast<uint32_t>(static_cast<uint8_t>(p[0]));
  v |= static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 8;
  v |= static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 16;
  v |= static_cast<uint32_t>(static_cast<uint8_t>(p[3])) << 24;
  return v;
}

}  // namespace

FilterBlockBuilder::FilterBlockBuilder(size_t bits_per_key)
    : keys_(), bits_per_key_(bits_per_key == 0 ? 10 : bits_per_key) {}

void FilterBlockBuilder::AddKey(const std::string& key) {
  keys_.push_back(key);
}

std::string FilterBlockBuilder::Finish() {
  if (keys_.empty()) return {};

  const size_t bits = keys_.size() * bits_per_key_;
  BloomFilter bf(bits);
  for (const auto& k : keys_) {
    bf.Add(k);
  }

  const auto& data = bf.data();
  const uint32_t bits32 = static_cast<uint32_t>(bits);
  const uint32_t data_size = static_cast<uint32_t>(data.size());

  std::string out;
  out.reserve(8 + data_size);
  PutFixed32(&out, bits32);
  PutFixed32(&out, data_size);
  for (uint8_t b : data) {
    out.push_back(static_cast<char>(b));
  }

  return out;
}

void FilterBlockBuilder::Reset() {
  keys_.clear();
}

bool FilterBlockBuilder::Empty() const noexcept {
  return keys_.empty();
}

FilterBlockReader::FilterBlockReader(const char* data, size_t size)
    : data_(data), size_(size) {
  if (size_ < 8) {
    data_ = nullptr;
    size_ = 0;
  }
}

bool FilterBlockReader::MayMatch(const std::string& key) const {
  if (data_ == nullptr || size_ < 8) return true;

  const uint32_t bits32 = DecodeFixed32(data_);
  const uint32_t data_size = DecodeFixed32(data_ + 4);

  if (data_size == 0 || 8 + data_size > size_) return true;

  std::vector<uint8_t> raw(data_ + 8, data_ + 8 + data_size);
  auto bf = BloomFilter::FromRaw(bits32, raw);

  return bf.MayContain(key);
}

}  // namespace kv
