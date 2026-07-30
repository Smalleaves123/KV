#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace kv {

// Handle to a block within the file: offset and size.
struct BlockHandle {
  uint64_t offset = 0;
  uint64_t size = 0;

  void EncodeTo(std::string* out) const;
  static BlockHandle DecodeFrom(const char* p, const char* limit, bool* ok);
};

// Footer format (stored at end of SST file):
//   index_handle    (varint64 offset + varint64 size)
//   filter_handle   (varint64 offset + varint64 size)
//   max_seq         (fixed64, for recovery without manifest)
//   magic           (fixed64, 8 bytes)
// Total fixed-size footer: 2*(max_varint64*2) + 8 + 8 ≈ 48 bytes
static constexpr size_t kFooterEncodedSize = 48;
static constexpr uint64_t kSSTMagicV0 = 0x6b765f7373745f30ULL;  // "kv_sst_0"
static constexpr uint64_t kSSTMagicV1 = 0x6b765f7373745f31ULL;  // "kv_sst_1"
static constexpr uint64_t kSSTMagic = kSSTMagicV1;

struct Footer {
  BlockHandle index_handle;
  BlockHandle filter_handle;
  uint64_t max_seq = 0;
  uint32_t format_version = 1;

  std::string Encode() const;
  static Footer DecodeFrom(std::string_view data, bool* ok);
};

}  // namespace kv
