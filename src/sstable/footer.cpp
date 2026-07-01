#include "kv/sstable/footer.h"

#include <cstring>

#include "kv/common/encoding.h"

namespace kv {

void BlockHandle::EncodeTo(std::string* out) const {
  EncodeFixed64(out, offset);
  EncodeFixed64(out, size);
}

BlockHandle BlockHandle::DecodeFrom(const char* p, const char* limit,
                                    bool* ok) {
  BlockHandle h;
  if (p + 16 > limit) {
    *ok = false;
    return h;
  }
  h.offset = DecodeFixed64(p);
  h.size = DecodeFixed64(p + 8);
  *ok = true;
  return h;
}

std::string Footer::Encode() const {
  std::string out;
  out.reserve(kFooterEncodedSize);

  index_handle.EncodeTo(&out);   // 16 bytes
  filter_handle.EncodeTo(&out);  // 16 bytes
  EncodeFixed64(&out, max_seq);     // 8 bytes
  EncodeFixed64(&out, kSSTMagic);   // 8 bytes

  return out;
}

Footer Footer::DecodeFrom(std::string_view data, bool* ok) {
  Footer f;
  *ok = false;

  if (data.size() < kFooterEncodedSize) {
    return f;
  }

  const char* p = data.data();
  const char* limit = p + data.size();

  const char* footer_start = limit - kFooterEncodedSize;

  // Verify magic
  uint64_t magic = DecodeFixed64(footer_start + 40);
  if (magic != kSSTMagic) {
    return f;
  }

  bool h_ok = false;
  f.index_handle = BlockHandle::DecodeFrom(footer_start, footer_start + 32, &h_ok);
  if (!h_ok) return f;

  f.filter_handle = BlockHandle::DecodeFrom(
      footer_start + 16, footer_start + 32, &h_ok);
  if (!h_ok) return f;

  f.max_seq = DecodeFixed64(footer_start + 32);

  *ok = true;
  return f;
}

}  // namespace kv
