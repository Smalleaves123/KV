#include "kv/sstable/footer.h"

#include <cstring>

namespace kv {

static void PutFixed64(std::string* out, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    out->push_back(static_cast<char>(v & 0xFF));
    v >>= 8;
  }
}

static uint64_t DecodeFixed64(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) {
    v |= (static_cast<uint64_t>(static_cast<uint8_t>(p[i]))) << (8 * i);
  }
  return v;
}

void BlockHandle::EncodeTo(std::string* out) const {
  PutFixed64(out, offset);
  PutFixed64(out, size);
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
  PutFixed64(&out, max_seq);     // 8 bytes
  PutFixed64(&out, kSSTMagic);   // 8 bytes

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
