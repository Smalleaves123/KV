#include "kv/sstable/value_codec.h"

#include "kv/common/encoding.h"

namespace kv {
namespace {

constexpr char kMagic[] = "KVTTL001";
constexpr size_t kMagicSize = sizeof(kMagic) - 1;

}  // namespace

std::string EncodeSSTValue(const std::string& value, uint64_t expires_at_ms) {
  if (expires_at_ms == 0) {
    return value;
  }

  std::string encoded;
  encoded.reserve(kMagicSize + sizeof(uint64_t) + value.size());
  encoded.append(kMagic, kMagicSize);
  EncodeFixed64(&encoded, expires_at_ms);
  encoded.append(value);
  return encoded;
}

void DecodeSSTValue(const std::string& encoded, std::string* value,
                   uint64_t* expires_at_ms) {
  if (value == nullptr && expires_at_ms == nullptr) {
    return;
  }

  if (expires_at_ms != nullptr) *expires_at_ms = 0;
  if (encoded.size() < kMagicSize + sizeof(uint64_t) ||
      encoded.compare(0, kMagicSize, kMagic, kMagicSize) != 0) {
    if (value != nullptr) *value = encoded;
    return;
  }

  const uint64_t expiry = DecodeFixed64(encoded.data() + kMagicSize);
  if (expiry == 0) {
    if (value != nullptr) *value = encoded;
    return;
  }

  if (expires_at_ms != nullptr) *expires_at_ms = expiry;
  if (value != nullptr) {
    value->assign(encoded.data() + kMagicSize + sizeof(uint64_t),
                  encoded.size() - kMagicSize - sizeof(uint64_t));
  }
}

}  // namespace kv
