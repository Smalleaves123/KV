#include "kv/common/checksum.h"

#include <array>

namespace kv {
namespace {

constexpr uint32_t kCRC32Polynomial = 0xEDB88320U;

std::array<uint32_t, 256> BuildCRC32Table() {
  std::array<uint32_t, 256> table{};
  for (uint32_t i = 0; i < table.size(); ++i) {
    uint32_t crc = i;
    for (int bit = 0; bit < 8; ++bit) {
      if ((crc & 1U) != 0) {
        crc = (crc >> 1) ^ kCRC32Polynomial;
      } else {
        crc >>= 1;
      }
    }
    table[i] = crc;
  }
  return table;
}

const std::array<uint32_t, 256>& CRC32Table() {
  static const std::array<uint32_t, 256> table = BuildCRC32Table();
  return table;
}

}  // namespace

uint32_t CRC32(const char* data, size_t size) noexcept {
  uint32_t crc = 0xFFFFFFFFU;
  const auto& table = CRC32Table();
  for (size_t i = 0; i < size; ++i) {
    const auto byte = static_cast<unsigned char>(data[i]);
    crc = (crc >> 8) ^ table[(crc ^ byte) & 0xFFU];
  }
  return crc ^ 0xFFFFFFFFU;
}

}  // namespace kv
