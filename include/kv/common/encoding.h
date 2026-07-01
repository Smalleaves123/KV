#pragma once

#include <cstdint>
#include <string>

namespace kv {

// Little-endian fixed-width encoding/decoding utilities.
// Used across WAL, SSTable, manifest, and Raft storage.

inline void EncodeFixed32(std::string* out, uint32_t value) {
  out->push_back(static_cast<char>(value & 0xFF));
  out->push_back(static_cast<char>((value >> 8) & 0xFF));
  out->push_back(static_cast<char>((value >> 16) & 0xFF));
  out->push_back(static_cast<char>((value >> 24) & 0xFF));
}

inline void EncodeFixed64(std::string* out, uint64_t value) {
  for (int i = 0; i < 8; ++i) {
    out->push_back(static_cast<char>((value >> (8 * i)) & 0xFF));
  }
}

inline uint32_t DecodeFixed32(const char* p) {
  return static_cast<uint32_t>(static_cast<unsigned char>(p[0])) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[1])) << 8) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[2])) << 16) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[3])) << 24);
}

inline uint64_t DecodeFixed64(const char* p) {
  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value |= (static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i));
  }
  return value;
}

}  // namespace kv
