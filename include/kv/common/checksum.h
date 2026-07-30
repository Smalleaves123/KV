#pragma once

#include <cstddef>
#include <cstdint>

namespace kv {

uint32_t CRC32(const char* data, size_t size) noexcept;

}  // namespace kv
