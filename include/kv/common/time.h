#pragma once

#include <chrono>
#include <cstdint>

namespace kv {

inline uint64_t NowUnixMillis() noexcept {
  const auto now = std::chrono::system_clock::now().time_since_epoch();
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

inline bool IsExpired(uint64_t expires_at_ms, uint64_t now_ms) noexcept {
  return expires_at_ms != 0 && expires_at_ms <= now_ms;
}

}  // namespace kv
