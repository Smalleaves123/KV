#pragma once

#include <chrono>
#include <cstdint>
#include <limits>

#include "kv/common/status.h"

namespace kv {

inline uint64_t NowUnixMillis() noexcept {
  const auto now = std::chrono::system_clock::now().time_since_epoch();
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
}

inline bool IsExpired(uint64_t expires_at_ms, uint64_t now_ms) noexcept {
  return expires_at_ms != 0 && expires_at_ms <= now_ms;
}

inline Status TTLSecondsToMillis(int64_t seconds, int64_t* millis) {
  if (seconds > std::numeric_limits<int64_t>::max() / 1000) {
    return Status::InvalidArgument("ttl is too large");
  }
  *millis = seconds <= 0 ? seconds : seconds * 1000;
  return Status::OK();
}

inline Status ExpiryFromTTL(int64_t ttl_ms, uint64_t* expires_at_ms) {
  if (ttl_ms < 0) return Status::InvalidArgument("ttl must not be negative");
  if (ttl_ms == 0) {
    *expires_at_ms = 0;
    return Status::OK();
  }
  const uint64_t now_ms = NowUnixMillis();
  const uint64_t duration = static_cast<uint64_t>(ttl_ms);
  if (duration > std::numeric_limits<uint64_t>::max() - now_ms) {
    return Status::InvalidArgument("ttl is too large");
  }
  *expires_at_ms = now_ms + duration;
  return Status::OK();
}

}  // namespace kv
