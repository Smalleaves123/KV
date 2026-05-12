#pragma once

#include <atomic>
#include <thread>

namespace kv {

/// A simple spin lock backed by std::atomic<bool>.
/// Spin locks are suitable for very short critical sections where
/// the overhead of a context switch (mutex) would dominate.
class SpinLock {
 public:
  SpinLock() noexcept : locked_(false) {}

  SpinLock(const SpinLock&) = delete;
  SpinLock& operator=(const SpinLock&) = delete;

  void Lock() noexcept {
    bool expected = false;
    while (!locked_.compare_exchange_weak(expected, true,
                                          std::memory_order_acquire)) {
      expected = false;
#if defined(__x86_64__) || defined(__i386__)
      __builtin_ia32_pause();
#else
      std::this_thread::yield();
#endif
    }
  }

  bool TryLock() noexcept {
    bool expected = false;
    return locked_.compare_exchange_weak(expected, true,
                                         std::memory_order_acquire);
  }

  void Unlock() noexcept {
    locked_.store(false, std::memory_order_release);
  }

 private:
  std::atomic<bool> locked_;
};

/// RAII guard for SpinLock.
class SpinLockGuard {
 public:
  explicit SpinLockGuard(SpinLock& lock) : lock_(lock) { lock_.Lock(); }
  ~SpinLockGuard() { lock_.Unlock(); }

  SpinLockGuard(const SpinLockGuard&) = delete;
  SpinLockGuard& operator=(const SpinLockGuard&) = delete;

 private:
  SpinLock& lock_;
};

}  // namespace kv
