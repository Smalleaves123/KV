#pragma once

#include <mutex>

namespace kv {

/// A simple non-recursive mutex wrapping std::mutex.
/// The interface mirrors std::mutex so it can be used as a drop-in
/// replacement while keeping the abstraction in the kv namespace.
class Mutex {
 public:
  Mutex() = default;
  ~Mutex() = default;

  Mutex(const Mutex&) = delete;
  Mutex& operator=(const Mutex&) = delete;

  void Lock() { mu_.lock(); }
  bool TryLock() { return mu_.try_lock(); }
  void Unlock() { mu_.unlock(); }

  /// Access the underlying std::mutex for use with standard lock_guard etc.
  std::mutex& NativeHandle() { return mu_; }
  const std::mutex& NativeHandle() const { return mu_; }

 private:
  std::mutex mu_;
};

/// RAII guard for Mutex.
class MutexGuard {
 public:
  explicit MutexGuard(Mutex& m) : mu_(m) { mu_.Lock(); }
  ~MutexGuard() { mu_.Unlock(); }

  MutexGuard(const MutexGuard&) = delete;
  MutexGuard& operator=(const MutexGuard&) = delete;

 private:
  Mutex& mu_;
};

}  // namespace kv
