#pragma once

#include <shared_mutex>

namespace kv {

/// A read-write lock backed by std::shared_mutex.
/// Multiple readers can hold the lock simultaneously; writers get exclusive access.
class RWLock {
 public:
  RWLock() = default;
  ~RWLock() = default;

  RWLock(const RWLock&) = delete;
  RWLock& operator=(const RWLock&) = delete;

  /// Acquire exclusive (write) lock.
  void LockWrite() { mu_.lock(); }
  bool TryLockWrite() { return mu_.try_lock(); }
  void UnlockWrite() { mu_.unlock(); }

  /// Acquire shared (read) lock.
  void LockRead() { mu_.lock_shared(); }
  bool TryLockRead() { return mu_.try_lock_shared(); }
  void UnlockRead() { mu_.unlock_shared(); }

  /// Access the underlying std::shared_mutex.
  std::shared_mutex& NativeHandle() { return mu_; }
  const std::shared_mutex& NativeHandle() const { return mu_; }

 private:
  std::shared_mutex mu_;
};

/// RAII write guard for RWLock.
class WriteGuard {
 public:
  explicit WriteGuard(RWLock& lock) : lock_(lock) { lock_.LockWrite(); }
  ~WriteGuard() { lock_.UnlockWrite(); }

  WriteGuard(const WriteGuard&) = delete;
  WriteGuard& operator=(const WriteGuard&) = delete;

 private:
  RWLock& lock_;
};

/// RAII read guard for RWLock.
class ReadGuard {
 public:
  explicit ReadGuard(RWLock& lock) : lock_(lock) { lock_.LockRead(); }
  ~ReadGuard() { lock_.UnlockRead(); }

  ReadGuard(const ReadGuard&) = delete;
  ReadGuard& operator=(const ReadGuard&) = delete;

 private:
  RWLock& lock_;
};

}  // namespace kv
