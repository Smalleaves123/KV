#pragma once

#include <condition_variable>
#include <mutex>

namespace kv {

/// A countdown latch — a synchronisation primitive that blocks one or more
/// threads until a counter reaches zero.  Commonly used to signal that a set
/// of operations has completed.
class Latch {
 public:
  /// Creates a latch with the given initial count.
  explicit Latch(int count) : count_(count) {}

  Latch(const Latch&) = delete;
  Latch& operator=(const Latch&) = delete;

  /// Decrement the count.  If the count reaches zero, all waiting threads
  /// are woken.
  void CountDown() {
    std::lock_guard<std::mutex> lk(mu_);
    if (count_ > 0) {
      --count_;
    }
    if (count_ == 0) {
      cv_.notify_all();
    }
  }

  /// Block until the count reaches zero.
  void Wait() {
    std::unique_lock<std::mutex> lk(mu_);
    cv_.wait(lk, [this] { return count_ == 0; });
  }

  /// Returns the current count (useful for debugging / assertions).
  int Count() const {
    std::lock_guard<std::mutex> lk(mu_);
    return count_;
  }

 private:
  mutable std::mutex mu_;
  std::condition_variable cv_;
  int count_{0};
};

}  // namespace kv
