#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include <utility>

namespace kv {

/// A thread-safe, bounded blocking queue.
/// Producers block when the queue is full; consumers block when it is empty.
/// Useful as a work queue between producer and consumer threads.
template <typename T>
class BlockingQueue {
 public:
  /// Create a queue with the given capacity.
  /// capacity <= 0 means unbounded.
  explicit BlockingQueue(int capacity = 0)
      : capacity_(capacity > 0 ? capacity : 0), stopped_(false) {}

  BlockingQueue(const BlockingQueue&) = delete;
  BlockingQueue& operator=(const BlockingQueue&) = delete;

  /// Push an item into the queue.  Blocks if the queue is bounded and full.
  /// Returns false if the queue has been shut down.
  bool Push(T item) {
    {
      std::unique_lock<std::mutex> lk(mu_);
      not_full_.wait(lk, [this] {
        return stopped_ || capacity_ == 0 ||
               queue_.size() < static_cast<size_t>(capacity_);
      });
      if (stopped_) return false;
      queue_.push_back(std::move(item));
    }
    not_empty_.notify_one();
    return true;
  }

  /// Try to push without blocking.  Returns false if the queue is full
  /// or shut down.
  bool TryPush(T item) {
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (stopped_) return false;
      if (capacity_ > 0 && queue_.size() >= static_cast<size_t>(capacity_)) {
        return false;
      }
      queue_.push_back(std::move(item));
    }
    not_empty_.notify_one();
    return true;
  }

  /// Pop the front item.  Blocks if the queue is empty.
  /// Returns std::nullopt if the queue has been shut down and drained.
  std::optional<T> Pop() {
    std::unique_lock<std::mutex> lk(mu_);
    not_empty_.wait(lk, [this] { return stopped_ || !queue_.empty(); });
    if (stopped_ && queue_.empty()) {
      return std::nullopt;
    }
    T item = std::move(queue_.front());
    queue_.pop_front();
    lk.unlock();
    not_full_.notify_one();
    return item;
  }

  /// Try to pop without blocking.
  std::optional<T> TryPop() {
    std::unique_lock<std::mutex> lk(mu_);
    if (queue_.empty()) {
      return std::nullopt;
    }
    T item = std::move(queue_.front());
    queue_.pop_front();
    lk.unlock();
    not_full_.notify_one();
    return item;
  }

  /// Returns the current number of elements in the queue.
  size_t Size() const {
    std::lock_guard<std::mutex> lk(mu_);
    return queue_.size();
  }

  /// Returns true if the queue is empty.
  bool Empty() const {
    std::lock_guard<std::mutex> lk(mu_);
    return queue_.empty();
  }

  /// Clear all items from the queue.
  void Clear() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      queue_.clear();
    }
    not_full_.notify_all();
  }

  /// Shut down the queue.  All blocked Push/Pop calls will wake up
  /// and return false / std::nullopt.  Items remaining in the queue
  /// can still be drained via Pop / TryPop.
  void Shutdown() {
    {
      std::lock_guard<std::mutex> lk(mu_);
      stopped_ = true;
    }
    not_empty_.notify_all();
    not_full_.notify_all();
  }

 private:
  mutable std::mutex mu_;
  std::condition_variable not_empty_;
  std::condition_variable not_full_;
  std::deque<T> queue_;
  int capacity_{0};
  bool stopped_{false};
};

}  // namespace kv
