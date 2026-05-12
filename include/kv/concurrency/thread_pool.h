#pragma once

#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <thread>
#include <type_traits>
#include <vector>

#include "kv/concurrency/blocking_queue.h"

namespace kv {

/// A fixed-size thread pool for executing tasks asynchronously.
///
/// Usage:
///   ThreadPool pool(4);
///   auto f = pool.Submit([](int a, int b) { return a + b; }, 3, 4);
///   int result = f.get();  // 7
class ThreadPool {
 public:
  /// Create a thread pool with the given number of worker threads.
  /// If num_threads <= 0, defaults to std::thread::hardware_concurrency().
  explicit ThreadPool(int num_threads);
  ~ThreadPool();

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  /// Submit a callable (function, lambda, bind expression) to the pool.
  /// Returns a std::future that will hold the result once executed.
  template <typename F, typename... Args>
  auto Submit(F&& f, Args&&... args)
      -> std::future<typename std::invoke_result_t<F, Args...>> {
    using ReturnType = typename std::invoke_result_t<F, Args...>;

    auto task = std::make_shared<std::packaged_task<ReturnType()>>(
        std::bind(std::forward<F>(f), std::forward<Args>(args)...));

    std::future<ReturnType> result = task->get_future();
    queue_.Push([task]() { (*task)(); });
    return result;
  }

  /// Submit a void-returning callable without waiting for a future.
  void Execute(std::function<void()> task);

  /// Returns the number of worker threads.
  int NumThreads() const { return num_threads_; }

  /// Returns the approximate number of pending tasks.
  size_t PendingTasks() const { return queue_.Size(); }

  /// Block until the task queue is drained, then stop all workers.
  void WaitAndStop();

 private:
  void WorkerLoop();

  int num_threads_;
  BlockingQueue<std::function<void()>> queue_;
  std::vector<std::thread> workers_;
};

}  // namespace kv
