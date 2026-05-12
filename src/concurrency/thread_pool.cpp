#include "kv/concurrency/thread_pool.h"

#include <thread>

namespace kv {

ThreadPool::ThreadPool(int num_threads)
    : num_threads_(num_threads > 0 ? num_threads
                                   : static_cast<int>(std::thread::hardware_concurrency())),
      queue_() {
  for (int i = 0; i < num_threads_; ++i) {
    workers_.emplace_back(&ThreadPool::WorkerLoop, this);
  }
}

ThreadPool::~ThreadPool() {
  WaitAndStop();
}

void ThreadPool::Execute(std::function<void()> task) {
  queue_.Push(std::move(task));
}

void ThreadPool::WaitAndStop() {
  queue_.Shutdown();
  for (auto& w : workers_) {
    if (w.joinable()) {
      w.join();
    }
  }
}

void ThreadPool::WorkerLoop() {
  while (true) {
    auto opt = queue_.Pop();
    if (!opt.has_value()) {
      break;  // queue shut down and drained
    }
    (*opt)();
  }
}

}  // namespace kv
