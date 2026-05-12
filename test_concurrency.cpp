// Compile check for concurrency headers
#include "kv/concurrency/mutex.h"
#include "kv/concurrency/rw_lock.h"
#include "kv/concurrency/spin_lock.h"
#include "kv/concurrency/latch.h"
#include "kv/concurrency/blocking_queue.h"
#include "kv/concurrency/thread_pool.h"

#include <cassert>
#include <iostream>
#include <thread>

int main() {
  using namespace kv;

  // --- Mutex ---
  {
    Mutex m;
    m.Lock();
    m.Unlock();
    assert(m.TryLock());
    m.Unlock();
    {
      MutexGuard g(m);
    }
  }

  // --- RWLock ---
  {
    RWLock rw;
    rw.LockRead();
    rw.UnlockRead();
    rw.LockWrite();
    rw.UnlockWrite();
    {
      ReadGuard rg(rw);
    }
    {
      WriteGuard wg(rw);
    }
  }

  // --- SpinLock ---
  {
    SpinLock sl;
    sl.Lock();
    sl.Unlock();
    assert(sl.TryLock());
    sl.Unlock();
    {
      SpinLockGuard sg(sl);
    }
  }

  // --- Latch ---
  {
    Latch latch(2);
    std::thread t1([&] { latch.CountDown(); });
    std::thread t2([&] { latch.CountDown(); });
    t1.join();
    t2.join();
    latch.Wait(); // should return immediately
    assert(latch.Count() == 0);
  }

  // --- BlockingQueue ---
  {
    BlockingQueue<int> q(4);
    q.Push(1);
    q.Push(2);
    assert(q.Size() == 2);
    assert(q.Pop() == 1);
    assert(q.Pop() == 2);
    assert(q.Empty());

    assert(q.TryPush(3));
    auto val = q.TryPop();
    assert(val.has_value() && *val == 3);
    assert(q.TryPop().has_value() == false);
  }

  // --- ThreadPool ---
  {
    ThreadPool pool(2);
    auto f1 = pool.Submit([](int x) { return x * 2; }, 21);
    auto f2 = pool.Submit([](int a, int b) { return a + b; }, 10, 20);
    assert(f1.get() == 42);
    assert(f2.get() == 30);

    bool executed = false;
    pool.Execute([&] { executed = true; });
    pool.WaitAndStop();
    assert(executed);
  }

  std::cout << "All concurrency primitives pass!" << std::endl;
  return 0;
}
