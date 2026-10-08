#include "kv/testing/failure_injection.h"

#include <mutex>
#include <utility>

namespace kv {
namespace testing {
namespace {

#ifdef KV_ENABLE_FAILURE_INJECTION
struct InjectedFailure {
  bool enabled = false;
  FailurePoint point = FailurePoint::kAfterSSTableWriteBeforeManifest;
  Status status;
};

struct InjectedHook {
  FailurePoint point = FailurePoint::kDuringCompactionOutput;
  std::function<void()> callback;
};

InjectedHook& HookState() {
  static InjectedHook hook;
  return hook;
}

std::mutex& FailureMutex() {
  static std::mutex mu;
  return mu;
}

InjectedFailure& FailureState() {
  static InjectedFailure failure;
  return failure;
}
#endif

}  // namespace

void InjectFailure(FailurePoint point, const Status& status) {
#ifdef KV_ENABLE_FAILURE_INJECTION
  std::lock_guard<std::mutex> lk(FailureMutex());
  InjectedFailure& failure = FailureState();
  failure.enabled = true;
  failure.point = point;
  failure.status = status.ok() ? Status::IOError("injected failure")
                               : status;
#else
  (void)point;
  (void)status;
#endif
}

void ClearFailureInjection() {
#ifdef KV_ENABLE_FAILURE_INJECTION
  std::lock_guard<std::mutex> lk(FailureMutex());
  FailureState() = InjectedFailure{};
  HookState() = InjectedHook{};
#endif
}

void SetFailurePointHook(FailurePoint point, std::function<void()> hook) {
#ifdef KV_ENABLE_FAILURE_INJECTION
  std::lock_guard<std::mutex> lk(FailureMutex());
  HookState() = InjectedHook{point, std::move(hook)};
#else
  (void)point;
  (void)hook;
#endif
}

Status MaybeInjectFailure(FailurePoint point) {
#ifdef KV_ENABLE_FAILURE_INJECTION
  std::function<void()> hook;
  Status status = Status::OK();
  {
    std::lock_guard<std::mutex> lk(FailureMutex());
    if (HookState().point == point) {
      hook = std::move(HookState().callback);
    }
    InjectedFailure& failure = FailureState();
    if (failure.enabled && failure.point == point) {
      status = failure.status;
      failure = InjectedFailure{};
    }
  }
  if (hook) hook();
  return status;
#else
  (void)point;
  return Status::OK();
#endif
}

}  // namespace testing
}  // namespace kv
