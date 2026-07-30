#include "kv/testing/failure_injection.h"

#include <mutex>

namespace kv {
namespace testing {
namespace {

struct InjectedFailure {
  bool enabled = false;
  FailurePoint point = FailurePoint::kAfterSSTableWriteBeforeManifest;
  Status status;
};

std::mutex& FailureMutex() {
  static std::mutex mu;
  return mu;
}

InjectedFailure& FailureState() {
  static InjectedFailure failure;
  return failure;
}

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
#endif
}

Status MaybeInjectFailure(FailurePoint point) {
#ifdef KV_ENABLE_FAILURE_INJECTION
  std::lock_guard<std::mutex> lk(FailureMutex());
  InjectedFailure& failure = FailureState();
  if (!failure.enabled || failure.point != point) {
    return Status::OK();
  }

  Status status = failure.status;
  failure = InjectedFailure{};
  return status;
#else
  (void)point;
  return Status::OK();
#endif
}

}  // namespace testing
}  // namespace kv
