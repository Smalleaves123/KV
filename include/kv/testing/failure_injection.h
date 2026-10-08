#pragma once

#include <functional>

#include "kv/common/status.h"

namespace kv {
namespace testing {

enum class FailurePoint {
  kAfterSSTableWriteBeforeManifest,
  kAfterManifestAppendBeforeSync,
  kAfterManifestSyncBeforeWALCleanup,
  kDuringCompactionOutput,
  kAfterCompactionAddBeforeRemove,
  kDuringCheckpointStage,
  kDuringCheckpointPublish,
  kAfterCheckpointPublishBeforeInit,
};

void InjectFailure(FailurePoint point, const Status& status);
// One-shot callback at a failure point, invoked without the injection mutex.
// In non-test builds this is a no-op, like failure injection itself.
void SetFailurePointHook(FailurePoint point, std::function<void()> hook);
void ClearFailureInjection();
Status MaybeInjectFailure(FailurePoint point);

}  // namespace testing
}  // namespace kv
