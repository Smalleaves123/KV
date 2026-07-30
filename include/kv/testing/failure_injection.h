#pragma once

#include "kv/common/status.h"

namespace kv {
namespace testing {

enum class FailurePoint {
  kAfterSSTableWriteBeforeManifest,
  kAfterManifestAppendBeforeSync,
  kAfterManifestSyncBeforeWALCleanup,
  kDuringCompactionOutput,
  kAfterCompactionAddBeforeRemove,
};

void InjectFailure(FailurePoint point, const Status& status);
void ClearFailureInjection();
Status MaybeInjectFailure(FailurePoint point);

}  // namespace testing
}  // namespace kv
