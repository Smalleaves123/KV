#include "kv/version/file_meta.h"

namespace kv {

bool FileMeta::IsValid() const noexcept {
  return file_number > 0 && !file_path.empty();
}

}  // namespace kv
