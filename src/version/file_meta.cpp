#include "kv/version/file_meta.h"

namespace kv {

bool FileMeta::IsValid() const noexcept {
  return file_number > 0 && !file_path.empty() &&
         smallest_key.empty() == largest_key.empty() &&
         (!HasKeyRange() || smallest_key <= largest_key);
}

bool FileMeta::HasKeyRange() const noexcept {
  return !smallest_key.empty() && !largest_key.empty();
}

}  // namespace kv
