#include "kv/engine/snapshot.h"

#include <cstdint>
#include <memory>

namespace kv {
namespace {

class SnapshotImpl final : public Snapshot {
public:
  explicit SnapshotImpl(uint64_t seq) : seq_(seq) {}
  uint64_t sequence() const noexcept override { return seq_; }

private:
  uint64_t seq_;
};

} // namespace

std::unique_ptr<Snapshot> NewSnapshot(uint64_t seq) {
  return std::make_unique<SnapshotImpl>(seq);
}

} // namespace kv
