#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace kv {

class BlockBuilder {
 public:
  explicit BlockBuilder(size_t restart_interval = 16);

  void Add(const std::string& key, uint64_t seq, uint8_t type,
           const std::string& value);
  std::string Finish();
  void Reset();
  size_t Size() const noexcept;
  bool Empty() const noexcept;

 private:
  void EmitRestartPoint();

  std::string buffer_;
  std::string last_key_;
  std::vector<uint32_t> restarts_;
  size_t restart_interval_;
  size_t entry_count_;
  bool finished_;
};

}  // namespace kv
