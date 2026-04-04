#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "kv/common/slice.h"
#include "kv/common/status.h"

namespace kv {

class WriteBatch {
 public:
  enum class ValueType : uint8_t {
    kPut = 0,
    kDelete = 1,
  };

  struct Operation {
    ValueType type = ValueType::kPut;
    std::string key;
    std::string value;
  };

  class Handler {
   public:
    virtual ~Handler() = default;

    virtual Status Put(const Slice& key, const Slice& value) = 0;
    virtual Status Delete(const Slice& key) = 0;
  };

  WriteBatch();

  void Put(const Slice& key, const Slice& value);
  void Delete(const Slice& key);

  void Clear();
  void Append(const WriteBatch& other);

  bool Empty() const noexcept;
  size_t Count() const noexcept;
  size_t ApproximateSize() const noexcept;
  const std::vector<Operation>& operations() const noexcept;

  Status Iterate(Handler* handler) const;

 private:
  std::vector<Operation> operations_;
  size_t approximate_size_;
};

}  // namespace kv
