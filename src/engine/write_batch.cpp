#include "kv/engine/write_batch.h"

#include <utility>

namespace kv {

WriteBatch::WriteBatch() : operations_(), approximate_size_(0) {}

void WriteBatch::Put(const Slice& key, const Slice& value) {
  Operation op;
  op.type = ValueType::kPut;
  op.key = key.ToString();
  op.value = value.ToString();

  approximate_size_ += op.key.size();
  approximate_size_ += op.value.size();
  approximate_size_ += 1;

  operations_.push_back(std::move(op));
}

void WriteBatch::Delete(const Slice& key) {
  Operation op;
  op.type = ValueType::kDelete;
  op.key = key.ToString();
  op.value.clear();

  approximate_size_ += op.key.size();
  approximate_size_ += 1;

  operations_.push_back(std::move(op));
}

void WriteBatch::Clear() {
  operations_.clear();
  approximate_size_ = 0;
}

void WriteBatch::Append(const WriteBatch& other) {
  operations_.reserve(operations_.size() + other.operations_.size());
  for (const auto& op : other.operations_) {
    operations_.push_back(op);
  }
  approximate_size_ += other.approximate_size_;
}

bool WriteBatch::Empty() const noexcept {
  return operations_.empty();
}

size_t WriteBatch::Count() const noexcept {
  return operations_.size();
}

size_t WriteBatch::ApproximateSize() const noexcept {
  return approximate_size_;
}

const std::vector<WriteBatch::Operation>& WriteBatch::operations() const noexcept {
  return operations_;
}

Status WriteBatch::Iterate(Handler* handler) const {
  if (handler == nullptr) {
    return Status::InvalidArgument("write batch handler is null");
  }

  for (const auto& op : operations_) {
    Status s;
    if (op.type == ValueType::kPut) {
      s = handler->Put(op.key, op.value);
    } else {
      s = handler->Delete(op.key);
    }
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

}  // namespace kv
