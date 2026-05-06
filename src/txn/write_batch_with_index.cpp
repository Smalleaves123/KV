#include "kv/txn/write_batch_with_index.h"

namespace kv::txn {

void WriteBatchWithIndex::Put(const Slice& key, const Slice& value) {
	const std::string k = key.ToString();
	const std::string v = value.ToString();

	ops_.push_back(Operation{OpType::kPut, k, v});
	latest_[k] = IndexedValue{false, v};
}

void WriteBatchWithIndex::Delete(const Slice& key) {
	const std::string k = key.ToString();
	ops_.push_back(Operation{OpType::kDelete, k, std::string()});
	latest_[k] = IndexedValue{true, std::string()};
}

bool WriteBatchWithIndex::GetLatest(const Slice& key,
																		std::string* value,
																		bool* deleted) const {
	auto it = latest_.find(key.ToString());
	if (it == latest_.end()) {
		return false;
	}

	if (deleted != nullptr) {
		*deleted = it->second.deleted;
	}
	if (value != nullptr && !it->second.deleted) {
		*value = it->second.value;
	}
	return true;
}

bool WriteBatchWithIndex::Empty() const noexcept {
	return ops_.empty();
}

size_t WriteBatchWithIndex::Count() const noexcept {
	return ops_.size();
}

void WriteBatchWithIndex::Clear() {
	ops_.clear();
	latest_.clear();
}

const std::vector<WriteBatchWithIndex::Operation>&
WriteBatchWithIndex::operations() const noexcept {
	return ops_;
}

}  // namespace kv::txn
