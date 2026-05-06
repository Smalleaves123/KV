#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "kv/common/slice.h"

namespace kv::txn {

class WriteBatchWithIndex {
 public:
	enum class OpType {
		kPut = 0,
		kDelete = 1,
	};

	struct Operation {
		OpType type = OpType::kPut;
		std::string key;
		std::string value;
	};

	void Put(const Slice& key, const Slice& value);
	void Delete(const Slice& key);

	bool GetLatest(const Slice& key, std::string* value, bool* deleted) const;
	bool Empty() const noexcept;
	size_t Count() const noexcept;
	void Clear();

	const std::vector<Operation>& operations() const noexcept;

 private:
	struct IndexedValue {
		bool deleted = false;
		std::string value;
	};

	std::vector<Operation> ops_;
	std::unordered_map<std::string, IndexedValue> latest_;
};

}  // namespace kv::txn
