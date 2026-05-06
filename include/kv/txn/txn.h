#pragma once

#include <cstdint>
#include <string>

#include "kv/common/slice.h"
#include "kv/common/status.h"
#include "kv/txn/write_batch_with_index.h"

namespace kv::txn {

class TxnManager;

enum class TxnState {
	kActive = 0,
	kCommitted = 1,
	kAborted = 2,
};

class Txn {
 public:
	~Txn();

	uint64_t id() const noexcept;
	TxnState state() const noexcept;

	Status Get(const Slice& key, std::string* value);
	Status Put(const Slice& key, const Slice& value);
	Status Delete(const Slice& key);

	Status Commit();
	Status Rollback();

 private:
	friend class TxnManager;
	Txn(TxnManager* manager, uint64_t id);

	TxnManager* manager_;
	uint64_t id_;
	TxnState state_;
	WriteBatchWithIndex batch_;
};

}  // namespace kv::txn
