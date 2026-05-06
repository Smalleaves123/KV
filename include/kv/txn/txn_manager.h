#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "kv/common/slice.h"
#include "kv/common/status.h"
#include "kv/txn/lock_manager.h"

namespace kv::txn {

class Txn;

class TxnManager {
 public:
	TxnManager();

	std::unique_ptr<Txn> Begin();
	Status GetCommitted(const Slice& key, std::string* value) const;
	size_t Size() const;

 private:
	friend class Txn;

	Status TxnGet(Txn* txn, const Slice& key, std::string* value);
	Status TxnPut(Txn* txn, const Slice& key, const Slice& value);
	Status TxnDelete(Txn* txn, const Slice& key);
	Status TxnCommit(Txn* txn);
	Status TxnRollback(Txn* txn);

	static bool IsValidKey(const Slice& key);

	mutable std::mutex mu_;
	uint64_t next_txn_id_;
	std::unordered_map<std::string, std::string> kv_;
	LockManager lock_manager_;
};

}  // namespace kv::txn
