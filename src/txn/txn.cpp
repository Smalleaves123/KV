#include "kv/txn/txn.h"

#include "kv/txn/txn_manager.h"

namespace kv::txn {

Txn::Txn(TxnManager* manager, uint64_t id)
		: manager_(manager), id_(id), state_(TxnState::kActive), batch_() {}

Txn::~Txn() {
	if (state_ == TxnState::kActive && manager_ != nullptr) {
		(void)manager_->TxnRollback(this);
	}
}

uint64_t Txn::id() const noexcept {
	return id_;
}

TxnState Txn::state() const noexcept {
	return state_;
}

Status Txn::Get(const Slice& key, std::string* value) {
	if (manager_ == nullptr) {
		return Status::IOError("transaction manager is null");
	}
	return manager_->TxnGet(this, key, value);
}

Status Txn::Put(const Slice& key, const Slice& value) {
	if (manager_ == nullptr) {
		return Status::IOError("transaction manager is null");
	}
	return manager_->TxnPut(this, key, value);
}

Status Txn::Delete(const Slice& key) {
	if (manager_ == nullptr) {
		return Status::IOError("transaction manager is null");
	}
	return manager_->TxnDelete(this, key);
}

Status Txn::Commit() {
	if (manager_ == nullptr) {
		return Status::IOError("transaction manager is null");
	}
	return manager_->TxnCommit(this);
}

Status Txn::Rollback() {
	if (manager_ == nullptr) {
		return Status::IOError("transaction manager is null");
	}
	return manager_->TxnRollback(this);
}

}  // namespace kv::txn
