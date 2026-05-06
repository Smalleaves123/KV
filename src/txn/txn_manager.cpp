#include "kv/txn/txn_manager.h"

#include "kv/txn/txn.h"

namespace kv::txn {

TxnManager::TxnManager() : next_txn_id_(1), kv_(), lock_manager_() {}

std::unique_ptr<Txn> TxnManager::Begin() {
	std::lock_guard<std::mutex> lk(mu_);
	return std::unique_ptr<Txn>(new Txn(this, next_txn_id_++));
}

Status TxnManager::GetCommitted(const Slice& key, std::string* value) const {
	if (value == nullptr) {
		return Status::InvalidArgument("value is null");
	}
	if (!IsValidKey(key)) {
		return Status::InvalidArgument("empty key");
	}

	std::lock_guard<std::mutex> lk(mu_);
	auto it = kv_.find(key.ToString());
	if (it == kv_.end()) {
		return Status::NotFound("key not found");
	}
	*value = it->second;
	return Status::OK();
}

size_t TxnManager::Size() const {
	std::lock_guard<std::mutex> lk(mu_);
	return kv_.size();
}

Status TxnManager::TxnGet(Txn* txn, const Slice& key, std::string* value) {
	if (txn == nullptr || value == nullptr) {
		return Status::InvalidArgument("invalid argument");
	}
	if (txn->state_ != TxnState::kActive) {
		return Status::InvalidArgument("transaction already finished");
	}
	if (!IsValidKey(key)) {
		return Status::InvalidArgument("empty key");
	}

	bool deleted = false;
	if (txn->batch_.GetLatest(key, value, &deleted)) {
		if (deleted) {
			return Status::NotFound("key deleted in txn");
		}
		return Status::OK();
	}

	return GetCommitted(key, value);
}

Status TxnManager::TxnPut(Txn* txn, const Slice& key, const Slice& value) {
	if (txn == nullptr) {
		return Status::InvalidArgument("txn is null");
	}
	if (txn->state_ != TxnState::kActive) {
		return Status::InvalidArgument("transaction already finished");
	}
	if (!IsValidKey(key)) {
		return Status::InvalidArgument("empty key");
	}

	const std::string k = key.ToString();
	if (!lock_manager_.TryLockExclusive(txn->id_, k)) {
		return Status::AlreadyExists("write conflict");
	}

	txn->batch_.Put(key, value);
	return Status::OK();
}

Status TxnManager::TxnDelete(Txn* txn, const Slice& key) {
	if (txn == nullptr) {
		return Status::InvalidArgument("txn is null");
	}
	if (txn->state_ != TxnState::kActive) {
		return Status::InvalidArgument("transaction already finished");
	}
	if (!IsValidKey(key)) {
		return Status::InvalidArgument("empty key");
	}

	const std::string k = key.ToString();
	if (!lock_manager_.TryLockExclusive(txn->id_, k)) {
		return Status::AlreadyExists("write conflict");
	}

	txn->batch_.Delete(key);
	return Status::OK();
}

Status TxnManager::TxnCommit(Txn* txn) {
	if (txn == nullptr) {
		return Status::InvalidArgument("txn is null");
	}
	if (txn->state_ != TxnState::kActive) {
		return Status::InvalidArgument("transaction already finished");
	}

	{
		std::lock_guard<std::mutex> lk(mu_);
		for (const auto& op : txn->batch_.operations()) {
			if (op.type == WriteBatchWithIndex::OpType::kPut) {
				kv_[op.key] = op.value;
			} else {
				kv_.erase(op.key);
			}
		}
	}

	txn->batch_.Clear();
	txn->state_ = TxnState::kCommitted;
	lock_manager_.UnlockAll(txn->id_);
	return Status::OK();
}

Status TxnManager::TxnRollback(Txn* txn) {
	if (txn == nullptr) {
		return Status::InvalidArgument("txn is null");
	}
	if (txn->state_ != TxnState::kActive) {
		return Status::InvalidArgument("transaction already finished");
	}

	txn->batch_.Clear();
	txn->state_ = TxnState::kAborted;
	lock_manager_.UnlockAll(txn->id_);
	return Status::OK();
}

bool TxnManager::IsValidKey(const Slice& key) {
	return !key.empty();
}

}  // namespace kv::txn
