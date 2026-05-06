#include "kv/txn/lock_manager.h"

namespace kv::txn {

bool LockManager::TryLockShared(uint64_t owner_id, const std::string& key) {
	if (owner_id == 0 || key.empty()) {
		return false;
	}

	std::lock_guard<std::mutex> lk(mu_);
	LockState& st = locks_[key];
	if (st.writer != 0 && st.writer != owner_id) {
		return false;
	}

	st.readers.insert(owner_id);
	owned_keys_[owner_id].insert(key);
	return true;
}

bool LockManager::TryLockExclusive(uint64_t owner_id, const std::string& key) {
	if (owner_id == 0 || key.empty()) {
		return false;
	}

	std::lock_guard<std::mutex> lk(mu_);
	LockState& st = locks_[key];
	if (st.writer != 0 && st.writer != owner_id) {
		return false;
	}

	if (!st.readers.empty()) {
		if (st.readers.size() > 1 || st.readers.find(owner_id) == st.readers.end()) {
			return false;
		}
	}

	st.writer = owner_id;
	st.readers.erase(owner_id);
	owned_keys_[owner_id].insert(key);
	return true;
}

void LockManager::Unlock(uint64_t owner_id, const std::string& key) {
	if (owner_id == 0 || key.empty()) {
		return;
	}

	std::lock_guard<std::mutex> lk(mu_);
	auto lock_it = locks_.find(key);
	if (lock_it != locks_.end()) {
		LockState& st = lock_it->second;
		if (st.writer == owner_id) {
			st.writer = 0;
		}
		st.readers.erase(owner_id);
		if (st.writer == 0 && st.readers.empty()) {
			locks_.erase(lock_it);
		}
	}

	auto own_it = owned_keys_.find(owner_id);
	if (own_it != owned_keys_.end()) {
		own_it->second.erase(key);
		if (own_it->second.empty()) {
			owned_keys_.erase(own_it);
		}
	}
}

void LockManager::UnlockAll(uint64_t owner_id) {
	if (owner_id == 0) {
		return;
	}

	std::lock_guard<std::mutex> lk(mu_);
	auto own_it = owned_keys_.find(owner_id);
	if (own_it == owned_keys_.end()) {
		return;
	}

	for (const std::string& key : own_it->second) {
		auto lock_it = locks_.find(key);
		if (lock_it == locks_.end()) {
			continue;
		}

		LockState& st = lock_it->second;
		if (st.writer == owner_id) {
			st.writer = 0;
		}
		st.readers.erase(owner_id);
		if (st.writer == 0 && st.readers.empty()) {
			locks_.erase(lock_it);
		}
	}

	owned_keys_.erase(own_it);
}

bool LockManager::IsLocked(const std::string& key) const {
	std::lock_guard<std::mutex> lk(mu_);
	auto it = locks_.find(key);
	if (it == locks_.end()) {
		return false;
	}
	return it->second.writer != 0 || !it->second.readers.empty();
}

size_t LockManager::HeldBy(uint64_t owner_id) const {
	std::lock_guard<std::mutex> lk(mu_);
	auto it = owned_keys_.find(owner_id);
	if (it == owned_keys_.end()) {
		return 0;
	}
	return it->second.size();
}

}  // namespace kv::txn
