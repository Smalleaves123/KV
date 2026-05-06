#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace kv::txn {

class LockManager {
 public:
	bool TryLockShared(uint64_t owner_id, const std::string& key);
	bool TryLockExclusive(uint64_t owner_id, const std::string& key);

	void Unlock(uint64_t owner_id, const std::string& key);
	void UnlockAll(uint64_t owner_id);

	bool IsLocked(const std::string& key) const;
	size_t HeldBy(uint64_t owner_id) const;

 private:
	struct LockState {
		uint64_t writer = 0;
		std::unordered_set<uint64_t> readers;
	};

	mutable std::mutex mu_;
	std::unordered_map<std::string, LockState> locks_;
	std::unordered_map<uint64_t, std::unordered_set<std::string>> owned_keys_;
};

}  // namespace kv::txn
