#include "kv/cluster/hash_ring.h"

#include <algorithm>
#include <unordered_set>

namespace kv {

HashRing::HashRing(size_t virtual_nodes_per_weight)
		: virtual_nodes_per_weight_(std::max<size_t>(1, virtual_nodes_per_weight)) {}

bool HashRing::AddNode(const NodeInfo& node) {
	if (!node.IsValid()) {
		return false;
	}

	std::lock_guard<std::mutex> lk(mu_);
	RemoveVirtualNodesLocked(node.id);
	nodes_[node.id] = node;
	if (node.alive) {
		AddVirtualNodesLocked(node);
	}
	return true;
}

bool HashRing::RemoveNode(const std::string& node_id) {
	std::lock_guard<std::mutex> lk(mu_);
	auto it = nodes_.find(node_id);
	if (it == nodes_.end()) {
		return false;
	}
	RemoveVirtualNodesLocked(node_id);
	nodes_.erase(it);
	return true;
}

bool HashRing::SetNodeAlive(const std::string& node_id, bool alive) {
	std::lock_guard<std::mutex> lk(mu_);
	auto it = nodes_.find(node_id);
	if (it == nodes_.end()) {
		return false;
	}
	if (it->second.alive == alive) {
		return true;
	}

	RemoveVirtualNodesLocked(node_id);
	it->second.alive = alive;
	if (alive) {
		AddVirtualNodesLocked(it->second);
	}
	return true;
}

std::optional<NodeInfo> HashRing::Lookup(const std::string& key) const {
	std::lock_guard<std::mutex> lk(mu_);
	if (ring_.empty()) {
		return std::nullopt;
	}

	const uint64_t h = Hash64(key);
	auto it = ring_.lower_bound(h);
	if (it == ring_.end()) {
		it = ring_.begin();
	}

	auto node_it = nodes_.find(it->second);
	if (node_it == nodes_.end()) {
		return std::nullopt;
	}
	return node_it->second;
}

std::vector<NodeInfo> HashRing::LookupN(const std::string& key, size_t n) const {
	std::vector<NodeInfo> out;
	if (n == 0) {
		return out;
	}

	std::lock_guard<std::mutex> lk(mu_);
	if (ring_.empty()) {
		return out;
	}

	const size_t target = std::min(n, nodes_.size());
	out.reserve(target);

	std::unordered_set<std::string> picked;
	const uint64_t h = Hash64(key);
	auto it = ring_.lower_bound(h);
	if (it == ring_.end()) {
		it = ring_.begin();
	}

	size_t walked = 0;
	while (!ring_.empty() && out.size() < target && walked < ring_.size()) {
		const std::string& node_id = it->second;
		if (picked.insert(node_id).second) {
			auto node_it = nodes_.find(node_id);
			if (node_it != nodes_.end() && node_it->second.alive) {
				out.push_back(node_it->second);
			}
		}

		++it;
		if (it == ring_.end()) {
			it = ring_.begin();
		}
		++walked;
	}

	return out;
}

std::vector<NodeInfo> HashRing::Nodes() const {
	std::lock_guard<std::mutex> lk(mu_);
	std::vector<NodeInfo> out;
	out.reserve(nodes_.size());
	for (const auto& kv : nodes_) {
		out.push_back(kv.second);
	}
	std::sort(out.begin(), out.end(), [](const NodeInfo& a, const NodeInfo& b) {
		return a.id < b.id;
	});
	return out;
}

size_t HashRing::NodeCount() const {
	std::lock_guard<std::mutex> lk(mu_);
	return nodes_.size();
}

size_t HashRing::ActiveNodeCount() const {
	std::lock_guard<std::mutex> lk(mu_);
	size_t active = 0;
	for (const auto& kv : nodes_) {
		if (kv.second.alive) {
			++active;
		}
	}
	return active;
}

void HashRing::Clear() {
	std::lock_guard<std::mutex> lk(mu_);
	nodes_.clear();
	ring_.clear();
}

void HashRing::RemoveVirtualNodesLocked(const std::string& node_id) {
	for (auto it = ring_.begin(); it != ring_.end();) {
		if (it->second == node_id) {
			it = ring_.erase(it);
		} else {
			++it;
		}
	}
}

void HashRing::AddVirtualNodesLocked(const NodeInfo& node) {
	const size_t replicas =
			std::max<size_t>(1, static_cast<size_t>(node.weight)) * virtual_nodes_per_weight_;

	for (size_t i = 0; i < replicas; ++i) {
		const std::string token = node.id + "#" + std::to_string(i);
		uint64_t h = Hash64(token);
		while (ring_.find(h) != ring_.end()) {
			++h;
		}
		ring_[h] = node.id;
	}
}

uint64_t HashRing::Hash64(const std::string& data) {
	// FNV-1a 64-bit hash: deterministic and dependency-free for stable routing.
	uint64_t hash = 1469598103934665603ULL;
	for (const unsigned char ch : data) {
		hash ^= static_cast<uint64_t>(ch);
		hash *= 1099511628211ULL;
	}
	return hash;
}

}  // namespace kv
