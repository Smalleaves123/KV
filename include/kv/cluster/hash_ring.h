#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "kv/cluster/node.h"

namespace kv {

class HashRing {
 public:
	explicit HashRing(size_t virtual_nodes_per_weight = 32);

	bool AddNode(const NodeInfo& node);
	bool RemoveNode(const std::string& node_id);
	bool SetNodeAlive(const std::string& node_id, bool alive);

	std::optional<NodeInfo> Lookup(const std::string& key) const;
	std::vector<NodeInfo> LookupN(const std::string& key, size_t n) const;

	std::vector<NodeInfo> Nodes() const;
	size_t NodeCount() const;
	size_t ActiveNodeCount() const;
	void Clear();

 private:
	void RemoveVirtualNodesLocked(const std::string& node_id);
	void AddVirtualNodesLocked(const NodeInfo& node);
	static uint64_t Hash64(const std::string& data);

	const size_t virtual_nodes_per_weight_;
	mutable std::mutex mu_;
	std::unordered_map<std::string, NodeInfo> nodes_;
	std::map<uint64_t, std::string> ring_;
};

}  // namespace kv
