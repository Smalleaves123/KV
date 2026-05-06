#pragma once

#include <memory>
#include <string>
#include <vector>

#include "kv/cluster/router.h"

namespace kv {

class ClusterManager {
 public:
	explicit ClusterManager(size_t virtual_nodes_per_weight = 32);

	bool AddNode(const NodeInfo& node);
	bool RemoveNode(const std::string& node_id);
	bool SetNodeAlive(const std::string& node_id, bool alive);

	bool Route(const std::string& key, NodeInfo* node) const;
	std::vector<NodeInfo> RouteReplicas(const std::string& key,
																			size_t replica_count) const;

	std::vector<NodeInfo> Nodes() const;
	size_t NodeCount() const;
	size_t ActiveNodeCount() const;
	void Clear();

 private:
	std::shared_ptr<HashRing> ring_;
	Router router_;
};

}  // namespace kv
