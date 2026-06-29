#pragma once

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "kv/cluster/router.h"
#include "kv/engine/write_batch.h"

namespace kv {

struct ClusterStatus {
	size_t node_count = 0;
	size_t active_node_count = 0;
	std::vector<NodeInfo> nodes;
};

struct ClusterBatchGroup {
	NodeInfo node;
	WriteBatch batch;
};

using ClusterBatchHandler =
		std::function<Status(const ClusterBatchGroup& group, size_t index, size_t total)>;

class ClusterManager {
 public:
	explicit ClusterManager(size_t virtual_nodes_per_weight = 32);

	bool AddNode(const NodeInfo& node);
	bool RemoveNode(const std::string& node_id);
	bool SetNodeAlive(const std::string& node_id, bool alive);
	bool GetNode(const std::string& node_id, NodeInfo* node) const;

	bool Route(const std::string& key, NodeInfo* node) const;
	std::vector<NodeInfo> RouteReplicas(const std::string& key,
																			size_t replica_count) const;
	bool RouteKeys(const std::vector<std::string>& keys,
								 std::vector<NodeInfo>* nodes) const;
	bool PartitionBatch(const WriteBatch& batch,
										 std::vector<ClusterBatchGroup>* groups) const;
	Status ExecutePartitionedBatch(const WriteBatch& batch,
																 const ClusterBatchHandler& handler) const;

	void SetLocalNodeId(std::string node_id);
	const std::string& LocalNodeId() const noexcept;

	std::vector<NodeInfo> Nodes() const;
	size_t NodeCount() const;
	size_t ActiveNodeCount() const;
	ClusterStatus GetStatus() const;
	void Clear();

 private:
	std::shared_ptr<HashRing> ring_;
	Router router_;
	std::string local_node_id_;
};

}  // namespace kv
