#include "kv/cluster/cluster_manager.h"

#include <utility>

namespace kv {

ClusterManager::ClusterManager(size_t virtual_nodes_per_weight)
		: ring_(std::make_shared<HashRing>(virtual_nodes_per_weight)), router_(ring_) {}

bool ClusterManager::AddNode(const NodeInfo& node) {
	return ring_->AddNode(node);
}

bool ClusterManager::RemoveNode(const std::string& node_id) {
	return ring_->RemoveNode(node_id);
}

bool ClusterManager::SetNodeAlive(const std::string& node_id, bool alive) {
	return ring_->SetNodeAlive(node_id, alive);
}

bool ClusterManager::GetNode(const std::string& node_id, NodeInfo* node) const {
	if (node == nullptr) {
		return false;
	}

	const std::optional<NodeInfo> found = ring_->GetNode(node_id);
	if (!found.has_value()) {
		return false;
	}

	*node = *found;
	return true;
}

bool ClusterManager::Route(const std::string& key, NodeInfo* node) const {
	return router_.Route(key, node);
}

std::vector<NodeInfo> ClusterManager::RouteReplicas(const std::string& key,
																										size_t replica_count) const {
	return router_.RouteReplicas(key, replica_count);
}

bool ClusterManager::RouteKeys(const std::vector<std::string>& keys,
															 std::vector<NodeInfo>* nodes) const {
	if (nodes == nullptr) {
		return false;
	}

	nodes->clear();
	nodes->reserve(keys.size());
	for (const auto& key : keys) {
		NodeInfo node;
		if (!Route(key, &node)) {
			nodes->clear();
			return false;
		}
		nodes->push_back(node);
	}
	return true;
}

void ClusterManager::SetLocalNodeId(std::string node_id) {
	local_node_id_ = std::move(node_id);
}

const std::string& ClusterManager::LocalNodeId() const noexcept {
	return local_node_id_;
}

std::vector<NodeInfo> ClusterManager::Nodes() const {
	return ring_->Nodes();
}

size_t ClusterManager::NodeCount() const {
	return ring_->NodeCount();
}

size_t ClusterManager::ActiveNodeCount() const {
	return ring_->ActiveNodeCount();
}

ClusterStatus ClusterManager::GetStatus() const {
	ClusterStatus status;
	status.node_count = NodeCount();
	status.active_node_count = ActiveNodeCount();
	status.nodes = Nodes();
	return status;
}

void ClusterManager::Clear() {
	ring_->Clear();
}

}
