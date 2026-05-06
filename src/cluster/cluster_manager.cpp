#include "kv/cluster/cluster_manager.h"

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

bool ClusterManager::Route(const std::string& key, NodeInfo* node) const {
	return router_.Route(key, node);
}

std::vector<NodeInfo> ClusterManager::RouteReplicas(const std::string& key,
																										size_t replica_count) const {
	return router_.RouteReplicas(key, replica_count);
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

void ClusterManager::Clear() {
	ring_->Clear();
}

}
