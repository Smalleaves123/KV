#include "kv/cluster/router.h"

namespace kv {

Router::Router() : ring_(std::make_shared<HashRing>()) {}

Router::Router(std::shared_ptr<HashRing> ring) : ring_(std::move(ring)) {
	if (!ring_) {
		ring_ = std::make_shared<HashRing>();
	}
}

void Router::SetRing(std::shared_ptr<HashRing> ring) {
	if (!ring) {
		ring = std::make_shared<HashRing>();
	}
	std::lock_guard<std::mutex> lk(mu_);
	ring_ = std::move(ring);
}

std::shared_ptr<HashRing> Router::GetRing() const {
	std::lock_guard<std::mutex> lk(mu_);
	return ring_;
}

bool Router::Route(const std::string& key, NodeInfo* node) const {
	if (node == nullptr) {
		return false;
	}

	std::shared_ptr<HashRing> ring;
	{
		std::lock_guard<std::mutex> lk(mu_);
		ring = ring_;
	}

	const std::optional<NodeInfo> picked = ring->Lookup(key);
	if (!picked.has_value()) {
		return false;
	}

	*node = *picked;
	return true;
}

std::vector<NodeInfo> Router::RouteReplicas(const std::string& key,
																						size_t replica_count) const {
	std::shared_ptr<HashRing> ring;
	{
		std::lock_guard<std::mutex> lk(mu_);
		ring = ring_;
	}
	return ring->LookupN(key, replica_count);
}

}  // namespace kv
