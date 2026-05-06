#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "kv/cluster/hash_ring.h"

namespace kv {

class Router {
 public:
	Router();
	explicit Router(std::shared_ptr<HashRing> ring);

	void SetRing(std::shared_ptr<HashRing> ring);
	std::shared_ptr<HashRing> GetRing() const;

	bool Route(const std::string& key, NodeInfo* node) const;
	std::vector<NodeInfo> RouteReplicas(const std::string& key,
																			size_t replica_count) const;

 private:
	mutable std::mutex mu_;
	std::shared_ptr<HashRing> ring_;
};

}  // namespace kv
