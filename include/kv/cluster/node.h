#pragma once

#include <cstdint>
#include <string>

namespace kv {

struct NodeInfo {
	std::string id;
	std::string host;
	uint16_t port = 0;
	uint32_t weight = 1;
	bool alive = true;

	bool IsValid() const;
	std::string Address() const;

	bool operator==(const NodeInfo& rhs) const;
};

}  // namespace kv
