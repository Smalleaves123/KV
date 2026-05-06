#include "kv/cluster/node.h"

namespace kv {

bool NodeInfo::IsValid() const {
	return !id.empty() && !host.empty() && port != 0;
}

std::string NodeInfo::Address() const {
	return host + ":" + std::to_string(port);
}

bool NodeInfo::operator==(const NodeInfo& rhs) const {
	return id == rhs.id && host == rhs.host && port == rhs.port &&
				 weight == rhs.weight && alive == rhs.alive;
}

}  // namespace kv
