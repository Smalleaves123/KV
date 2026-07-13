#include "server_config.h"

#include <cstdlib>
#include <fstream>
#include <optional>
#include <sstream>

namespace kv::app {

int ParsePort(const char* s) {
  if (s == nullptr)
    return -1;
  char* end = nullptr;
  const long v = std::strtol(s, &end, 10);
  if (end == s || *end != '\0' || v <= 0 || v > 65535)
    return -1;
  return static_cast<int>(v);
}

uint64_t ParseNodeId(const char* s) {
  if (s == nullptr)
    return 0;
  char* end = nullptr;
  const long long v = std::strtoll(s, &end, 10);
  if (end == s || *end != '\0' || v <= 0)
    return 0;
  return static_cast<uint64_t>(v);
}

std::string Trim(const std::string& s) {
  const size_t begin = s.find_first_not_of(" \t\r\n");
  if (begin == std::string::npos)
    return {};
  const size_t end = s.find_last_not_of(" \t\r\n");
  return s.substr(begin, end - begin + 1);
}

std::string StripComment(const std::string& s) {
  const size_t pos = s.find('#');
  return Trim(pos == std::string::npos ? s : s.substr(0, pos));
}

bool ParseBoolValue(const std::string& s, bool* value) {
  if (value == nullptr)
    return false;
  if (s == "true" || s == "TRUE" || s == "1") {
    *value = true;
    return true;
  }
  if (s == "false" || s == "FALSE" || s == "0") {
    *value = false;
    return true;
  }
  return false;
}

bool ParseSizeValue(const std::string& s, size_t* value) {
  if (value == nullptr)
    return false;
  char* end = nullptr;
  const unsigned long long parsed = std::strtoull(s.c_str(), &end, 10);
  if (end == s.c_str() || *end != '\0')
    return false;
  *value = static_cast<size_t>(parsed);
  return true;
}

bool ParseCachePolicyValue(const std::string& s, kv::CachePolicy* policy) {
  if (policy == nullptr)
    return false;
  if (s == "lru" || s == "LRU") {
    *policy = kv::CachePolicy::kLRU;
    return true;
  }
  if (s == "lfu" || s == "LFU") {
    *policy = kv::CachePolicy::kLFU;
    return true;
  }
  if (s == "shard_lru" || s == "SHARD_LRU" || s == "slru" || s == "SLRU") {
    *policy = kv::CachePolicy::kShardLRU;
    return true;
  }
  return false;
}

bool ParseInt64Value(const std::string& s, int64_t* value) {
  if (value == nullptr)
    return false;
  char* end = nullptr;
  const long long parsed = std::strtoll(s.c_str(), &end, 10);
  if (end == s.c_str() || *end != '\0')
    return false;
  *value = static_cast<int64_t>(parsed);
  return true;
}

bool ParseIntValue(const std::string& s, int* value) {
  if (value == nullptr)
    return false;
  const int parsed = ParsePort(s.c_str());
  if (parsed < 0)
    return false;
  *value = parsed;
  return true;
}

bool ParseUInt32Value(const std::string& s, uint32_t* value) {
  if (value == nullptr)
    return false;
  char* end = nullptr;
  const unsigned long parsed = std::strtoul(s.c_str(), &end, 10);
  if (end == s.c_str() || *end != '\0')
    return false;
  *value = static_cast<uint32_t>(parsed);
  return true;
}

bool ParseNodeListEntry(const std::string& item, kv::NodeInfo* node) {
  if (node == nullptr)
    return false;

  std::istringstream iss(item);
  std::string part;
  std::vector<std::string> fields;
  while (std::getline(iss, part, ',')) {
    fields.push_back(Trim(part));
  }

  for (const auto& field : fields) {
    const size_t colon = field.find(':');
    if (colon == std::string::npos) {
      continue;
    }
    const std::string key = Trim(field.substr(0, colon));
    const std::string value = Trim(field.substr(colon + 1));
    if (key == "id") {
      node->id = value;
    } else if (key == "host") {
      node->host = value;
    } else if (key == "port") {
      int port = 0;
      if (ParseIntValue(value, &port)) {
        node->port = static_cast<uint16_t>(port);
      }
    } else if (key == "weight") {
      uint32_t weight = 0;
      if (ParseUInt32Value(value, &weight)) {
        node->weight = weight;
      }
    } else if (key == "alive") {
      bool alive = true;
      if (ParseBoolValue(value, &alive)) {
        node->alive = alive;
      }
    }
  }

  return node->IsValid();
}

std::optional<AppConfigFile> LoadConfigFile(const std::string& path,
                                            std::string* error) {
  std::ifstream in(path);
  if (!in.is_open()) {
    return std::nullopt;
  }

  AppConfigFile cfg;
  enum class Section : uint8_t {
    kNone,
    kServer,
    kStorage,
    kRaft,
    kCache,
    kRaftPeers,
    kCluster,
    kClusterNodes,
  };
  Section section = Section::kNone;
  kv::RaftConfig::Peer current_peer;
  kv::NodeInfo current_node;
  uint64_t current_peer_id = 0;
  bool in_peer = false;
  bool in_node = false;

  auto flush_peer = [&]() {
    if (in_peer && current_peer_id > 0 && current_peer.raft_port > 0 &&
        !current_peer.host.empty()) {
      cfg.raft.peers[current_peer_id] = current_peer;
    }
    current_peer = kv::RaftConfig::Peer{};
    current_peer_id = 0;
    in_peer = false;
  };

  auto flush_node = [&]() {
    if (in_node && current_node.IsValid()) {
      cfg.cluster_nodes.push_back(current_node);
    }
    current_node = kv::NodeInfo{};
    in_node = false;
  };

  std::string line;
  int lineno = 0;
  while (std::getline(in, line)) {
    ++lineno;
    const size_t indent = line.find_first_not_of(' ');
    const std::string cleaned = StripComment(line);
    if (cleaned.empty())
      continue;

    if (indent == 0) {
      flush_peer();
      flush_node();
      if (cleaned == "server:") {
        section = Section::kServer;
      } else if (cleaned == "storage:") {
        section = Section::kStorage;
      } else if (cleaned == "raft:") {
        section = Section::kRaft;
      } else if (cleaned == "cache:") {
        section = Section::kCache;
      } else if (cleaned == "cluster:") {
        section = Section::kCluster;
      } else {
        section = Section::kNone;
      }
      continue;
    }

    if (section == Section::kRaft && cleaned == "peers:") {
      flush_peer();
      section = Section::kRaftPeers;
      continue;
    }

    if (section == Section::kCluster && cleaned == "nodes:") {
      flush_node();
      section = Section::kClusterNodes;
      continue;
    }

    if (section == Section::kRaftPeers) {
      if (indent == 4 && cleaned.rfind("- ", 0) == 0) {
        flush_peer();
        in_peer = true;
        const std::string rest = Trim(cleaned.substr(2));
        if (!rest.empty()) {
          const size_t colon = rest.find(':');
          if (colon == std::string::npos) {
            if (error != nullptr) {
              *error = "invalid raft peer entry at line " + std::to_string(lineno);
            }
            return std::nullopt;
          }
          const std::string key = Trim(rest.substr(0, colon));
          const std::string value = Trim(rest.substr(colon + 1));
          if (key == "id") {
            current_peer_id = ParseNodeId(value.c_str());
          }
        }
        continue;
      }
      if (indent >= 6 && in_peer) {
        const size_t colon = cleaned.find(':');
        if (colon == std::string::npos)
          continue;
        const std::string key = Trim(cleaned.substr(0, colon));
        const std::string value = Trim(cleaned.substr(colon + 1));
        if (key == "id") {
          current_peer_id = ParseNodeId(value.c_str());
        } else if (key == "host") {
          current_peer.host = value;
        } else if (key == "raft_port") {
          int port = 0;
          if (ParseIntValue(value, &port)) {
            current_peer.raft_port = static_cast<uint16_t>(port);
          }
        }
        continue;
      }
    }

    if (section == Section::kClusterNodes) {
      if (indent == 4 && cleaned.rfind("- ", 0) == 0) {
        flush_node();
        in_node = true;
        const std::string rest = Trim(cleaned.substr(2));
        if (!rest.empty()) {
          const size_t colon = rest.find(':');
          if (colon == std::string::npos) {
            if (error != nullptr) {
              *error = "invalid cluster node entry at line " + std::to_string(lineno);
            }
            return std::nullopt;
          }
          const std::string key = Trim(rest.substr(0, colon));
          const std::string value = Trim(rest.substr(colon + 1));
          if (key == "id") {
            current_node.id = value;
          }
        }
        continue;
      }
      if (indent >= 6 && in_node) {
        const size_t colon = cleaned.find(':');
        if (colon == std::string::npos)
          continue;
        const std::string key = Trim(cleaned.substr(0, colon));
        const std::string value = Trim(cleaned.substr(colon + 1));
        if (key == "id") {
          current_node.id = value;
        } else if (key == "host") {
          current_node.host = value;
        } else if (key == "port") {
          int port = 0;
          if (ParseIntValue(value, &port)) {
            current_node.port = static_cast<uint16_t>(port);
          }
        } else if (key == "weight") {
          uint32_t weight = 0;
          if (ParseUInt32Value(value, &weight)) {
            current_node.weight = weight;
          }
        } else if (key == "alive") {
          bool alive = true;
          if (ParseBoolValue(value, &alive)) {
            current_node.alive = alive;
          }
        }
        continue;
      }
    }

    const size_t colon = cleaned.find(':');
    if (colon == std::string::npos)
      continue;
    const std::string key = Trim(cleaned.substr(0, colon));
    const std::string value = Trim(cleaned.substr(colon + 1));

    if (section == Section::kServer) {
      if (key == "port") {
        ParseIntValue(value, &cfg.server_port);
      } else if (key == "db_path") {
        cfg.db_path = value;
      }
      continue;
    }

    if (section == Section::kStorage) {
      if (key == "sync_on_write") {
        ParseBoolValue(value, &cfg.sync_on_write);
      } else if (key == "memtable_write_buffer_size") {
        ParseSizeValue(value, &cfg.memtable_write_buffer_size);
      } else if (key == "compaction_min_input_files") {
        ParseSizeValue(value, &cfg.compaction_min_input_files);
      } else if (key == "auto_compaction_enabled") {
        ParseBoolValue(value, &cfg.auto_compaction_enabled);
      }
      continue;
    }

    if (section == Section::kRaft) {
      if (key == "enabled") {
        ParseBoolValue(value, &cfg.raft_enabled);
      } else if (key == "node_id") {
        cfg.raft.node_id = ParseNodeId(value.c_str());
      } else if (key == "raft_port") {
        int port = 0;
        if (ParseIntValue(value, &port)) {
          cfg.raft.raft_port = static_cast<uint16_t>(port);
        }
      } else if (key == "data_dir") {
        cfg.raft.data_dir = value;
      }
      continue;
    }

    if (section == Section::kCache) {
      if (key == "enabled") {
        ParseBoolValue(value, &cfg.cache_enabled);
      } else if (key == "policy") {
        ParseCachePolicyValue(value, &cfg.cache_policy);
      } else if (key == "capacity") {
        ParseSizeValue(value, &cfg.cache_capacity);
      } else if (key == "ttl_ms") {
        ParseInt64Value(value, &cfg.cache_ttl_ms);
      }
      continue;
    }

    if (section == Section::kCluster) {
      if (key == "local_node_id") {
        cfg.cluster_local_node_id = value;
      }
    }
  }

  flush_peer();
  flush_node();
  return cfg;
}

// Parse Raft peers from env: "1:127.0.0.1:9528,2:127.0.0.1:9628"
std::unordered_map<uint64_t, kv::RaftConfig::Peer> ParsePeers(const std::string& s) {
  std::unordered_map<uint64_t, kv::RaftConfig::Peer> peers;
  std::istringstream iss(s);
  std::string item;
  while (std::getline(iss, item, ',')) {
    if (item.empty())
      continue;
    std::istringstream pis(item);
    std::string id_str, host, port_str;
    if (!std::getline(pis, id_str, ':'))
      continue;
    if (!std::getline(pis, host, ':'))
      continue;
    if (!std::getline(pis, port_str, ':'))
      continue;
    uint64_t id = ParseNodeId(id_str.c_str());
    int p = ParsePort(port_str.c_str());
    if (id > 0 && p > 0) {
      peers[id] = {host, static_cast<uint16_t>(p)};
    }
  }
  return peers;
}

std::vector<kv::NodeInfo> ParseClusterNodes(const std::string& s) {
  std::vector<kv::NodeInfo> nodes;
  std::istringstream iss(s);
  std::string item;
  while (std::getline(iss, item, ';')) {
    kv::NodeInfo node;
    if (ParseNodeListEntry(Trim(item), &node)) {
      nodes.push_back(node);
    }
  }
  return nodes;
}

void EnsureSelfNode(std::vector<kv::NodeInfo>* nodes, const std::string& self_id,
                    const std::string& self_host, uint16_t self_port) {
  if (nodes == nullptr || self_port == 0) {
    return;
  }

  for (const auto& node : *nodes) {
    if (node.port == self_port) {
      return;
    }
  }

  kv::NodeInfo self;
  self.id = self_id;
  self.host = self_host;
  self.port = self_port;
  self.weight = 1;
  self.alive = true;
  nodes->push_back(self);
}

} // namespace kv::app
