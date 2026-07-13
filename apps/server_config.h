#pragma once

#include "kv/cache/cache.h"
#include "kv/cluster/node.h"
#include "kv/raft/raft_server.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace kv::app {

struct AppConfigFile {
  int server_port = 9527;
  std::string db_path = "data/db";
  bool raft_enabled = false;
  kv::RaftConfig raft;
  std::string cluster_local_node_id = "local";
  std::vector<kv::NodeInfo> cluster_nodes;
  bool cache_enabled = false;
  kv::CachePolicy cache_policy = kv::CachePolicy::kLRU;
  size_t cache_capacity = 1024;
  int64_t cache_ttl_ms = 0;
  bool sync_on_write = false;
  size_t memtable_write_buffer_size = 4 * 1024 * 1024;
  size_t compaction_min_input_files = 2;
  bool auto_compaction_enabled = true;
};

int ParsePort(const char* value);
uint64_t ParseNodeId(const char* value);
bool ParseBoolValue(const std::string& value, bool* result);
bool ParseSizeValue(const std::string& value, size_t* result);
bool ParseCachePolicyValue(const std::string& value, kv::CachePolicy* policy);

std::optional<AppConfigFile> LoadConfigFile(const std::string& path,
                                            std::string* error);

std::unordered_map<uint64_t, kv::RaftConfig::Peer> ParsePeers(const std::string& value);
std::vector<kv::NodeInfo> ParseClusterNodes(const std::string& value);
void EnsureSelfNode(std::vector<kv::NodeInfo>* nodes, const std::string& self_id,
                    const std::string& self_host, uint16_t self_port);

} // namespace kv::app
