#include "kv/net/command.h"

#include <cctype>
#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

#include "kv/cluster/cluster_manager.h"
#include "kv/engine/write_batch.h"
#include "kv/net/protocol.h"

namespace kv::net {

namespace {

std::string ToUpper(std::string s) {
  for (char& ch : s) {
    ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
  }
  return s;
}

bool ParseSizeT(const std::string& text, size_t* value) {
  if (value == nullptr || text.empty()) {
    return false;
  }

  char* end = nullptr;
  const unsigned long long parsed = std::strtoull(text.c_str(), &end, 10);
  if (end == nullptr || *end != '\0') {
    return false;
  }

  *value = static_cast<size_t>(parsed);
  return true;
}

std::string NodeToLine(const NodeInfo& node) {
  std::ostringstream oss;
  oss << "id=" << node.id
      << " address=" << node.Address()
      << " weight=" << node.weight
      << " alive=" << (node.alive ? 1 : 0);
  return oss.str();
}

std::string ClusterStatusToText(const ClusterStatus& status) {
  std::ostringstream oss;
  oss << "cluster.node_count=" << status.node_count << "\n"
      << "cluster.active_node_count=" << status.active_node_count;
  for (size_t i = 0; i < status.nodes.size(); ++i) {
    oss << "\nnode[" << i << "]." << NodeToLine(status.nodes[i]);
  }
  return oss.str();
}

std::vector<std::string> EncodeNodeArray(const std::vector<NodeInfo>& nodes) {
  std::vector<std::string> encoded;
  encoded.reserve(nodes.size());
  for (const auto& node : nodes) {
    encoded.push_back(protocol::BulkString(NodeToLine(node)));
  }
  return encoded;
}

enum class BatchOpType {
  kSet = 0,
  kDel,
};

struct BatchOp {
  BatchOpType type = BatchOpType::kSet;
  std::string key;
  std::string value;
};

struct BatchGroup {
  NodeInfo node;
  std::vector<BatchOp> ops;
};

bool ParseBatchOps(const std::vector<std::string>& args, size_t start,
                   std::vector<BatchOp>* ops, std::string* error) {
  if (ops == nullptr) {
    if (error != nullptr) {
      *error = "batch operation sink is null";
    }
    return false;
  }

  ops->clear();
  for (size_t i = start; i < args.size();) {
    const std::string op = ToUpper(args[i]);
    if (op == "SET") {
      if (i + 2 >= args.size()) {
        if (error != nullptr) {
          *error = "wrong number of arguments for 'CLUSTER BATCH SET'";
        }
        return false;
      }

      ops->push_back(BatchOp{BatchOpType::kSet, args[i + 1], args[i + 2]});
      i += 3;
      continue;
    }

    if (op == "DEL") {
      if (i + 1 >= args.size()) {
        if (error != nullptr) {
          *error = "wrong number of arguments for 'CLUSTER BATCH DEL'";
        }
        return false;
      }

      ops->push_back(BatchOp{BatchOpType::kDel, args[i + 1], {}});
      i += 2;
      continue;
    }

    if (error != nullptr) {
      *error = "unsupported operation for 'CLUSTER BATCH'";
    }
    return false;
  }

  return true;
}

bool GroupBatchOps(const ClusterManager& cluster_manager,
                   const std::vector<BatchOp>& ops, std::vector<BatchGroup>* groups,
                   std::string* error) {
  if (groups == nullptr) {
    if (error != nullptr) {
      *error = "batch group sink is null";
    }
    return false;
  }

  groups->clear();
  std::vector<std::string> seen_node_ids;

  for (const auto& op : ops) {
    NodeInfo node;
    if (!cluster_manager.Route(op.key, &node)) {
      if (error != nullptr) {
        *error = "no route available";
      }
      groups->clear();
      return false;
    }

    size_t index = groups->size();
    for (size_t i = 0; i < seen_node_ids.size(); ++i) {
      if (seen_node_ids[i] == node.id) {
        index = i;
        break;
      }
    }

    if (index == groups->size()) {
      seen_node_ids.push_back(node.id);
      groups->push_back(BatchGroup{node, {}});
    }

    groups->at(index).ops.push_back(op);
  }

  return true;
}

std::string BatchOpToText(const BatchOp& op) {
  std::ostringstream oss;
  oss << (op.type == BatchOpType::kSet ? "SET " : "DEL ") << op.key;
  if (op.type == BatchOpType::kSet) {
    oss << " " << op.value;
  }
  return oss.str();
}

std::string BatchPlanToText(const std::vector<BatchGroup>& groups) {
  std::ostringstream oss;
  oss << "cluster.batch_group_count=" << groups.size();
  for (size_t i = 0; i < groups.size(); ++i) {
    const auto& group = groups[i];
    oss << "\ngroup[" << i << "].node=" << NodeToLine(group.node)
        << "\ngroup[" << i << "].op_count=" << group.ops.size();
    for (size_t j = 0; j < group.ops.size(); ++j) {
      oss << "\ngroup[" << i << "].op[" << j << "]="
          << BatchOpToText(group.ops[j]);
    }
  }
  return oss.str();
}

}  // namespace

CommandExecutor::CommandExecutor(DB* db, const ClusterManager* cluster_manager)
    : db_(db), cluster_manager_(cluster_manager) {}

std::string CommandExecutor::Execute(const Command& cmd) const {
  using namespace protocol;

  if (db_ == nullptr) {
    return Error("db is null");
  }

  switch (cmd.type) {
    case CommandType::kPing: {
      if (!cmd.args.empty()) return Error("wrong number of arguments for 'PING'");
      return SimpleString("PONG");
    }

    case CommandType::kGet: {
      if (cmd.args.size() != 1) return Error("wrong number of arguments for 'GET'");
      std::string value;
      Status s = db_->Get(ReadOptions{}, cmd.args[0], &value);
      if (s.ok()) return BulkString(value);
      if (s.IsNotFound()) return Nil();
      return Error(s.ToString());
    }

    case CommandType::kSet: {
      if (cmd.args.size() != 2) return Error("wrong number of arguments for 'SET'");
      Status s = db_->Put(WriteOptions{}, cmd.args[0], cmd.args[1]);
      if (!s.ok()) return Error(s.ToString());
      return SimpleString("OK");
    }

    case CommandType::kDel: {
      if (cmd.args.size() != 1) return Error("wrong number of arguments for 'DEL'");
      Status s = db_->Delete(WriteOptions{}, cmd.args[0]);
      if (!s.ok() && !s.IsNotFound()) return Error(s.ToString());
      return SimpleString("OK");
    }

    case CommandType::kMGet: {
      if (cmd.args.empty()) return Error("wrong number of arguments for 'MGET'");
      std::vector<std::string> items;
      items.reserve(cmd.args.size());

      for (const auto& key : cmd.args) {
        std::string value;
        Status s = db_->Get(ReadOptions{}, key, &value);
        if (s.ok()) items.push_back(BulkString(value));
        else if (s.IsNotFound()) items.push_back(Nil());
        else return Error(s.ToString());
      }

      return Array(items);
    }

    case CommandType::kInfo:
    case CommandType::kStats: {
      if (!cmd.args.empty()) {
        return Error("wrong number of arguments for 'INFO/STATS'");
      }

      CacheStats cache_stats;
      Status s = db_->GetCacheStats(&cache_stats);
      if (!s.ok()) {
        return Error(s.ToString());
      }

      ReadPathStats read_path_stats;
      s = db_->GetReadPathStats(&read_path_stats);
      if (!s.ok()) {
        return Error(s.ToString());
      }

      CompactionStats compaction_stats;
      s = db_->GetCompactionStats(&compaction_stats);
      if (!s.ok()) {
        return Error(s.ToString());
      }

      std::ostringstream oss;
      oss << "cache.hit=" << cache_stats.hit << "\n"
          << "cache.miss=" << cache_stats.miss << "\n"
          << "cache.evict=" << cache_stats.evict << "\n"
          << "cache.expire=" << cache_stats.expire << "\n"
          << "read.table_cache_hits=" << read_path_stats.table_cache_hits << "\n"
          << "read.table_cache_misses="
          << read_path_stats.table_cache_misses << "\n"
          << "read.table_cache_evictions="
          << read_path_stats.table_cache_evictions << "\n"
          << "read.table_cache_entries="
          << read_path_stats.table_cache_entries << "\n"
          << "read.bloom_queries=" << read_path_stats.bloom_queries << "\n"
          << "read.bloom_negatives=" << read_path_stats.bloom_negatives << "\n"
          << "compaction.trigger_attempts=" << compaction_stats.trigger_attempts << "\n"
          << "compaction.skipped_due_snapshot="
          << compaction_stats.skipped_due_snapshot << "\n"
          << "compaction.skipped_due_threshold="
          << compaction_stats.skipped_due_threshold << "\n"
          << "compaction.succeeded=" << compaction_stats.succeeded << "\n"
          << "compaction.failed=" << compaction_stats.failed;
      return BulkString(oss.str());
    }

    case CommandType::kCluster: {
      if (cmd.args.empty()) {
        return Error("wrong number of arguments for 'CLUSTER'");
      }

      if (cluster_manager_ == nullptr) {
        return Error("cluster manager is null");
      }

      const std::string subcommand = ToUpper(cmd.args[0]);
      if (subcommand == "ROUTE") {
        if (cmd.args.size() < 2 || cmd.args.size() > 3) {
          return Error("wrong number of arguments for 'CLUSTER ROUTE'");
        }

        const std::string& key = cmd.args[1];
        size_t replica_count = 1;
        if (cmd.args.size() == 3) {
          if (!ParseSizeT(cmd.args[2], &replica_count) || replica_count == 0) {
            return Error("invalid replica count for 'CLUSTER ROUTE'");
          }
        }

        if (replica_count == 1) {
          NodeInfo node;
          if (!cluster_manager_->Route(key, &node)) {
            return Error("no route available");
          }
          return Array({BulkString(NodeToLine(node))});
        }

        const std::vector<NodeInfo> nodes =
            cluster_manager_->RouteReplicas(key, replica_count);
        if (nodes.empty()) {
          return Error("no route available");
        }
        return Array(EncodeNodeArray(nodes));
      }

      if (subcommand == "STATUS") {
        if (cmd.args.size() > 2) {
          return Error("wrong number of arguments for 'CLUSTER STATUS'");
        }

        if (cmd.args.size() == 2) {
          NodeInfo node;
          if (!cluster_manager_->GetNode(cmd.args[1], &node)) {
            return Error("node not found");
          }
          return BulkString(NodeToLine(node));
        }

        return BulkString(ClusterStatusToText(cluster_manager_->GetStatus()));
      }

      if (subcommand == "BATCH") {
        if (cmd.args.size() < 2) {
          return Error("wrong number of arguments for 'CLUSTER BATCH'");
        }

        std::vector<BatchOp> ops;
        std::string error;
        if (!ParseBatchOps(cmd.args, 1, &ops, &error)) {
          return Error(error);
        }

        std::vector<BatchGroup> groups;
        if (!GroupBatchOps(*cluster_manager_, ops, &groups, &error)) {
          return Error(error);
        }

        const std::string& local_node_id = cluster_manager_->LocalNodeId();
        if (local_node_id.empty()) {
          return Error("cluster local node id is not configured");
        }

        if (groups.size() != 1) {
          return Error("cluster batch routes to multiple nodes");
        }

        const BatchGroup& group = groups.front();
        if (group.node.id != local_node_id) {
          return Error("cluster batch routes to a remote node");
        }

        WriteBatch batch;
        for (const auto& op : ops) {
          if (op.type == BatchOpType::kSet) {
            batch.Put(op.key, op.value);
          } else {
            batch.Delete(op.key);
          }
        }

        Status s = db_->Write(WriteOptions{}, batch);
        if (!s.ok()) {
          return Error(s.ToString());
        }
        return SimpleString("OK");
      }

      if (subcommand == "PLAN") {
        if (cmd.args.size() < 2) {
          return Error("wrong number of arguments for 'CLUSTER PLAN'");
        }

        std::vector<BatchOp> ops;
        std::string error;
        if (!ParseBatchOps(cmd.args, 1, &ops, &error)) {
          return Error(error);
        }

        std::vector<BatchGroup> groups;
        if (!GroupBatchOps(*cluster_manager_, ops, &groups, &error)) {
          return Error(error);
        }

        return BulkString(BatchPlanToText(groups));
      }

      return Error("unknown cluster command");
    }

    case CommandType::kBegin:
    case CommandType::kExec:
    case CommandType::kAbort:
      return Error("transaction commands must be handled by session");

    case CommandType::kInvalid:
    default:
      return Error("unknown command");
  }
}

}  // namespace kv::net
