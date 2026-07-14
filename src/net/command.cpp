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

bool ParseInt64(const std::string& text, int64_t* value) {
  if (value == nullptr || text.empty()) return false;
  char* end = nullptr;
  const long long parsed = std::strtoll(text.c_str(), &end, 10);
  if (end == nullptr || *end != '\0') return false;
  *value = static_cast<int64_t>(parsed);
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

std::string EncodeNodeFields(const NodeInfo& node) {
  return protocol::Array({
      protocol::BulkString(node.id),
      protocol::BulkString(node.host),
      protocol::BulkString(std::to_string(node.port)),
      protocol::BulkString(std::to_string(node.weight)),
      protocol::BulkString(node.alive ? "1" : "0"),
  });
}

bool ParseClusterWriteBatch(const std::vector<std::string>& args, size_t start,
                           WriteBatch* batch, std::string* error) {
  if (batch == nullptr) {
    if (error != nullptr) {
      *error = "batch sink is null";
    }
    return false;
  }

  batch->Clear();
  for (size_t i = start; i < args.size();) {
    const std::string op = ToUpper(args[i]);
    if (op == "SET") {
      if (i + 2 >= args.size()) {
        if (error != nullptr) {
          *error = "wrong number of arguments for 'CLUSTER BATCH SET'";
        }
        return false;
      }

      batch->Put(args[i + 1], args[i + 2]);
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

      batch->Delete(args[i + 1]);
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

std::string EncodeBatchOp(const WriteBatch::Operation& op) {
  std::vector<std::string> encoded_fields;
  encoded_fields.reserve(3);
  encoded_fields.push_back(protocol::BulkString(
      op.type == WriteBatch::ValueType::kPut ? "SET" : "DEL"));
  encoded_fields.push_back(protocol::BulkString(op.key));
  if (op.type == WriteBatch::ValueType::kPut) {
    encoded_fields.push_back(protocol::BulkString(op.value));
  } else {
    encoded_fields.push_back(protocol::Nil());
  }
  return protocol::Array(encoded_fields);
}

std::string EncodeBatchPlan(const std::vector<ClusterBatchGroup>& groups) {
  std::vector<std::string> encoded_groups;
  encoded_groups.reserve(groups.size());

  for (const auto& group : groups) {
    std::vector<std::string> encoded_ops;
    encoded_ops.reserve(group.batch.Count());
    for (const auto& op : group.batch.operations()) {
      encoded_ops.push_back(EncodeBatchOp(op));
    }
    encoded_groups.push_back(protocol::Array({
        EncodeNodeFields(group.node),
        protocol::Array(encoded_ops),
    }));
  }

  return protocol::Array(encoded_groups);
}

std::string ProtocolMessageForStatus(const Status& status) {
  if (status.message().empty()) {
    return status.ToString();
  }
  return status.message();
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

    case CommandType::kExpire: {
      if (cmd.args.size() != 2) {
        return Error("wrong number of arguments for 'EXPIRE'");
      }
      int64_t seconds = 0;
      if (!ParseInt64(cmd.args[1], &seconds)) {
        return Error("invalid TTL value for 'EXPIRE'");
      }
      Status s = db_->Expire(WriteOptions{}, cmd.args[0], seconds);
      if (s.ok()) return Integer(1);
      if (s.IsNotFound()) return Integer(0);
      return Error(s.ToString());
    }

    case CommandType::kTTL: {
      if (cmd.args.size() != 1) {
        return Error("wrong number of arguments for 'TTL'");
      }
      int64_t seconds = -2;
      Status s = db_->TTL(ReadOptions{}, cmd.args[0], &seconds);
      if (!s.ok()) return Error(s.ToString());
      return Integer(seconds);
    }

    case CommandType::kPersist: {
      if (cmd.args.size() != 1) {
        return Error("wrong number of arguments for 'PERSIST'");
      }
      Status s = db_->Persist(WriteOptions{}, cmd.args[0]);
      if (s.ok()) return Integer(1);
      if (s.IsNotFound()) return Integer(0);
      return Error(s.ToString());
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

        WriteBatch batch;
        std::string error;
        if (!ParseClusterWriteBatch(cmd.args, 1, &batch, &error)) {
          return Error(error);
        }

        const std::string& local_node_id = cluster_manager_->LocalNodeId();
        if (local_node_id.empty()) {
          return Error("cluster local node id is not configured");
        }

        const Status s = cluster_manager_->ExecutePartitionedBatch(
            batch, [&](const ClusterBatchGroup& group, size_t /*index*/, size_t total) {
              if (total != 1) {
                return Status::InvalidArgument("cluster batch routes to multiple nodes");
              }
              if (group.node.id != local_node_id) {
                return Status::InvalidArgument("cluster batch routes to a remote node");
              }
              return db_->Write(WriteOptions{}, group.batch);
            });
        if (!s.ok()) {
          return Error(ProtocolMessageForStatus(s));
        }
        return SimpleString("OK");
      }

      if (subcommand == "PLAN") {
        if (cmd.args.size() < 2) {
          return Error("wrong number of arguments for 'CLUSTER PLAN'");
        }

        WriteBatch batch;
        std::string error;
        if (!ParseClusterWriteBatch(cmd.args, 1, &batch, &error)) {
          return Error(error);
        }

        std::vector<ClusterBatchGroup> groups;
        if (!cluster_manager_->PartitionBatch(batch, &groups)) {
          return Error("no route available");
        }

        return EncodeBatchPlan(groups);
      }

      return Error("unknown cluster command");
    }

    case CommandType::kScan: {
      // SCAN [key] [LIMIT N]
      std::string start_key;
      int64_t limit = 100;

      size_t arg_idx = 0;
      if (arg_idx < cmd.args.size() && std::toupper(static_cast<unsigned char>(cmd.args[arg_idx][0])) != 'L') {
        start_key = cmd.args[arg_idx];
        ++arg_idx;
      }
      if (arg_idx < cmd.args.size() &&
          ToUpper(cmd.args[arg_idx]) == "LIMIT" &&
          arg_idx + 1 < cmd.args.size()) {
        char* end = nullptr;
        const long v = std::strtol(cmd.args[arg_idx + 1].c_str(), &end, 10);
        if (end != nullptr && *end == '\0' && v > 0) {
          limit = static_cast<int64_t>(v);
        } else {
          return Error("invalid LIMIT value for 'SCAN'");
        }
      }

      auto it = db_->NewIterator(ReadOptions{});
      if (it == nullptr) {
        return Error("failed to create iterator");
      }

      if (start_key.empty()) {
        it->SeekToFirst();
      } else {
        it->Seek(start_key);
      }

      std::vector<std::string> items;
      items.reserve(static_cast<size_t>(limit) * 2);

      for (int64_t count = 0; count < limit && it->Valid(); it->Next(), ++count) {
        items.push_back(protocol::BulkString(it->key().ToString()));
        items.push_back(protocol::BulkString(it->value().ToString()));
      }

      return protocol::Array(items);
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
