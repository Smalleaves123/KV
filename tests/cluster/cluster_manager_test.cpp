#include "kv/cluster/cluster_manager.h"

#include <gtest/gtest.h>

namespace kv {
namespace {

std::pair<std::string, std::string> FindCrossNodeKeys(ClusterManager* mgr,
                                                      NodeInfo* first_node = nullptr,
                                                      NodeInfo* second_node = nullptr) {
  if (mgr == nullptr) {
    return {};
  }

  std::string first_key;
  NodeInfo first;
  for (int i = 0; i < 5000; ++i) {
    const std::string candidate = "key_" + std::to_string(i);
    NodeInfo node;
    if (!mgr->Route(candidate, &node)) {
      continue;
    }
    if (first_key.empty()) {
      first_key = candidate;
      first = node;
      continue;
    }
    if (node.id != first.id) {
      if (first_node != nullptr) {
        *first_node = first;
      }
      if (second_node != nullptr) {
        *second_node = node;
      }
      return {first_key, candidate};
    }
  }
  return {};
}

TEST(ClusterManagerTest, RouteOnNonEmptyCluster) {
  ClusterManager mgr(8);

  EXPECT_TRUE(mgr.AddNode(NodeInfo{"n1", "127.0.0.1", 9001, 1, true}));
  EXPECT_TRUE(mgr.AddNode(NodeInfo{"n2", "127.0.0.1", 9002, 1, true}));

  NodeInfo node;
  EXPECT_TRUE(mgr.Route("user:1001", &node));
  EXPECT_TRUE(node.id == "n1" || node.id == "n2");
}

TEST(ClusterManagerTest, SetNodeAliveAffectsRouting) {
  ClusterManager mgr(8);

  EXPECT_TRUE(mgr.AddNode(NodeInfo{"n1", "127.0.0.1", 9001, 1, true}));
  EXPECT_TRUE(mgr.AddNode(NodeInfo{"n2", "127.0.0.1", 9002, 1, true}));

  EXPECT_TRUE(mgr.SetNodeAlive("n2", false));
  EXPECT_EQ(mgr.ActiveNodeCount(), 1U);

  NodeInfo node;
  EXPECT_TRUE(mgr.Route("k", &node));
  EXPECT_EQ(node.id, "n1");
}

TEST(ClusterManagerTest, ReplicaRoutingReturnsUniqueNodes) {
  ClusterManager mgr(8);

  EXPECT_TRUE(mgr.AddNode(NodeInfo{"n1", "127.0.0.1", 9001, 1, true}));
  EXPECT_TRUE(mgr.AddNode(NodeInfo{"n2", "127.0.0.1", 9002, 1, true}));
  EXPECT_TRUE(mgr.AddNode(NodeInfo{"n3", "127.0.0.1", 9003, 1, true}));

  const std::vector<NodeInfo> replicas = mgr.RouteReplicas("order:42", 2);
  ASSERT_EQ(replicas.size(), 2U);
  EXPECT_NE(replicas[0].id, replicas[1].id);
}

TEST(ClusterManagerTest, GetNodeAndStatusExposeClusterState) {
  ClusterManager mgr(8);

  EXPECT_TRUE(mgr.AddNode(NodeInfo{"n1", "127.0.0.1", 9001, 2, true}));
  EXPECT_TRUE(mgr.AddNode(NodeInfo{"n2", "127.0.0.1", 9002, 1, false}));

  NodeInfo node;
  EXPECT_TRUE(mgr.GetNode("n1", &node));
  EXPECT_EQ(node.id, "n1");
  EXPECT_EQ(node.weight, 2U);

  ClusterStatus status = mgr.GetStatus();
  EXPECT_EQ(status.node_count, 2U);
  EXPECT_EQ(status.active_node_count, 1U);
  ASSERT_EQ(status.nodes.size(), 2U);
}

TEST(ClusterManagerTest, RouteKeysPreservesOrder) {
  ClusterManager mgr(8);

  EXPECT_TRUE(mgr.AddNode(NodeInfo{"n1", "127.0.0.1", 9001, 1, true}));
  EXPECT_TRUE(mgr.AddNode(NodeInfo{"n2", "127.0.0.1", 9002, 1, true}));

  std::vector<NodeInfo> nodes;
  EXPECT_TRUE(mgr.RouteKeys({"k1", "k2", "k3"}, &nodes));
  ASSERT_EQ(nodes.size(), 3U);
}

TEST(ClusterManagerTest, PartitionBatchGroupsOperationsByNode) {
  ClusterManager mgr(8);

  EXPECT_TRUE(mgr.AddNode(NodeInfo{"n1", "127.0.0.1", 9001, 1, true}));
  EXPECT_TRUE(mgr.AddNode(NodeInfo{"n2", "127.0.0.1", 9002, 1, true}));

  NodeInfo first_node;
  NodeInfo second_node;
  const auto keys = FindCrossNodeKeys(&mgr, &first_node, &second_node);
  ASSERT_FALSE(keys.first.empty());
  ASSERT_FALSE(keys.second.empty());
  ASSERT_NE(first_node.id, second_node.id);

  WriteBatch batch;
  batch.Put(keys.first, "v1");
  batch.Delete(keys.second);
  batch.Put(keys.first, "v3");

  std::vector<ClusterBatchGroup> groups;
  EXPECT_TRUE(mgr.PartitionBatch(batch, &groups));
  ASSERT_EQ(groups.size(), 2U);
  EXPECT_EQ(groups[0].node.id, first_node.id);
  EXPECT_EQ(groups[1].node.id, second_node.id);
  EXPECT_EQ(groups[0].batch.Count(), 2U);
  EXPECT_EQ(groups[1].batch.Count(), 1U);

  const auto& first_ops = groups[0].batch.operations();
  const auto& second_ops = groups[1].batch.operations();
  ASSERT_EQ(first_ops.size(), 2U);
  ASSERT_EQ(second_ops.size(), 1U);
  EXPECT_EQ(first_ops[0].key, keys.first);
  EXPECT_EQ(first_ops[1].key, keys.first);
  EXPECT_EQ(second_ops[0].key, keys.second);
}

TEST(ClusterManagerTest, ExecutePartitionedBatchInvokesHandlerInOrder) {
  ClusterManager mgr(8);

  EXPECT_TRUE(mgr.AddNode(NodeInfo{"n1", "127.0.0.1", 9001, 1, true}));
  EXPECT_TRUE(mgr.AddNode(NodeInfo{"n2", "127.0.0.1", 9002, 1, true}));

  std::string first_key;
  std::string second_key;
  NodeInfo first_node;
  NodeInfo second_node;
  for (int i = 0; i < 2000; ++i) {
    const std::string candidate = "exec_" + std::to_string(i);
    NodeInfo node;
    ASSERT_TRUE(mgr.Route(candidate, &node));
    if (first_key.empty()) {
      first_key = candidate;
      first_node = node;
      continue;
    }
    if (node.id != first_node.id) {
      second_key = candidate;
      second_node = node;
      break;
    }
  }

  ASSERT_FALSE(first_key.empty());
  ASSERT_FALSE(second_key.empty());
  ASSERT_NE(first_node.id, second_node.id);

  WriteBatch batch;
  batch.Put(first_key, "v1");
  batch.Delete(second_key);
  batch.Put(first_key, "v2");

  std::vector<std::string> seen;
  Status s = mgr.ExecutePartitionedBatch(
      batch, [&](const ClusterBatchGroup& group, size_t index, size_t total) {
        seen.push_back(group.node.id + ":" + std::to_string(index) + "/" +
                       std::to_string(total) + ":" +
                       std::to_string(group.batch.Count()));
        return Status::OK();
      });

  EXPECT_TRUE(s.ok());
  ASSERT_EQ(seen.size(), 2U);
  EXPECT_EQ(seen[0].find(first_node.id), 0U);
  EXPECT_EQ(seen[1].find(second_node.id), 0U);
  EXPECT_NE(seen[0].find("0/2"), std::string::npos);
  EXPECT_NE(seen[1].find("1/2"), std::string::npos);
}

TEST(ClusterManagerTest, ExecutePartitionedBatchStopsOnHandlerError) {
  ClusterManager mgr(8);

  EXPECT_TRUE(mgr.AddNode(NodeInfo{"n1", "127.0.0.1", 9001, 1, true}));
  EXPECT_TRUE(mgr.AddNode(NodeInfo{"n2", "127.0.0.1", 9002, 1, true}));

  NodeInfo first_node;
  const auto keys = FindCrossNodeKeys(&mgr, &first_node);
  ASSERT_FALSE(keys.first.empty());
  ASSERT_FALSE(keys.second.empty());

  WriteBatch batch;
  batch.Put(keys.first, "v1");
  batch.Put(keys.second, "v2");

  int calls = 0;
  Status s = mgr.ExecutePartitionedBatch(
      batch, [&](const ClusterBatchGroup&, size_t, size_t total) {
        ++calls;
        if (total > 1) {
          return Status::InvalidArgument("multi-node batch");
        }
        return Status::OK();
      });

  EXPECT_TRUE(s.IsInvalidArgument());
  EXPECT_EQ(s.message(), "multi-node batch");
  EXPECT_EQ(calls, 1);
}

}  // namespace
}  // namespace kv
