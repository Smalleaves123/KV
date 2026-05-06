#include "kv/cluster/cluster_manager.h"

#include <gtest/gtest.h>

namespace kv {
namespace {

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

}  // namespace
}  // namespace kv
