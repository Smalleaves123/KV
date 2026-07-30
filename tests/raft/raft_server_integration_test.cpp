#include "kv/raft/raft_server.h"

#include <chrono>
#include <filesystem>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "kv/engine/db.h"
#include "kv/engine/write_applier.h"
#include "kv/common/time.h"
#include "kv/raft/raft_rpc_codec.h"
#include "kv/net/session.h"

namespace kv {
namespace {

struct TestNode {
  uint64_t node_id = 0;
  uint16_t raft_port = 0;
  uint16_t client_port = 0;
  DBOptions db_options;
  std::string raft_dir;
  std::unique_ptr<DB> db;
  std::unique_ptr<RaftServer> raft;
};

std::string MakeBasePath(const std::string& name, int node_id) {
  static int counter = 0;
  ++counter;
  std::ostringstream oss;
  oss << "test_tmp/raft/" << name << "_" << counter << "_node_" << node_id;
  return oss.str();
}

void RemoveDirIfExists(const std::string& path) {
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
}

bool WaitUntil(const std::function<bool()>& pred,
               std::chrono::milliseconds timeout,
               std::chrono::milliseconds interval =
                   std::chrono::milliseconds(50)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (pred()) {
      return true;
    }
    std::this_thread::sleep_for(interval);
  }
  return pred();
}

class RaftServerIntegrationTest : public ::testing::Test {
 protected:
  RaftConfig MakeRaftConfig(const TestNode& node) const {
    RaftConfig config;
    config.node_id = node.node_id;
    config.raft_port = node.raft_port;
    config.data_dir = node.raft_dir;
    config.peers = peers_;
    return config;
  }

  Status StartNode(TestNode* node) {
    if (node == nullptr || node->db == nullptr) {
      return Status::InvalidArgument("test node is unavailable");
    }
    auto* applier = dynamic_cast<WriteApplier*>(node->db.get());
    if (applier == nullptr) {
      return Status::InvalidArgument("test DB is not a write applier");
    }
    node->raft = std::make_unique<RaftServer>(MakeRaftConfig(*node), applier);
    return node->raft->Start();
  }

  void SetUp() override {
    constexpr uint16_t kBasePort = 21100;
    constexpr uint16_t kClientBasePort = 22100;
    for (uint64_t id = 1; id <= 3; ++id) {
      peers_[id] = {"127.0.0.1", static_cast<uint16_t>(kBasePort + id),
                    static_cast<uint16_t>(kClientBasePort + id)};
    }

    for (uint64_t id = 1; id <= 3; ++id) {
      TestNode node;
      node.node_id = id;
      node.raft_port = static_cast<uint16_t>(kBasePort + id);
      node.client_port = static_cast<uint16_t>(kClientBasePort + id);

      const std::string base = MakeBasePath("replication", static_cast<int>(id));
      node.db_options.db_path = base + "_db";
      node.raft_dir = base + "_raft";

      RemoveDirIfExists(node.db_options.db_path);
      RemoveDirIfExists(node.raft_dir);

      Status s = DB::Open(node.db_options, &node.db);
      ASSERT_TRUE(s.ok()) << s.ToString();

      nodes_.push_back(std::move(node));
      s = StartNode(&nodes_.back());
      if (!s.ok() &&
          s.ToString().find("raft rpc bind: Operation not permitted") !=
              std::string::npos) {
        GTEST_SKIP() << "sandbox does not permit raft rpc listener sockets";
      }
      ASSERT_TRUE(s.ok()) << s.ToString();

    }
  }

  void TearDown() override {
    for (auto& node : nodes_) {
      if (node.raft) {
        (void)node.raft->Stop();
      }
      if (node.db) {
        (void)node.db->Close();
      }
      RemoveDirIfExists(node.db_options.db_path);
      RemoveDirIfExists(node.raft_dir);
    }
    nodes_.clear();
  }

  std::vector<TestNode> nodes_;
  std::unordered_map<uint64_t, RaftConfig::Peer> peers_;
};

TEST_F(RaftServerIntegrationTest, LeaderReplicatesCommittedWriteToAllNodes) {
  RaftServer* leader = nullptr;
  ASSERT_TRUE(WaitUntil(
      [&]() {
        for (auto& node : nodes_) {
          if (node.raft->IsLeader()) {
            leader = node.raft.get();
            return true;
          }
        }
        return false;
      },
      std::chrono::seconds(5)))
      << "leader was not elected in time";

  ASSERT_NE(leader, nullptr);
  const RaftStats leader_stats = leader->GetStats();
  EXPECT_TRUE(leader_stats.running);
  EXPECT_TRUE(leader_stats.is_leader);
  EXPECT_EQ(leader_stats.leader_id, leader->NodeId());
  EXPECT_EQ(leader_stats.peers.size(), 2U);

  TestNode* follower = nullptr;
  for (auto& node : nodes_) {
    if (node.raft.get() != leader) {
      follower = &node;
      break;
    }
  }
  ASSERT_NE(follower, nullptr);
  net::Session follower_session(follower->db.get(), nullptr, "",
                                follower->raft.get());
  EXPECT_EQ(follower_session.HandleLine("SET redirect value"),
            "-MOVED 127.0.0.1:" + std::to_string(
                static_cast<uint16_t>(22100 + leader->NodeId())) + "\r\n");

  const Status s = leader->Propose(raft::EncodePutCmd("alpha", "beta"));
  ASSERT_TRUE(s.ok()) << s.ToString();

  ASSERT_TRUE(WaitUntil(
      [&]() {
        for (auto& node : nodes_) {
          std::string value;
          Status get = node.db->Get(ReadOptions{}, "alpha", &value);
          if (!get.ok() || value != "beta") {
            return false;
          }
        }
        return true;
      },
      std::chrono::seconds(5)))
      << "replicated value did not become visible on all nodes";

  ASSERT_TRUE(leader->Propose(raft::EncodePutCmd("ttl-key", "ttl-value")).ok());
  ASSERT_TRUE(WaitUntil(
      [&]() {
        for (auto& node : nodes_) {
          std::string value;
          if (!node.db->Get(ReadOptions{}, "ttl-key", &value).ok() ||
              value != "ttl-value") {
            return false;
          }
        }
        return true;
      },
      std::chrono::seconds(5)));

  ASSERT_TRUE(leader->Propose(raft::EncodeExpireCmd(
                                 "ttl-key", NowUnixMillis() + 1000))
                  .ok());
  ASSERT_TRUE(WaitUntil(
      [&]() {
        for (auto& node : nodes_) {
          int64_t ttl = -2;
          if (!node.db->TTL(ReadOptions{}, "ttl-key", &ttl).ok() || ttl < 0) {
            return false;
          }
        }
        return true;
      },
      std::chrono::seconds(5)));

  std::this_thread::sleep_for(std::chrono::milliseconds(1200));
  for (auto& node : nodes_) {
    std::string value;
    EXPECT_TRUE(node.db->Get(ReadOptions{}, "ttl-key", &value).IsNotFound());
  }
}

TEST_F(RaftServerIntegrationTest, RestartedFollowerCatchesUpCommittedWrite) {
  TestNode* leader_node = nullptr;
  ASSERT_TRUE(WaitUntil(
      [&]() {
        for (auto& node : nodes_) {
          if (node.raft->IsLeader()) {
            leader_node = &node;
            return true;
          }
        }
        return false;
      },
      std::chrono::seconds(5)));
  ASSERT_NE(leader_node, nullptr);

  TestNode* follower = nullptr;
  for (auto& node : nodes_) {
    if (&node != leader_node) {
      follower = &node;
      break;
    }
  }
  ASSERT_NE(follower, nullptr);

  ASSERT_TRUE(follower->raft->Stop().ok());
  follower->raft.reset();
  ASSERT_TRUE(leader_node->raft->Propose(
                  raft::EncodePutCmd("after-follower-restart", "value"))
                  .ok());
  ASSERT_TRUE(StartNode(follower).ok());

  ASSERT_TRUE(WaitUntil(
      [&]() {
        std::string value;
        return follower->db->Get(ReadOptions{}, "after-follower-restart",
                                 &value)
                   .ok() &&
               value == "value";
      },
      std::chrono::seconds(5)));
}

TEST_F(RaftServerIntegrationTest, NewLeaderCommitsAfterOldLeaderRestarts) {
  TestNode* old_leader = nullptr;
  ASSERT_TRUE(WaitUntil(
      [&]() {
        for (auto& node : nodes_) {
          if (node.raft->IsLeader()) {
            old_leader = &node;
            return true;
          }
        }
        return false;
      },
      std::chrono::seconds(5)));
  ASSERT_NE(old_leader, nullptr);

  ASSERT_TRUE(old_leader->raft->Stop().ok());
  old_leader->raft.reset();

  TestNode* new_leader = nullptr;
  ASSERT_TRUE(WaitUntil(
      [&]() {
        for (auto& node : nodes_) {
          if (node.raft != nullptr && node.raft->IsLeader()) {
            new_leader = &node;
            return true;
          }
        }
        return false;
      },
      std::chrono::seconds(5)));
  ASSERT_NE(new_leader, nullptr);
  ASSERT_NE(new_leader, old_leader);

  ASSERT_TRUE(new_leader->raft->Propose(
                  raft::EncodePutCmd("after-leader-failover", "value"))
                  .ok());
  const Status restart = StartNode(old_leader);
  ASSERT_TRUE(restart.ok()) << restart.ToString();

  ASSERT_TRUE(WaitUntil(
      [&]() {
        for (auto& node : nodes_) {
          std::string value;
          if (!node.db->Get(ReadOptions{}, "after-leader-failover", &value)
                   .ok() ||
              value != "value") {
            return false;
          }
        }
        return true;
      },
      std::chrono::seconds(5)));
}

}  // namespace
}  // namespace kv
