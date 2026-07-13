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
#include "kv/raft/raft_rpc_codec.h"

namespace kv {
namespace {

struct TestNode {
  uint64_t node_id = 0;
  uint16_t raft_port = 0;
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
  void SetUp() override {
    constexpr uint16_t kBasePort = 21100;
    std::unordered_map<uint64_t, RaftConfig::Peer> peers;
    for (uint64_t id = 1; id <= 3; ++id) {
      peers[id] = {"127.0.0.1", static_cast<uint16_t>(kBasePort + id)};
    }

    for (uint64_t id = 1; id <= 3; ++id) {
      TestNode node;
      node.node_id = id;
      node.raft_port = static_cast<uint16_t>(kBasePort + id);

      const std::string base = MakeBasePath("replication", static_cast<int>(id));
      node.db_options.db_path = base + "_db";
      node.raft_dir = base + "_raft";

      RemoveDirIfExists(node.db_options.db_path);
      RemoveDirIfExists(node.raft_dir);

      Status s = DB::Open(node.db_options, &node.db);
      ASSERT_TRUE(s.ok()) << s.ToString();

      RaftConfig cfg;
      cfg.node_id = id;
      cfg.raft_port = node.raft_port;
      cfg.data_dir = node.raft_dir;
      cfg.peers = peers;

      auto* applier = dynamic_cast<WriteApplier*>(node.db.get());
      ASSERT_NE(applier, nullptr);
      node.raft = std::make_unique<RaftServer>(cfg, applier);
      s = node.raft->Start();
      if (!s.ok() &&
          s.ToString().find("raft rpc bind: Operation not permitted") !=
              std::string::npos) {
        GTEST_SKIP() << "sandbox does not permit raft rpc listener sockets";
      }
      ASSERT_TRUE(s.ok()) << s.ToString();

      nodes_.push_back(std::move(node));
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
}

}  // namespace
}  // namespace kv
