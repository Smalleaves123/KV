#include "kv/raft/raft_log.h"

#include <filesystem>

#include <gtest/gtest.h>

#include "kv/raft/raft_storage_impl.h"
#include "kv/raft/raft_rpc_codec.h"

namespace kv {
namespace raft {
namespace {

// 内存存储，用于 RaftLog 测试
class MemStorage : public RaftStorage {
 public:
  HardState InitialState() const override {
    return hs_;
  }

  void SaveHardState(const HardState& state) override {
    hs_ = state;
  }

  RaftSnapshotMeta SnapshotMeta() const override { return snapshot_meta_; }

  void SaveSnapshotMeta(const RaftSnapshotMeta& meta) override {
    snapshot_meta_ = meta;
  }

  std::vector<LogEntry> Entries(uint64_t low, uint64_t high) const override {
    if (low < FirstIndex() || high > LastIndex() + 1 || low > high) {
      return {};
    }
    std::vector<LogEntry> result;
    for (uint64_t i = low; i < high; ++i) {
      result.push_back(logs_.at(i));
    }
    return result;
  }

  uint64_t Term(uint64_t index) const override {
    if (index == 0) return 0;
    auto it = logs_.find(index);
    if (it == logs_.end()) return 0;
    return it->second.term;
  }

  uint64_t FirstIndex() const override {
    if (logs_.empty()) return snapshot_meta_.last_included_index + 1;
    return logs_.begin()->first;
  }

  uint64_t LastIndex() const override {
    if (logs_.empty()) return snapshot_meta_.last_included_index;
    return logs_.rbegin()->first;
  }

  void Append(const std::vector<LogEntry>& entries) override {
    for (const auto& e : entries) {
      logs_[e.index] = e;
    }
  }

  void TruncatePrefix(uint64_t index) override {
    auto it = logs_.begin();
    while (it != logs_.end() && it->first < index) {
      it = logs_.erase(it);
    }
  }

  void TruncateSuffix(uint64_t index) override {
    auto it = logs_.upper_bound(index);
    logs_.erase(it, logs_.end());
  }

 private:
  HardState hs_;
  RaftSnapshotMeta snapshot_meta_;
  std::map<uint64_t, LogEntry> logs_;
};

TEST(RaftLogTest, NewLogHasIndexZero) {
  auto storage = std::make_shared<MemStorage>();
  RaftLog log(storage);

  EXPECT_EQ(log.FirstIndex(), 1u);
  EXPECT_EQ(log.LastIndex(), 0u);
  EXPECT_EQ(log.commit_index(), 0u);
  EXPECT_EQ(log.applied(), 0u);
}

TEST(RaftLogTest, AppendIncreasesLastIndex) {
  auto storage = std::make_shared<MemStorage>();
  RaftLog log(storage);

  std::vector<LogEntry> entries = {
    {1, 1, "cmd1"},
    {1, 2, "cmd2"},
  };
  log.Append(entries);

  EXPECT_EQ(log.LastIndex(), 2u);
  EXPECT_EQ(log.Term(1), 1u);
  EXPECT_EQ(log.Term(2), 1u);
}

TEST(RaftLogTest, AppendWithGapIsAllowed) {
  auto storage = std::make_shared<MemStorage>();
  RaftLog log(storage);

  std::vector<LogEntry> entries = {
    {2, 5, "x"},
  };
  log.Append(entries);

  // 从 index 5 开始
  EXPECT_EQ(log.FirstIndex(), 5u);
  EXPECT_EQ(log.LastIndex(), 5u);
  EXPECT_EQ(log.Term(5), 2u);
}

TEST(RaftLogTest, EmptyAppendDoesNothing) {
  auto storage = std::make_shared<MemStorage>();
  RaftLog log(storage);

  log.Append({});
  EXPECT_EQ(log.LastIndex(), 0u);
}

TEST(RaftLogTest, MatchLog) {
  auto storage = std::make_shared<MemStorage>();
  RaftLog log(storage);

  log.Append({{1, 1, "a"}, {1, 2, "b"}});

  EXPECT_TRUE(log.MatchLog(1, 1));
  EXPECT_TRUE(log.MatchLog(2, 1));
  EXPECT_FALSE(log.MatchLog(2, 2));
  EXPECT_FALSE(log.MatchLog(3, 1));
}

TEST(RaftLogTest, CommitToAdvancesCommitIndex) {
  auto storage = std::make_shared<MemStorage>();
  RaftLog log(storage);

  log.Append({{1, 1, "a"}, {1, 2, "b"}, {1, 3, "c"}});
  EXPECT_EQ(log.commit_index(), 0u);

  log.CommitTo(2);
  EXPECT_EQ(log.commit_index(), 2u);

  // 不能超过 last index
  log.CommitTo(10);
  EXPECT_EQ(log.commit_index(), 2u);
}

TEST(RaftLogTest, AppliedToAdvancesApplied) {
  auto storage = std::make_shared<MemStorage>();
  RaftLog log(storage);

  log.Append({{1, 1, "a"}, {1, 2, "b"}});
  log.CommitTo(2);

  log.AppliedTo(1);
  EXPECT_EQ(log.applied(), 1u);

  log.AppliedTo(2);
  EXPECT_EQ(log.applied(), 2u);

  // 不能超过 commit_index
  log.AppliedTo(0);
  EXPECT_EQ(log.applied(), 2u);
}

TEST(RaftLogTest, EntriesReturnsRange) {
  auto storage = std::make_shared<MemStorage>();
  RaftLog log(storage);

  log.Append({{1, 1, "a"}, {1, 2, "b"}, {2, 3, "c"}});

  auto got = log.Entries(2, 4);
  ASSERT_EQ(got.size(), 2u);
  EXPECT_EQ(got[0].index, 2u);
  EXPECT_EQ(got[0].data, "b");
  EXPECT_EQ(got[1].index, 3u);
  EXPECT_EQ(got[1].data, "c");
}

TEST(RaftLogTest, EntriesWithInvalidRangeReturnsEmpty) {
  auto storage = std::make_shared<MemStorage>();
  RaftLog log(storage);

  log.Append({{1, 1, "a"}, {1, 2, "b"}});

  EXPECT_TRUE(log.Entries(0, 1).empty());
  EXPECT_TRUE(log.Entries(3, 4).empty());
  EXPECT_TRUE(log.Entries(2, 1).empty());
}

TEST(RaftLogTest, AppendWithConflictTruncates) {
  auto storage = std::make_shared<MemStorage>();
  RaftLog log(storage);

  log.Append({{1, 1, "a"}, {1, 2, "b"}, {1, 3, "c"}});
  EXPECT_EQ(log.LastIndex(), 3u);

  // 从 index 2 开始冲突，覆盖后面的日志
  log.Append({{2, 2, "b2"}, {2, 3, "c2"}, {2, 4, "d"}});

  EXPECT_EQ(log.LastIndex(), 4u);
  EXPECT_EQ(log.Term(2), 2u);
  EXPECT_EQ(log.Term(3), 2u);
  EXPECT_EQ(log.Term(4), 2u);

  // index 1 保持不变
  EXPECT_EQ(log.Term(1), 1u);
}

TEST(RaftLogTest, TermForUnknownIndexReturnsZero) {
  auto storage = std::make_shared<MemStorage>();
  RaftLog log(storage);

  EXPECT_EQ(log.Term(0), 0u);
  EXPECT_EQ(log.Term(1), 0u);
  EXPECT_EQ(log.Term(100), 0u);
}

TEST(RaftLogTest, CommitToBoundary) {
  auto storage = std::make_shared<MemStorage>();
  RaftLog log(storage);

  // 没有日志时 commit 被拒绝
  log.CommitTo(1);
  EXPECT_EQ(log.commit_index(), 0u);

  log.Append({{1, 1, "x"}});
  log.CommitTo(1);
  EXPECT_EQ(log.commit_index(), 1u);
}

TEST(RaftLogTest, CompactionKeepsSnapshotBoundaryTerm) {
  auto storage = std::make_shared<MemStorage>();
  RaftLog log(storage);
  log.Append({{1, 1, "a"}, {1, 2, "b"}, {2, 3, "c"}});
  log.CommitTo(2);
  log.AppliedTo(2);

  ASSERT_TRUE(log.CompactTo(RaftSnapshotMeta{2, 1}));
  EXPECT_EQ(log.FirstIndex(), 3u);
  EXPECT_EQ(log.LastIndex(), 3u);
  EXPECT_EQ(log.Term(2), 1u);
  EXPECT_TRUE(log.MatchLog(2, 1));
  EXPECT_TRUE(log.Entries(1, 3).empty());
  ASSERT_EQ(log.Entries(3, 4).size(), 1u);
  EXPECT_EQ(log.Entries(3, 4)[0].data, "c");
}

TEST(RaftLogTest, RestoreSnapshotDropsOlderEntries) {
  auto storage = std::make_shared<MemStorage>();
  RaftLog log(storage);
  log.Append({{1, 1, "a"}, {1, 2, "b"}, {1, 3, "c"}});

  ASSERT_TRUE(log.RestoreSnapshot(RaftSnapshotMeta{5, 3}));
  EXPECT_EQ(log.FirstIndex(), 6u);
  EXPECT_EQ(log.LastIndex(), 5u);
  EXPECT_EQ(log.Term(5), 3u);
  EXPECT_TRUE(log.Entries(1, 6).empty());
}

TEST(FileRaftStorageTest, SnapshotMetaPersistsAcrossReopen) {
  const std::string path = "test_tmp/raft/snapshot_meta_persistence";
  std::error_code ec;
  std::filesystem::remove_all(path, ec);

  {
    FileRaftStorage storage(path);
    storage.SaveSnapshotMeta(RaftSnapshotMeta{42, 7});
    const RaftSnapshotMeta meta = storage.SnapshotMeta();
    EXPECT_EQ(meta.last_included_index, 42U);
    EXPECT_EQ(meta.last_included_term, 7U);
  }

  {
    FileRaftStorage storage(path);
    const RaftSnapshotMeta meta = storage.SnapshotMeta();
    EXPECT_EQ(meta.last_included_index, 42U);
    EXPECT_EQ(meta.last_included_term, 7U);
  }

  std::filesystem::remove_all(path, ec);
}

TEST(RaftRpcCodecTest, InstallSnapshotRoundTrip) {
  InstallSnapshotArgs args;
  args.term = 4;
  args.leader_id = 2;
  args.meta = RaftSnapshotMeta{17, 3};
  args.data = "checkpoint-bytes";

  const std::string body = EncodeInstallSnapshot(args);
  InstallSnapshotArgs decoded;
  ASSERT_TRUE(DecodeInstallSnapshot(body.data(), body.size(), &decoded));
  EXPECT_EQ(decoded.term, args.term);
  EXPECT_EQ(decoded.leader_id, args.leader_id);
  EXPECT_EQ(decoded.meta.last_included_index,
            args.meta.last_included_index);
  EXPECT_EQ(decoded.meta.last_included_term, args.meta.last_included_term);
  EXPECT_EQ(decoded.data, args.data);

  InstallSnapshotReply reply{4, true, 17};
  const std::string reply_body = EncodeInstallSnapshotReply(reply);
  InstallSnapshotReply decoded_reply;
  ASSERT_TRUE(DecodeInstallSnapshotReply(reply_body.data(), reply_body.size(),
                                          &decoded_reply));
  EXPECT_EQ(decoded_reply.term, 4U);
  EXPECT_TRUE(decoded_reply.success);
  EXPECT_EQ(decoded_reply.match_index, 17U);
}

}  // namespace
}  // namespace raft
}  // namespace kv
