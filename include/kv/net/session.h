#pragma once

#include <memory>
#include <string>
#include <vector>

#include "kv/engine/db.h"
#include "kv/net/command.h"
#include "kv/net/command_parser.h"

namespace kv {
class ClusterManager;
}

namespace kv::net {

enum class TxnEvent {
  kNone = 0,
  kBegin,
  kCommit,
  kAbort,
  kConflict,
};

class Session {
 public:
  explicit Session(DB* db, ClusterManager* cluster_manager = nullptr);
  ~Session();

  // 输入一行命令，返回编码后的响应
  std::string HandleLine(const std::string& line);
  std::string HandleTokens(const std::vector<std::string>& tokens);
  TxnEvent LastTxnEvent() const noexcept;

 private:
  std::string HandleCommand(const Command& cmd);
  std::string HandleTxnCommand(const Command& cmd);
  std::string HandleDataCommandInTxn(const Command& cmd);
  void SetLastTxnEvent(TxnEvent event) noexcept;

  DB* db_;
  CommandExecutor executor_;
  std::unique_ptr<Transaction> active_txn_;
  TxnEvent last_txn_event_;
};

}  // namespace kv::net
