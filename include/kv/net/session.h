#pragma once

#include <memory>
#include <string>

#include "kv/engine/db.h"
#include "kv/net/command.h"
#include "kv/net/command_parser.h"

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
  explicit Session(DB* db);
  ~Session();

  // 输入一行命令，返回编码后的响应
  std::string HandleLine(const std::string& line);
  TxnEvent LastTxnEvent() const noexcept;

 private:
  std::string HandleTxnCommand(const Command& cmd);
  std::string HandleDataCommandInTxn(const Command& cmd);
  void SetLastTxnEvent(TxnEvent event) noexcept;

  DB* db_;
  CommandExecutor executor_;
  std::unique_ptr<Transaction> active_txn_;
  TxnEvent last_txn_event_;
};

}  // namespace kv::net
