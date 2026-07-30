// src/net/session.cpp
#include "kv/net/session.h"

#include <cctype>
#include <utility>
#include <vector>

#include "kv/net/protocol.h"

namespace kv::net {

Session::Session(DB* db, ClusterManager* cluster_manager,
                 std::string requirepass)
    : db_(db),
      executor_(db, cluster_manager),
      requirepass_(std::move(requirepass)),
      authenticated_(requirepass_.empty()),
      active_txn_(),
      last_txn_event_(TxnEvent::kNone) {}

Session::~Session() {
  if (active_txn_ != nullptr) {
    (void)active_txn_->Rollback();
    active_txn_.reset();
  }
}

void Session::SetLastTxnEvent(TxnEvent event) noexcept {
  last_txn_event_ = event;
}

TxnEvent Session::LastTxnEvent() const noexcept {
  return last_txn_event_;
}

std::string Session::HandleLine(const std::string& line) {
  const Command cmd = CommandParser::ParseLine(line);
  return HandleCommand(cmd);
}

std::string Session::HandleTokens(const std::vector<std::string>& tokens) {
  const Command cmd = CommandParser::ParseTokens(tokens);
  return HandleCommand(cmd);
}

std::string Session::HandleCommand(const Command& cmd) {
  SetLastTxnEvent(TxnEvent::kNone);

  if (cmd.type == CommandType::kAuth) {
    return HandleAuthCommand(cmd);
  }
  if (!authenticated_) {
    return protocol::Error("NOAUTH authentication required");
  }

  switch (cmd.type) {
    case CommandType::kBegin:
    case CommandType::kExec:
    case CommandType::kAbort:
      return HandleTxnCommand(cmd);

    case CommandType::kGet:
    case CommandType::kSet:
    case CommandType::kDel:
    case CommandType::kExpire:
    case CommandType::kTTL:
    case CommandType::kPersist:
    case CommandType::kMGet:
    case CommandType::kInfo:
    case CommandType::kStats:
    case CommandType::kCluster:
    case CommandType::kScan:
    case CommandType::kIncr:
    case CommandType::kIncrBy:
    case CommandType::kDecr:
    case CommandType::kDecrBy:
    case CommandType::kHSet:
    case CommandType::kHGet:
    case CommandType::kHDel:
    case CommandType::kHExists:
    case CommandType::kHLen:
    case CommandType::kHGetAll:
    case CommandType::kHKeys:
    case CommandType::kHVals:
    case CommandType::kLPush:
    case CommandType::kRPush:
    case CommandType::kLPop:
    case CommandType::kRPop:
    case CommandType::kLLen:
    case CommandType::kLIndex:
    case CommandType::kLRange:
      if (active_txn_ != nullptr) {
        return HandleDataCommandInTxn(cmd);
      }
      return executor_.Execute(cmd);

    case CommandType::kPing:
    case CommandType::kInvalid:
    default:
      return executor_.Execute(cmd);
  }
}

std::string Session::HandleAuthCommand(const Command& cmd) {
  using namespace protocol;

  if (cmd.args.size() != 1) {
    return Error("wrong number of arguments for 'AUTH'");
  }
  if (requirepass_.empty()) {
    return Error("AUTH is not enabled");
  }
  if (cmd.args[0] != requirepass_) {
    return Error("invalid password");
  }

  authenticated_ = true;
  return SimpleString("OK");
}

std::string Session::HandleTxnCommand(const Command& cmd) {
  using namespace protocol;

  if (db_ == nullptr) {
    return Error("db is null");
  }

  if (cmd.type == CommandType::kBegin) {
    if (!cmd.args.empty()) {
      return Error("wrong number of arguments for 'BEGIN'");
    }
    if (active_txn_ != nullptr) {
      return Error("transaction already active");
    }

    TxnOptions options;
    Status s = db_->BeginTransaction(options, &active_txn_);
    if (!s.ok()) {
      return Error(s.ToString());
    }
    SetLastTxnEvent(TxnEvent::kBegin);
    return SimpleString("OK");
  }

  if (cmd.type == CommandType::kExec) {
    if (!cmd.args.empty()) {
      return Error("wrong number of arguments for 'EXEC'");
    }
    if (active_txn_ == nullptr) {
      return Error("no active transaction");
    }

    Status s = active_txn_->Commit();
    active_txn_.reset();
    if (!s.ok()) {
      if (s.IsAlreadyExists()) {
        SetLastTxnEvent(TxnEvent::kConflict);
        return Error("transaction conflict");
      }
      return Error(s.ToString());
    }

    SetLastTxnEvent(TxnEvent::kCommit);
    return SimpleString("OK");
  }

  if (cmd.type == CommandType::kAbort) {
    if (!cmd.args.empty()) {
      return Error("wrong number of arguments for 'ABORT'");
    }
    if (active_txn_ == nullptr) {
      return Error("no active transaction");
    }

    Status s = active_txn_->Rollback();
    active_txn_.reset();
    if (!s.ok()) {
      return Error(s.ToString());
    }
    SetLastTxnEvent(TxnEvent::kAbort);
    return SimpleString("OK");
  }

  return Error("unknown transaction command");
}

std::string Session::HandleDataCommandInTxn(const Command& cmd) {
  using namespace protocol;

  if (active_txn_ == nullptr) {
    return Error("no active transaction");
  }

  switch (cmd.type) {
    case CommandType::kGet: {
      if (cmd.args.size() != 1) {
        return Error("wrong number of arguments for 'GET'");
      }
      std::string value;
      Status s = active_txn_->Get(cmd.args[0], &value);
      if (s.ok()) {
        return BulkString(value);
      }
      if (s.IsNotFound()) {
        return Nil();
      }
      return Error(s.ToString());
    }

    case CommandType::kSet: {
      if (cmd.args.size() != 2) {
        return Error("wrong number of arguments for 'SET'");
      }
      Status s = active_txn_->Put(cmd.args[0], cmd.args[1]);
      if (!s.ok()) {
        return Error(s.ToString());
      }
      return SimpleString("OK");
    }

    case CommandType::kDel: {
      if (cmd.args.size() != 1) {
        return Error("wrong number of arguments for 'DEL'");
      }
      Status s = active_txn_->Delete(cmd.args[0]);
      if (!s.ok()) {
        return Error(s.ToString());
      }
      return SimpleString("OK");
    }

    case CommandType::kMGet: {
      if (cmd.args.empty()) {
        return Error("wrong number of arguments for 'MGET'");
      }
      std::vector<std::string> items;
      items.reserve(cmd.args.size());
      for (const auto& key : cmd.args) {
        std::string value;
        Status s = active_txn_->Get(key, &value);
        if (s.ok()) {
          items.push_back(BulkString(value));
        } else if (s.IsNotFound()) {
          items.push_back(Nil());
        } else {
          return Error(s.ToString());
        }
      }
      return Array(items);
    }

    case CommandType::kInfo:
    case CommandType::kStats:
      if (!cmd.args.empty()) {
        return Error("wrong number of arguments for 'INFO/STATS'");
      }
      return executor_.Execute(cmd);

    case CommandType::kScan:
      return executor_.Execute(cmd);

    case CommandType::kCluster: {
      if (cmd.args.empty()) {
        return Error("wrong number of arguments for 'CLUSTER'");
      }
      std::string subcommand = cmd.args[0];
      for (char& ch : subcommand) {
        ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
      }
      if (subcommand == "BATCH") {
        return Error("cluster batch is not supported in transaction");
      }
      return executor_.Execute(cmd);
    }

    default:
      return Error("unsupported command in transaction");
  }
}

}  // namespace kv::net
