// src/net/command.cpp
#include "kv/net/command.h"

#include <string>
#include <vector>

#include "kv/net/protocol.h"

namespace kv::net {

CommandExecutor::CommandExecutor(DB* db) : db_(db) {}

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
