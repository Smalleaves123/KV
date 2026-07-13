#include "kv/engine/db_impl.h"

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace kv {
namespace {

class OCCTransaction final : public Transaction {
public:
  OCCTransaction(DBImpl* db, TxnOptions options, uint64_t start_seq)
      : db_(db), options_(options), start_seq_(start_seq), active_(true), read_set_(),
        writes_(), write_index_() {}

  ~OCCTransaction() override {
    if (active_) {
      RollbackNoStatus();
    }
  }

  Status Get(const Slice& key, std::string* value) override {
    if (!active_) {
      return Status::InvalidArgument("transaction already finished");
    }
    if (db_ == nullptr) {
      return Status::IOError("transaction db is null");
    }
    Status s = db_->TxnValidateKey(key);
    if (!s.ok()) {
      return s;
    }
    if (value == nullptr) {
      return Status::InvalidArgument("value output pointer is null");
    }

    const std::string key_str = key.ToString();
    auto it = write_index_.find(key_str);
    if (it != write_index_.end()) {
      const auto& op = writes_[it->second];
      if (op.type == WriteBatch::ValueType::kDelete) {
        return Status::NotFound("key deleted");
      }
      *value = op.value;
      return Status::OK();
    }

    s = db_->TxnGetAtSequence(key, start_seq_, value);
    read_set_.insert(key_str);
    return s;
  }

  Status Put(const Slice& key, const Slice& value) override {
    if (!active_) {
      return Status::InvalidArgument("transaction already finished");
    }
    if (db_ == nullptr) {
      return Status::IOError("transaction db is null");
    }
    Status s = db_->TxnValidateKey(key);
    if (!s.ok()) {
      return s;
    }

    WriteBatch::Operation op;
    op.type = WriteBatch::ValueType::kPut;
    op.key = key.ToString();
    op.value = value.ToString();
    UpsertWrite(std::move(op));
    return Status::OK();
  }

  Status Delete(const Slice& key) override {
    if (!active_) {
      return Status::InvalidArgument("transaction already finished");
    }
    if (db_ == nullptr) {
      return Status::IOError("transaction db is null");
    }
    Status s = db_->TxnValidateKey(key);
    if (!s.ok()) {
      return s;
    }

    WriteBatch::Operation op;
    op.type = WriteBatch::ValueType::kDelete;
    op.key = key.ToString();
    UpsertWrite(std::move(op));
    return Status::OK();
  }

  Status Commit() override {
    if (!active_) {
      return Status::InvalidArgument("transaction already finished");
    }
    if (db_ == nullptr) {
      return Status::IOError("transaction db is null");
    }

    Status s = db_->TxnCommitOCC(options_, start_seq_, read_set_, writes_);
    active_ = false;
    db_->TxnUnregister(this);
    return s;
  }

  Status Rollback() override {
    if (!active_) {
      return Status::InvalidArgument("transaction already finished");
    }
    if (db_ == nullptr) {
      return Status::IOError("transaction db is null");
    }

    active_ = false;
    read_set_.clear();
    writes_.clear();
    write_index_.clear();
    db_->TxnUnregister(this);
    return Status::OK();
  }

private:
  void RollbackNoStatus() noexcept {
    active_ = false;
    read_set_.clear();
    writes_.clear();
    write_index_.clear();
    if (db_ != nullptr) {
      db_->TxnUnregister(this);
    }
  }

  void UpsertWrite(WriteBatch::Operation operation) {
    auto [it, inserted] = write_index_.insert({operation.key, writes_.size()});
    if (inserted) {
      writes_.push_back(std::move(operation));
    } else {
      writes_[it->second] = std::move(operation);
    }
  }

  DBImpl* db_;
  TxnOptions options_;
  uint64_t start_seq_;
  bool active_;
  std::unordered_set<std::string> read_set_;
  std::vector<WriteBatch::Operation> writes_;
  std::unordered_map<std::string, size_t> write_index_;
};

} // namespace

std::unique_ptr<Transaction> NewOCCTransaction(DBImpl* db, TxnOptions options,
                                               uint64_t start_seq) {
  return std::make_unique<OCCTransaction>(db, options, start_seq);
}

} // namespace kv
