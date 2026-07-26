#include "kv/data_types/list.h"

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "kv/engine/db.h"

namespace kv {

// ── 编码格式 ──────────────────────────────────────
// [elem_count:4B (LE)]
//   [elem1_len:4B (LE)][elem1_data]
//   [elem2_len:4B (LE)][elem2_data]
//   ...

static void PutU32(std::string* buf, uint32_t v) {
  buf->push_back(static_cast<char>(v & 0xFF));
  buf->push_back(static_cast<char>((v >> 8) & 0xFF));
  buf->push_back(static_cast<char>((v >> 16) & 0xFF));
  buf->push_back(static_cast<char>((v >> 24) & 0xFF));
}

static bool GetU32(const char* data, size_t size, size_t* pos,
                   uint32_t* out) {
  if (*pos + 4 > size) return false;
  *out = static_cast<uint32_t>(static_cast<unsigned char>(data[*pos])) |
         (static_cast<uint32_t>(static_cast<unsigned char>(data[*pos + 1]))
          << 8) |
         (static_cast<uint32_t>(static_cast<unsigned char>(data[*pos + 2]))
          << 16) |
         (static_cast<uint32_t>(static_cast<unsigned char>(data[*pos + 3]))
          << 24);
  *pos += 4;
  return true;
}

std::string List::Encode(const std::vector<std::string>& elems) {
  std::string buf;
  PutU32(&buf, static_cast<uint32_t>(elems.size()));
  for (const auto& e : elems) {
    PutU32(&buf, static_cast<uint32_t>(e.size()));
    buf.append(e);
  }
  return buf;
}

bool List::Decode(const std::string& data,
                  std::vector<std::string>* elems) {
  elems->clear();
  size_t pos = 0;
  uint32_t count = 0;
  if (!GetU32(data.data(), data.size(), &pos, &count)) return false;

  elems->reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    uint32_t len = 0;
    if (!GetU32(data.data(), data.size(), &pos, &len)) return false;
    if (pos + len > data.size()) return false;
    elems->push_back(std::string(data.data() + pos, len));
    pos += len;
  }
  return true;
}

// ── 内部辅助 ──────────────────────────────────────

// 从 DB 读取 list，解码；若不存在返回空列表
static Status LoadList(DB* db, const Slice& key,
                       std::vector<std::string>* elems) {
  std::string raw;
  Status s = db->Get(ReadOptions{}, key, &raw);
  if (s.IsNotFound()) {
    elems->clear();
    return Status::OK();
  }
  if (!s.ok()) return s;
  if (!List::Decode(raw, elems)) {
    return Status::Corruption("list encoding corrupted");
  }
  return Status::OK();
}

static Status LoadList(Transaction* txn, const Slice& key,
                       std::vector<std::string>* elems) {
  std::string raw;
  Status s = txn->Get(key, &raw);
  if (s.IsNotFound()) {
    elems->clear();
    return Status::OK();
  }
  if (!s.ok()) return s;
  if (!List::Decode(raw, elems)) {
    return Status::Corruption("list encoding corrupted");
  }
  return Status::OK();
}

static Status StoreList(Transaction* txn, const Slice& key,
                        const std::vector<std::string>& elems) {
  if (elems.empty()) return txn->Delete(key);
  return txn->Put(key, List::Encode(elems));
}

// ── 公共接口 ──────────────────────────────────────

Status List::LPush(DB* db, const Slice& key, const Slice& value,
                   size_t* new_len) {
  if (db == nullptr) return Status::InvalidArgument("db is null");
  constexpr int kMaxAttempts = 8;
  for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
    std::unique_ptr<Transaction> txn;
    Status s = db->BeginTransaction(TxnOptions{}, &txn);
    if (!s.ok()) return s;
    std::vector<std::string> elems;
    s = LoadList(txn.get(), key, &elems);
    if (!s.ok()) return s;
    elems.insert(elems.begin(), value.ToString());
    s = StoreList(txn.get(), key, elems);
    if (!s.ok()) return s;
    s = txn->Commit();
    if (s.ok()) {
      if (new_len) *new_len = elems.size();
      return Status::OK();
    }
    if (!s.IsAlreadyExists()) return s;
  }
  return Status::AlreadyExists("list update conflict");
}

Status List::RPush(DB* db, const Slice& key, const Slice& value,
                   size_t* new_len) {
  if (db == nullptr) return Status::InvalidArgument("db is null");
  constexpr int kMaxAttempts = 8;
  for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
    std::unique_ptr<Transaction> txn;
    Status s = db->BeginTransaction(TxnOptions{}, &txn);
    if (!s.ok()) return s;
    std::vector<std::string> elems;
    s = LoadList(txn.get(), key, &elems);
    if (!s.ok()) return s;
    elems.push_back(value.ToString());
    s = StoreList(txn.get(), key, elems);
    if (!s.ok()) return s;
    s = txn->Commit();
    if (s.ok()) {
      if (new_len) *new_len = elems.size();
      return Status::OK();
    }
    if (!s.IsAlreadyExists()) return s;
  }
  return Status::AlreadyExists("list update conflict");
}

Status List::LPop(DB* db, const Slice& key, std::string* value) {
  if (db == nullptr) return Status::InvalidArgument("db is null");
  constexpr int kMaxAttempts = 8;
  for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
    std::unique_ptr<Transaction> txn;
    Status s = db->BeginTransaction(TxnOptions{}, &txn);
    if (!s.ok()) return s;
    std::vector<std::string> elems;
    s = LoadList(txn.get(), key, &elems);
    if (!s.ok()) return s;
    if (elems.empty()) {
      (void)txn->Rollback();
      return Status::NotFound("list is empty");
    }
    const std::string popped = elems.front();
    elems.erase(elems.begin());
    s = StoreList(txn.get(), key, elems);
    if (!s.ok()) return s;
    s = txn->Commit();
    if (s.ok()) {
      if (value) *value = popped;
      return Status::OK();
    }
    if (!s.IsAlreadyExists()) return s;
  }
  return Status::AlreadyExists("list update conflict");
}

Status List::RPop(DB* db, const Slice& key, std::string* value) {
  if (db == nullptr) return Status::InvalidArgument("db is null");
  constexpr int kMaxAttempts = 8;
  for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
    std::unique_ptr<Transaction> txn;
    Status s = db->BeginTransaction(TxnOptions{}, &txn);
    if (!s.ok()) return s;
    std::vector<std::string> elems;
    s = LoadList(txn.get(), key, &elems);
    if (!s.ok()) return s;
    if (elems.empty()) {
      (void)txn->Rollback();
      return Status::NotFound("list is empty");
    }
    const std::string popped = elems.back();
    elems.pop_back();
    s = StoreList(txn.get(), key, elems);
    if (!s.ok()) return s;
    s = txn->Commit();
    if (s.ok()) {
      if (value) *value = popped;
      return Status::OK();
    }
    if (!s.IsAlreadyExists()) return s;
  }
  return Status::AlreadyExists("list update conflict");
}

Status List::LLen(DB* db, const Slice& key, size_t* len) {
  std::vector<std::string> elems;
  Status s = LoadList(db, key, &elems);
  if (!s.ok()) return s;

  *len = elems.size();
  return Status::OK();
}

// Convert a negative index without clamping positive out-of-range indexes.
static int64_t NormalizeIndex(int64_t idx, int64_t size) {
  return idx < 0 ? idx + size : idx;
}

Status List::LIndex(DB* db, const Slice& key, int64_t index,
                    std::string* value) {
  std::vector<std::string> elems;
  Status s = LoadList(db, key, &elems);
  if (!s.ok()) return s;

  if (elems.empty()) {
    return Status::NotFound("list is empty");
  }

  int64_t idx = NormalizeIndex(index, static_cast<int64_t>(elems.size()));
  if (idx < 0 || static_cast<size_t>(idx) >= elems.size()) {
    return Status::NotFound("index out of range");
  }

  *value = elems[static_cast<size_t>(idx)];
  return Status::OK();
}

Status List::LRange(DB* db, const Slice& key, int64_t start,
                    int64_t stop,
                    std::vector<std::string>* values) {
  std::vector<std::string> elems;
  Status s = LoadList(db, key, &elems);
  if (!s.ok()) return s;

  int64_t size = static_cast<int64_t>(elems.size());
  if (size == 0) {
    values->clear();
    return Status::OK();
  }

  int64_t s_idx = NormalizeIndex(start, size);
  int64_t e_idx = NormalizeIndex(stop, size);
  if (s_idx < 0) s_idx = 0;
  if (e_idx >= size) e_idx = size - 1;

  if (start >= size || stop < -size || s_idx > e_idx) {
    values->clear();
    return Status::OK();
  }

  values->clear();
  values->reserve(static_cast<size_t>(e_idx - s_idx + 1));
  for (int64_t i = s_idx; i <= e_idx; ++i) {
    values->push_back(elems[static_cast<size_t>(i)]);
  }
  return Status::OK();
}

}  // namespace kv
