#include "kv/data_types/hash.h"

#include <cstring>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "kv/engine/db.h"

namespace kv {

// ── 编码格式 ──────────────────────────────────────
// [field_count:4B (LE)]
//   [field1_len:4B (LE)][field1_data][value1_len:4B (LE)][value1_data]
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

std::string Hash::Encode(const std::map<std::string, std::string>& fields) {
  std::string buf;
  PutU32(&buf, static_cast<uint32_t>(fields.size()));
  for (const auto& [f, v] : fields) {
    PutU32(&buf, static_cast<uint32_t>(f.size()));
    buf.append(f);
    PutU32(&buf, static_cast<uint32_t>(v.size()));
    buf.append(v);
  }
  return buf;
}

bool Hash::Decode(const std::string& data,
                  std::map<std::string, std::string>* fields) {
  fields->clear();
  size_t pos = 0;
  uint32_t count = 0;
  if (!GetU32(data.data(), data.size(), &pos, &count)) return false;

  for (uint32_t i = 0; i < count; ++i) {
    uint32_t flen = 0, vlen = 0;
    if (!GetU32(data.data(), data.size(), &pos, &flen)) return false;
    if (pos + flen > data.size()) return false;
    std::string f(data.data() + pos, flen);
    pos += flen;

    if (!GetU32(data.data(), data.size(), &pos, &vlen)) return false;
    if (pos + vlen > data.size()) return false;
    std::string v(data.data() + pos, vlen);
    pos += vlen;

    (*fields)[f] = v;
  }
  return true;
}

// ── 公共接口 ──────────────────────────────────────

Status Hash::HSet(DB* db, const Slice& key, const Slice& field,
                  const Slice& value, int* created) {
  if (db == nullptr) return Status::InvalidArgument("db is null");
  constexpr int kMaxAttempts = 8;
  for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
    std::unique_ptr<Transaction> txn;
    Status s = db->BeginTransaction(TxnOptions{}, &txn);
    if (!s.ok()) return s;

    std::string raw;
    s = txn->Get(key, &raw);
    std::map<std::string, std::string> fields;
    if (s.ok()) {
      if (!Decode(raw, &fields)) {
        (void)txn->Rollback();
        return Status::Corruption("hash encoding corrupted");
      }
    } else if (!s.IsNotFound()) {
      (void)txn->Rollback();
      return s;
    }

    bool is_new = fields.find(field.ToString()) == fields.end();
    fields[field.ToString()] = value.ToString();
    s = txn->Put(key, Encode(fields));
    if (!s.ok()) {
      (void)txn->Rollback();
      return s;
    }
    s = txn->Commit();
    if (s.ok()) {
      if (created) *created = is_new ? 1 : 0;
      return Status::OK();
    }
    if (!s.IsAlreadyExists()) return s;
  }
  return Status::AlreadyExists("hash update conflict");
}

Status Hash::HGet(DB* db, const Slice& key, const Slice& field,
                  std::string* value) {
  std::string raw;
  Status s = db->Get(ReadOptions{}, key, &raw);
  if (!s.ok()) return s;

  std::map<std::string, std::string> fields;
  if (!Decode(raw, &fields)) {
    return Status::Corruption("hash encoding corrupted");
  }

  auto it = fields.find(field.ToString());
  if (it == fields.end()) {
    return Status::NotFound("field not found");
  }
  *value = it->second;
  return Status::OK();
}

Status Hash::HDel(DB* db, const Slice& key, const Slice& field,
                  int* deleted) {
  if (db == nullptr) return Status::InvalidArgument("db is null");
  constexpr int kMaxAttempts = 8;
  for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
    std::unique_ptr<Transaction> txn;
    Status s = db->BeginTransaction(TxnOptions{}, &txn);
    if (!s.ok()) return s;

    std::string raw;
    s = txn->Get(key, &raw);
    if (s.IsNotFound()) {
      (void)txn->Rollback();
      if (deleted) *deleted = 0;
      return Status::OK();
    }
    if (!s.ok()) {
      (void)txn->Rollback();
      return s;
    }

    std::map<std::string, std::string> fields;
    if (!Decode(raw, &fields)) {
      (void)txn->Rollback();
      return Status::Corruption("hash encoding corrupted");
    }
    auto it = fields.find(field.ToString());
    if (it == fields.end()) {
      (void)txn->Rollback();
      if (deleted) *deleted = 0;
      return Status::OK();
    }

    fields.erase(it);
    s = fields.empty() ? txn->Delete(key) : txn->Put(key, Encode(fields));
    if (!s.ok()) {
      (void)txn->Rollback();
      return s;
    }
    s = txn->Commit();
    if (s.ok()) {
      if (deleted) *deleted = 1;
      return Status::OK();
    }
    if (!s.IsAlreadyExists()) return s;
  }
  return Status::AlreadyExists("hash update conflict");
}

Status Hash::HExists(DB* db, const Slice& key, const Slice& field,
                     bool* exists) {
  std::string raw;
  Status s = db->Get(ReadOptions{}, key, &raw);
  if (s.IsNotFound()) {
    *exists = false;
    return Status::OK();
  }
  if (!s.ok()) return s;

  std::map<std::string, std::string> fields;
  if (!Decode(raw, &fields)) {
    return Status::Corruption("hash encoding corrupted");
  }

  *exists = fields.find(field.ToString()) != fields.end();
  return Status::OK();
}

Status Hash::HLen(DB* db, const Slice& key, size_t* len) {
  std::string raw;
  Status s = db->Get(ReadOptions{}, key, &raw);
  if (s.IsNotFound()) {
    *len = 0;
    return Status::OK();
  }
  if (!s.ok()) return s;

  std::map<std::string, std::string> fields;
  if (!Decode(raw, &fields)) {
    return Status::Corruption("hash encoding corrupted");
  }

  *len = fields.size();
  return Status::OK();
}

Status Hash::HGetAll(DB* db, const Slice& key,
                     std::map<std::string, std::string>* fields) {
  std::string raw;
  Status s = db->Get(ReadOptions{}, key, &raw);
  if (s.IsNotFound()) {
    fields->clear();
    return Status::OK();
  }
  if (!s.ok()) return s;

  if (!Decode(raw, fields)) {
    return Status::Corruption("hash encoding corrupted");
  }
  return Status::OK();
}

Status Hash::HKeys(DB* db, const Slice& key,
                   std::vector<std::string>* field_names) {
  std::map<std::string, std::string> fields;
  Status s = HGetAll(db, key, &fields);
  if (!s.ok()) return s;

  field_names->clear();
  field_names->reserve(fields.size());
  for (const auto& [f, _] : fields) {
    field_names->push_back(f);
  }
  return Status::OK();
}

Status Hash::HVals(DB* db, const Slice& key,
                   std::vector<std::string>* values) {
  std::map<std::string, std::string> fields;
  Status s = HGetAll(db, key, &fields);
  if (!s.ok()) return s;

  values->clear();
  values->reserve(fields.size());
  for (const auto& [_, v] : fields) {
    values->push_back(v);
  }
  return Status::OK();
}

}  // namespace kv
