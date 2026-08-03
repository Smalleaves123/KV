#include "kv/raft/raft_rpc_codec.h"

namespace kv {
namespace raft {

// ---- internal BE helpers ----

namespace {

void PushBE64(std::string* out, uint64_t v) {
  for (int i = 7; i >= 0; --i)
    out->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}
void PushBE32(std::string* out, uint32_t v) {
  for (int i = 3; i >= 0; --i)
    out->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}
void PushBE8(std::string* out, uint8_t v) {
  out->push_back(static_cast<char>(v));
}

uint64_t PopBE64(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i)
    v = (v << 8) | static_cast<uint8_t>(p[i]);
  return v;
}
uint32_t PopBE32(const char* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i)
    v = (v << 8) | static_cast<uint8_t>(p[i]);
  return v;
}

}  // namespace

// ======================== Framing ========================

std::string EncodeMessage(RaftMsgType type, const std::string& body) {
  std::string msg;
  uint32_t total = 1 + static_cast<uint32_t>(body.size());
  msg.reserve(4 + total);
  PushBE32(&msg, total);
  PushBE8(&msg, static_cast<uint8_t>(type));
  msg.append(body);
  return msg;
}

bool DecodeMessage(const char* data, size_t len,
                   RaftMsgType* type, std::string* body) {
  if (len < 5) return false;

  uint32_t total = PopBE32(data);
  if (total < 1 || total > kMaxRaftMsgSize) return false;
  if (4 + total > len) return false;

  uint8_t t = static_cast<uint8_t>(data[4]);
  switch (t) {
    case 0x01: *type = RaftMsgType::kRequestVote; break;
    case 0x02: *type = RaftMsgType::kRequestVoteReply; break;
    case 0x03: *type = RaftMsgType::kAppendEntries; break;
    case 0x04: *type = RaftMsgType::kAppendEntriesReply; break;
    case 0x05: *type = RaftMsgType::kInstallSnapshot; break;
    case 0x06: *type = RaftMsgType::kInstallSnapshotReply; break;
    default: return false;
  }

  body->assign(data + 5, total - 1);
  return true;
}

// ======================== RequestVote ========================

std::string EncodeRequestVote(const RequestVoteArgs& args) {
  std::string out;
  out.reserve(32);
  PushBE64(&out, args.term);
  PushBE64(&out, args.candidate_id);
  PushBE64(&out, args.last_log_index);
  PushBE64(&out, args.last_log_term);
  return out;
}

bool DecodeRequestVote(const char* data, size_t len,
                       RequestVoteArgs* args) {
  if (len < 32) return false;
  args->term           = PopBE64(data);
  args->candidate_id   = PopBE64(data + 8);
  args->last_log_index = PopBE64(data + 16);
  args->last_log_term  = PopBE64(data + 24);
  return true;
}

// ======================== RequestVoteReply ========================

std::string EncodeRequestVoteReply(const RequestVoteReply& reply) {
  std::string out;
  out.reserve(9);
  PushBE64(&out, reply.term);
  PushBE8(&out, reply.vote_granted ? 1 : 0);
  return out;
}

bool DecodeRequestVoteReply(const char* data, size_t len,
                            RequestVoteReply* reply) {
  if (len < 9) return false;
  reply->term         = PopBE64(data);
  reply->vote_granted = (data[8] != 0);
  return true;
}

// ======================== AppendEntries ========================

std::string EncodeAppendEntries(const AppendEntriesArgs& args) {
  std::string out;
  // Header: 5 × uint64 + 1 × uint32 = 44 bytes
  out.reserve(44 + args.entries.size() * (20 + 64));
  PushBE64(&out, args.term);
  PushBE64(&out, args.leader_id);
  PushBE64(&out, args.prev_log_index);
  PushBE64(&out, args.prev_log_term);
  PushBE64(&out, args.leader_commit);
  PushBE32(&out, static_cast<uint32_t>(args.entries.size()));

  for (const auto& e : args.entries) {
    PushBE64(&out, e.term);
    PushBE64(&out, e.index);
    PushBE32(&out, static_cast<uint32_t>(e.data.size()));
    out.append(e.data);
  }
  return out;
}

bool DecodeAppendEntries(const char* data, size_t len,
                         AppendEntriesArgs* args) {
  if (len < 44) return false;

  args->term           = PopBE64(data);
  args->leader_id      = PopBE64(data + 8);
  args->prev_log_index = PopBE64(data + 16);
  args->prev_log_term  = PopBE64(data + 24);
  args->leader_commit  = PopBE64(data + 32);

  uint32_t count = PopBE32(data + 40);
  const char* p = data + 44;

  args->entries.clear();
  args->entries.reserve(count);

  for (uint32_t i = 0; i < count; ++i) {
    if (p + 20 > data + len) return false;

    LogEntry e;
    e.term  = PopBE64(p);
    e.index = PopBE64(p + 8);
    uint32_t data_len = PopBE32(p + 16);
    p += 20;

    if (p + data_len > data + len) return false;
    e.data.assign(p, data_len);
    p += data_len;

    args->entries.push_back(std::move(e));
  }
  return true;
}

// ======================== AppendEntriesReply ========================

std::string EncodeAppendEntriesReply(const AppendEntriesReply& reply) {
  std::string out;
  out.reserve(33);
  PushBE64(&out, reply.term);
  PushBE8(&out, reply.success ? 1 : 0);
  PushBE64(&out, reply.match_index);
  PushBE64(&out, reply.conflict_index);
  PushBE64(&out, reply.conflict_term);
  return out;
}

bool DecodeAppendEntriesReply(const char* data, size_t len,
                              AppendEntriesReply* reply) {
  if (len < 9) return false;
  reply->term    = PopBE64(data);
  reply->success = (data[8] != 0);
  reply->match_index = 0;
  reply->conflict_index = 0;
  reply->conflict_term = 0;
  if (len >= 17) {
    reply->match_index = PopBE64(data + 9);
  }
  if (len >= 25) {
    reply->conflict_index = PopBE64(data + 17);
  }
  if (len >= 33) {
    reply->conflict_term = PopBE64(data + 25);
  }
  return true;
}

// ======================== InstallSnapshot ========================

std::string EncodeInstallSnapshot(const InstallSnapshotArgs& args) {
  std::string out;
  out.reserve(36 + args.data.size());
  PushBE64(&out, args.term);
  PushBE64(&out, args.leader_id);
  PushBE64(&out, args.meta.last_included_index);
  PushBE64(&out, args.meta.last_included_term);
  PushBE32(&out, static_cast<uint32_t>(args.data.size()));
  out.append(args.data);
  return out;
}

bool DecodeInstallSnapshot(const char* data, size_t len,
                           InstallSnapshotArgs* args) {
  if (len < 36) return false;
  args->term = PopBE64(data);
  args->leader_id = PopBE64(data + 8);
  args->meta.last_included_index = PopBE64(data + 16);
  args->meta.last_included_term = PopBE64(data + 24);
  const uint32_t data_len = PopBE32(data + 32);
  if (data_len > len - 36) return false;
  args->data.assign(data + 36, data_len);
  return true;
}

std::string EncodeInstallSnapshotReply(const InstallSnapshotReply& reply) {
  std::string out;
  out.reserve(17);
  PushBE64(&out, reply.term);
  PushBE8(&out, reply.success ? 1 : 0);
  PushBE64(&out, reply.match_index);
  return out;
}

bool DecodeInstallSnapshotReply(const char* data, size_t len,
                                InstallSnapshotReply* reply) {
  if (len < 17) return false;
  reply->term = PopBE64(data);
  reply->success = data[8] != 0;
  reply->match_index = PopBE64(data + 9);
  return true;
}

// ======================== Command encoding ========================

std::string EncodePutCmd(const std::string& key, const std::string& value) {
  std::string cmd;
  cmd.reserve(1 + 4 + key.size() + 4 + value.size());
  cmd.push_back('S');
  PushBE32(&cmd, static_cast<uint32_t>(key.size()));
  cmd.append(key);
  PushBE32(&cmd, static_cast<uint32_t>(value.size()));
  cmd.append(value);
  return cmd;
}

std::string EncodePutWithExpiryCmd(const std::string& key,
                                   const std::string& value,
                                   uint64_t expires_at_ms) {
  std::string cmd;
  cmd.reserve(1 + 4 + key.size() + 4 + value.size() + 8);
  cmd.push_back('T');
  PushBE32(&cmd, static_cast<uint32_t>(key.size()));
  cmd.append(key);
  PushBE32(&cmd, static_cast<uint32_t>(value.size()));
  cmd.append(value);
  PushBE64(&cmd, expires_at_ms);
  return cmd;
}

std::string EncodeExpireCmd(const std::string& key, uint64_t expires_at_ms) {
  std::string cmd;
  cmd.reserve(1 + 4 + key.size() + 8);
  cmd.push_back('E');
  PushBE32(&cmd, static_cast<uint32_t>(key.size()));
  cmd.append(key);
  PushBE64(&cmd, expires_at_ms);
  return cmd;
}

std::string EncodeDelCmd(const std::string& key) {
  std::string cmd;
  cmd.reserve(1 + 4 + key.size());
  cmd.push_back('D');
  PushBE32(&cmd, static_cast<uint32_t>(key.size()));
  cmd.append(key);
  return cmd;
}

std::string EncodeNoopCmd() {
  return std::string(1, 'N');
}

bool DecodeCmd(const std::string& data, char* op, std::string* key,
               std::string* value, uint64_t* expires_at_ms) {
  if (data.empty()) return false;

  *op = data[0];
  if (expires_at_ms != nullptr) *expires_at_ms = 0;
  if (*op == 'N') {
    key->clear();
    value->clear();
    return data.size() == 1;
  }
  if (*op != 'S' && *op != 'D' && *op != 'T' && *op != 'E') return false;

  if (data.size() < 5) return false;
  uint32_t key_len = PopBE32(data.data() + 1);
  if (5 + key_len > data.size()) return false;

  key->assign(data.data() + 5, key_len);

  if (*op == 'E') {
    const char* ep = data.data() + 5 + key_len;
    if (ep + 8 > data.data() + data.size()) return false;
    value->clear();
    if (expires_at_ms != nullptr) *expires_at_ms = PopBE64(ep);
  } else if (*op == 'S' || *op == 'T') {
    const char* vp = data.data() + 5 + key_len;
    if (vp + 4 > data.data() + data.size()) return false;
    uint32_t value_len = PopBE32(vp);
    vp += 4;
    if (vp + value_len > data.data() + data.size()) return false;
    value->assign(vp, value_len);
    if (*op == 'T') {
      const char* ep = vp + value_len;
      if (ep + 8 > data.data() + data.size()) return false;
      if (expires_at_ms != nullptr) *expires_at_ms = PopBE64(ep);
    }
  } else {
    value->clear();
  }
  return true;
}

// ======================== I/O helpers ========================

platform::SocketIoResult ReadFull(platform::SocketHandle fd, void* data,
                                  size_t len) {
  auto* p = static_cast<char*>(data);
  size_t nread = 0;
  while (nread < len) {
    const platform::SocketIoResult n =
        platform::ReceiveSocket(fd, p + nread, len - nread, 0);
    if (n < 0) {
      if (platform::IsInterruptedSocketError(platform::LastSocketError()))
        continue;
      return -1;
    }
    if (n == 0) break;
    nread += static_cast<size_t>(n);
  }
  return static_cast<platform::SocketIoResult>(nread);
}

platform::SocketIoResult WriteFull(platform::SocketHandle fd,
                                   const void* data, size_t len) {
  auto* p = static_cast<const char*>(data);
  size_t written = 0;
  while (written < len) {
    const platform::SocketIoResult n =
        platform::SendSocket(fd, p + written, len - written, 0);
    if (n < 0) {
      if (platform::IsInterruptedSocketError(platform::LastSocketError()))
        continue;
      return -1;
    }
    written += static_cast<size_t>(n);
  }
  return static_cast<platform::SocketIoResult>(written);
}

std::string ReadMessage(platform::SocketHandle fd) {
  // Length prefix (4 bytes BE)
  uint8_t len_buf[4];
  if (ReadFull(fd, len_buf, 4) != 4) return {};

  uint32_t total = PopBE32(reinterpret_cast<const char*>(len_buf));
  if (total < 1 || total > kMaxRaftMsgSize) return {};

  // Read the rest (type + body)
  std::string msg(total, '\0');
  if (ReadFull(fd, msg.data(), total) !=
      static_cast<platform::SocketIoResult>(total))
    return {};

  // Prepend the length for DecodeMessage
  std::string full;
  full.reserve(4 + total);
  full.append(reinterpret_cast<const char*>(len_buf), 4);
  full.append(msg);
  return full;
}

}  // namespace raft
}  // namespace kv
