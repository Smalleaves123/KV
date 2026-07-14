#pragma once

#include <cstdint>
#include <string>

#include "kv/common/socket_compat.h"
#include "kv/raft/raft_rpc.h"

namespace kv {
namespace raft {

// ---- wire format constants ----

// Message types on the wire
enum class RaftMsgType : uint8_t {
  kRequestVote      = 0x01,
  kRequestVoteReply = 0x02,
  kAppendEntries    = 0x03,
  kAppendEntriesReply = 0x04,
};

// Max message size (10 MB)
constexpr uint32_t kMaxRaftMsgSize = 10 * 1024 * 1024;

// ---- framing: [4 bytes BE total_length][1 byte msg_type][body...] ----

// Encode a message with framing. Returns the complete wire bytes.
std::string EncodeMessage(RaftMsgType type, const std::string& body);

// Decode a framed message.  Sets *type and *body.  Returns false on error.
bool DecodeMessage(const char* data, size_t len,
                   RaftMsgType* type, std::string* body);

// ---- individual RPC serialization ----

// RequestVote
std::string EncodeRequestVote(const RequestVoteArgs& args);
bool DecodeRequestVote(const char* data, size_t len, RequestVoteArgs* args);

std::string EncodeRequestVoteReply(const RequestVoteReply& reply);
bool DecodeRequestVoteReply(const char* data, size_t len,
                            RequestVoteReply* reply);

// AppendEntries
std::string EncodeAppendEntries(const AppendEntriesArgs& args);
bool DecodeAppendEntries(const char* data, size_t len,
                         AppendEntriesArgs* args);

std::string EncodeAppendEntriesReply(const AppendEntriesReply& reply);
bool DecodeAppendEntriesReply(const char* data, size_t len,
                              AppendEntriesReply* reply);

// ---- Raft log command encoding (for Propose payload) ----

// Encode a Put command: 'S' + key_len(4 BE) + key + value_len(4 BE) + value
std::string EncodePutCmd(const std::string& key, const std::string& value);

// Encode a Put with an absolute wall-clock expiry (0 means persistent).
std::string EncodePutWithExpiryCmd(const std::string& key,
                                   const std::string& value,
                                   uint64_t expires_at_ms);

// Encode a logical expiry update. The value is read at apply time on each
// replica; zero clears the expiry.
std::string EncodeExpireCmd(const std::string& key, uint64_t expires_at_ms);

// Encode a Delete command: 'D' + key_len(4 BE) + key
std::string EncodeDelCmd(const std::string& key);

// Encode a no-op barrier command: 'N'
std::string EncodeNoopCmd();

// Decode a command. Sets *op ('S', 'D', 'T', 'E', or 'N'), *key, *value.
// Returns false on parse error.
bool DecodeCmd(const std::string& data, char* op, std::string* key,
               std::string* value, uint64_t* expires_at_ms = nullptr);

// ---- I/O helpers (handle EINTR, short reads/writes) ----

// Read exactly `len` bytes from fd into `data`.  Returns bytes read, or -1.
platform::SocketIoResult ReadFull(platform::SocketHandle fd, void* data,
                                   size_t len);

// Write all `len` bytes from `data` to fd.  Returns bytes written, or -1.
platform::SocketIoResult WriteFull(platform::SocketHandle fd,
                                   const void* data, size_t len);

// Read one complete framed message from fd.  Returns the raw bytes.
// On EOF or error, returns an empty string.
std::string ReadMessage(platform::SocketHandle fd);

}  // namespace raft
}  // namespace kv
