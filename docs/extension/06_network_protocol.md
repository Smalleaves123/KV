# Network Protocol Hardening Guide

The TCP server makes the engine usable as a service. KVEngine already supports
Redis-like commands, RESP-like responses, parser tests, codec tests, session
tests, and socket integration tests. The next protocol work should focus on a
complete RESP request parser, binary-safe values, pipelining, authentication,
and eventually TLS.

## Goals

Short-term goals:

- Fully support RESP array requests.
- Support bulk strings containing spaces, CRLF, null bytes, and arbitrary
  non-UTF-8 payloads.
- Support partial-frame buffering.
- Support pipelining with ordered responses.

Medium-term goals:

- Add `AUTH`.
- Add `requirepass` to server configuration.
- Add optional TLS.
- Improve leader redirection in Raft mode.

## Existing Code To Read First

- `include/kv/net/command.h`
- `src/net/command.cpp`
- `include/kv/net/command_parser.h`
- `src/net/command_parser.cpp`
- `include/kv/net/protocol.h`
- `src/net/protocol.cpp`
- `include/kv/net/codec.h`
- `src/net/codec.cpp`
- `include/kv/net/session.h`
- `src/net/session.cpp`
- `include/kv/net/connection.h`
- `src/net/connection.cpp`
- `include/kv/net/server.h`
- `src/net/server.cpp`
- `apps/kv_server.cpp`
- `apps/server_config.cpp`
- `tests/net/`
- `docs/network_protocol.md`

## Command Representation

Make sure command arguments are byte strings, not whitespace-only tokens.

`std::string` can store null bytes, so this shape is fine:

```cpp
struct Command {
  CommandType type;
  std::vector<std::string> args;
};
```

Rules:

- Do not parse RESP bulk values with whitespace splitting.
- Do not assume values are UTF-8.
- Do not use C string functions that stop at null bytes.
- Error messages can be text; payloads must be bytes.

## RESP Request Parser

RESP array example:

```text
*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nalice\r\n
```

Bulk string values may contain:

- spaces;
- CRLF;
- null bytes;
- arbitrary bytes.

## Incremental Parser

Implement the request parser as a state machine:

```cpp
enum class ParseState {
  kArrayLen,
  kBulkLen,
  kBulkData,
};
```

Return a tri-state result:

```cpp
enum class DecodeResult {
  kNeedMore,
  kOk,
  kError,
};
```

This lets the connection handle both partial reads and multiple pipelined
requests in one buffer.

## Pipelining

A single read buffer may contain multiple commands:

```text
PING\r\nPING\r\nGET key\r\n
```

or RESP:

```text
*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$4\r\nname\r\n
```

Session flow:

1. Read bytes from the connection.
2. Decode as many complete requests as possible.
3. Execute them in order.
4. Write responses in order.
5. Keep incomplete bytes in the buffer for the next read.

Response order must always match request order.

## Line Mode Compatibility

The existing line-oriented mode is useful for demos:

```text
SET name alice
GET name
```

Keep it if practical, but document its limits:

- line mode is not binary-safe;
- values with spaces should use RESP bulk strings;
- null bytes require RESP.

If line mode keeps supporting `SET`, define whether everything after the key is
treated as the value. Even then, RESP remains the correct binary-safe path.

## Response Encoding

Keep Redis-like RESP response types:

- simple string: `+OK\r\n`
- error: `-ERR message\r\n`
- integer: `:1\r\n`
- bulk string: `$5\r\nalice\r\n`
- nil: `$-1\r\n`
- array: `*2\r\n...`

Bulk string lengths must be byte lengths, not character counts.

## Authentication

### Configuration

Add:

```yaml
server:
  requirepass: ""
```

Optional environment variable:

```text
KV_REQUIREPASS=secret
```

An empty password means authentication is disabled.

### Session State

Add:

```cpp
bool authenticated_ = false;
```

If `requirepass` is empty, sessions can start authenticated.

### Command Behavior

Before authentication:

- `AUTH password` is allowed.
- Decide whether `PING` is allowed. The simplest behavior is to reject
  everything except `AUTH`.
- Other commands return a `NOAUTH` error.

On success:

- return `OK`;
- mark the session authenticated.

On failure:

- return an error;
- keep the session unauthenticated.

## TLS

TLS should come after binary-safe RESP and auth. Reserve config shape first:

```yaml
server:
  tls:
    enabled: false
    cert_file: ""
    key_file: ""
```

Implementation concerns:

- OpenSSL or platform TLS dependency management;
- Windows, macOS, and Linux support;
- test certificate generation;
- whether TLS and plain TCP can listen at the same time.

## Raft Leader Redirection

In Raft mode, a follower receiving a write should return enough information for
the client to retry against the leader.

Possible response:

```text
-MOVED 127.0.0.1:9527
```

or:

```text
-ERR not leader; leader=127.0.0.1:9527
```

The Raft server should expose:

```cpp
bool IsLeader() const;
std::optional<NodeAddress> GetLeaderAddress() const;
```

The session layer can then decide whether to execute or redirect.

## Test Plan

Parser tests:

- RESP array command;
- bulk value containing spaces;
- bulk value containing CRLF;
- bulk value containing null bytes;
- partial frame returns `NeedMore`;
- malformed array length;
- malformed bulk length;
- insufficient bulk data;
- multiple pipelined commands.

Session tests:

- pipelined requests execute in order;
- values with spaces round-trip correctly;
- binary values round-trip correctly;
- unauthenticated commands are rejected;
- `AUTH` success enables commands;
- `AUTH` failure keeps commands blocked.

Server integration tests:

- real socket sends RESP;
- real socket sends a frame in multiple chunks;
- real socket sends pipelined commands;
- multiple clients pipeline concurrently.

## Acceptance Criteria

Basic acceptance:

- RESP requests are binary-safe.
- Partial reads do not produce false protocol errors.
- Pipelined responses are ordered.
- Line mode remains compatible for simple usage.

Behavioral acceptance:

- Null bytes do not truncate values.
- Auth defaults to disabled and does not break existing tests.
- When auth is enabled, unauthenticated sessions cannot execute data commands.

## Suggested Pull Request Split

1. Refactor codec into an incremental parser.
2. Fully support RESP bulk array requests.
3. Add pipelining.
4. Add binary-safe command tests.
5. Add `AUTH` and configuration.
6. Add Raft leader redirect.
7. Evaluate TLS.
