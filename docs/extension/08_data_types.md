# Redis-like Data Types Extension Guide

KVEngine already has counters, hashes, and lists exposed through the TCP command
layer. The next data-structure work can add sets, sorted sets, bitmaps, and a
stream-like append-only list. The important part is not only adding commands;
it is also defining persistence, TTL, transactions, type checking, and Raft
atomicity.

## Goals

Short-term goals:

- Add a Set data type and command set.
- Define TTL behavior for compound data types.
- Ensure compound data survives DB reopen.
- Add wrong-type handling.

Medium-term goals:

- Add Sorted Set.
- Add Bitmap.
- Add a stream-like append-only list.
- Replicate compound operations atomically in Raft mode.

## Existing Code To Read First

- `include/kv/data_types/counter.h`
- `src/data_types/counter.cpp`
- `include/kv/data_types/hash.h`
- `src/data_types/hash.cpp`
- `include/kv/data_types/list.h`
- `src/data_types/list.cpp`
- `src/net/command_parser.cpp`
- `src/net/command.cpp`
- `src/net/session.cpp`
- `src/txn/txn.cpp`
- `src/txn/txn_manager.cpp`
- `src/raft/raft_server.cpp`
- `tests/data_types/`
- `tests/net/command_test.cpp`
- `tests/net/parser_test.cpp`
- `tests/net/server_integration_test.cpp`

Related docs:

- `docs/network_protocol.md`
- `docs/transaction.md`
- `docs/raft.md`

## Encoding Strategy

Compound types can start with namespaced internal keys:

```text
type:<logical-key>:<field-or-member> -> encoded-value
```

For a set:

```text
set:myset:alice -> "1"
set:myset:bob   -> "1"
```

This is simple, but it has risks:

- deleting a logical key requires deleting all member keys;
- listing members requires prefix iteration;
- logical keys or members containing `:` can be ambiguous.

A better long-term encoding is length-prefixed:

```text
S <key-len> <key-bytes> <member-len> <member-bytes>
```

Even if the first version uses simple string prefixes, centralize encoding:

```cpp
std::string EncodeSetMemberKey(const Slice& key, const Slice& member);
bool DecodeSetMemberKey(const Slice& encoded,
                        std::string* key,
                        std::string* member);
```

Do not duplicate key-construction rules across command handlers.

## Type Metadata

Decide whether each logical key has type metadata.

Without metadata:

- implementation is simpler;
- wrong-type detection is hard;
- whole-key TTL is harder to model.

With metadata:

```text
meta:<key> -> type=set
```

Benefits:

- `WRONGTYPE` errors become possible;
- whole-key TTL has a natural home;
- existence checks are clearer;
- cardinality and other logical metadata can live nearby.

Recommendation: introduce type metadata before adding Set. It prevents a lot of
awkward migration work later.

## TTL Semantics

Use whole-key TTL for compound types:

- TTL applies to the logical key.
- All members or fields share that TTL.
- Individual member/field expiration is not supported.

This is simpler, close to Redis key-level TTL, and easier to make atomic in
Raft mode.

Define these cases:

- when a logical key expires, compound commands should treat it as missing;
- physical cleanup of member keys can be lazy;
- `TTL myset` returns the logical key TTL;
- `PERSIST myset` removes the logical key TTL;
- writing to an expired logical key creates a new logical key.

## Set

### Command Subset

First version:

- `SADD key member [member ...]`
- `SREM key member [member ...]`
- `SISMEMBER key member`
- `SCARD key`
- `SMEMBERS key`

Response behavior:

- `SADD` returns the number of newly added members.
- `SREM` returns the number of removed members.
- `SISMEMBER` returns 1 or 0.
- `SCARD` returns member count.
- `SMEMBERS` returns an array of members.

### Internal Flow

`SADD`:

1. Check logical key type.
2. If the key does not exist, write type metadata.
3. Write one member key per member.
4. Update cardinality metadata.

`SREM`:

1. Check type.
2. Delete member keys.
3. Update cardinality.
4. If cardinality becomes zero, delete type metadata or leave an empty set,
   depending on the chosen semantics.

`SMEMBERS`:

1. Check type and TTL.
2. Prefix-scan member keys.
3. Decode each member.
4. Return an array.

### Cardinality

Maintain cardinality metadata:

```text
meta:<key>:cardinality -> uint64
```

Multi-member updates should use `WriteBatch` so metadata and member keys are
updated atomically.

## Sorted Set

### Command Subset

First version:

- `ZADD key score member`
- `ZREM key member`
- `ZSCORE key member`
- `ZRANGE key start stop`

### Two Indexes

Sorted sets need lookup by member and ordering by score:

```text
zset-member:<key>:<member> -> score
zset-score:<key>:<encoded-score>:<member> -> marker
```

`ZADD` for an existing member:

1. Read old score.
2. Delete old score index.
3. Write new member index.
4. Write new score index.

### Score Encoding

If lexicographic key order should match numeric score order, encode carefully.

First version recommendation:

- support `int64_t` scores;
- transform to sortable unsigned form;
- encode as fixed-width big-endian bytes.

Add double scores later if needed.

## Bitmap

### Command Subset

First version:

- `SETBIT key offset value`
- `GETBIT key offset`
- `BITCOUNT key`

### Storage

Use chunks:

```text
bitmap:<key>:<chunk-index> -> bytes
```

For example, each chunk can be 4096 bytes.

Benefits:

- large bitmaps do not require rewriting one huge value;
- sparse bitmaps use space more efficiently.

Define:

- maximum offset;
- value must be 0 or 1;
- bit ordering inside a byte;
- behavior for missing chunks.

## Stream-like Append-only List

### Command Subset

First version:

- `XADD key id field value [field value ...]`
- `XRANGE key start end [COUNT n]`
- `XLEN key`

Simplify at first:

- support auto-generated ids;
- do not support consumer groups;
- do not support trimming until the core format is stable.

### ID Format

Use a Redis-like id:

```text
milliseconds-sequence
```

Within the same millisecond, sequence must increase.

Storage:

```text
stream:<key>:<id> -> encoded fields
meta:<key>:last_id -> id
```

## Transaction Semantics

Compound commands often expand into multiple internal writes. They must be
atomic:

- either all internal writes become visible;
- or none of them do.

Use `WriteBatch` for compound mutations.

If a compound command is used inside `MULTI/EXEC`:

- do not eagerly mutate the DB before `EXEC`;
- store the logical command or a deferred batch;
- validate type/version at `EXEC` time.

If this is too large for the first version, explicitly reject unsupported
compound commands inside transactions and add tests for that behavior.

## Raft Semantics

In Raft mode, one user command should map to one atomic proposal.

Avoid this:

```text
SADD key a b c
  -> propose member a
  -> propose member b
  -> propose member c
```

That can partially apply if the process fails between proposals.

Prefer:

- encode the logical command in one Raft log entry;
- apply it through the state machine;
- generate a `WriteBatch` during apply;
- make the whole command visible atomically.

The unsupported-command handling in `src/raft/raft_server.cpp` is a natural
future extension point.

## Wrong-Type Handling

If a key is a string value, `SADD key member` should return a wrong-type error.

Define logical types:

- string/plain KV;
- counter;
- hash;
- list;
- set;
- sorted set;
- bitmap;
- stream.

If existing counter/hash/list behavior does not use unified type metadata, plan
a compatibility path before enforcing strict wrong-type checks everywhere.

## Test Plan

Data type unit tests:

- `SADD`, `SREM`, `SISMEMBER`, `SCARD`, `SMEMBERS`;
- `ZADD`, `ZREM`, `ZSCORE`, `ZRANGE`;
- `SETBIT`, `GETBIT`, `BITCOUNT`;
- `XADD`, `XRANGE`, `XLEN`;
- duplicate members;
- empty keys and members;
- large members;
- binary members after RESP is binary-safe.

Persistence tests:

- write compound data;
- close and reopen;
- verify members, cardinality, scores, bitmap bits, and stream entries.

TTL tests:

- set TTL on a logical key;
- after expiration, compound commands treat the key as missing;
- `PERSIST` removes expiration;
- TTL does not apply to individual members.

Network tests:

- parser arity validation;
- response format;
- wrong arity;
- wrong-type errors;
- compound commands in pipelined requests.

Transaction tests:

- compound mutations become visible after `EXEC`;
- rollback makes them invisible;
- concurrent modification conflicts behave as documented.

Raft tests:

- execute compound command on leader;
- followers reach the same state;
- restart preserves state;
- one compound command does not partially apply.

## Acceptance Criteria

Basic acceptance:

- New types have unit tests and network command tests.
- Data survives reopen.
- Wrong-type behavior is explicit.
- TTL semantics are documented.

Behavioral acceptance:

- Multi-member mutations are atomically visible.
- Raft mode does not partially replicate a compound command.
- Key encoding is robust for special characters and binary payloads.

## Suggested Pull Request Split

1. Add unified logical key type metadata.
2. Implement Set encoding and local operations.
3. Expose Set network commands.
4. Add Set TTL, persistence, and transaction tests.
5. Implement Sorted Set.
6. Implement Bitmap.
7. Implement stream-like list.
8. Wire compound commands into Raft apply.
