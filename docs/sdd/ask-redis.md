# ASK Redis — SDD

> `bagart/ask-client-redis` — the platform's Redis runtime (queues, DLQ, locks, cache).

## Model (verified 2026-09-17)

- **Fiber-native:** single non-blocking connection shared across fibers; `Connection/` owns reconnect/backoff.
- **Queue:** Redis Streams behind `OutboundQueueContract`; channels `tg-outbound`, DLQ `tg-dlq:{botId}`; visibility timeout = lease; `LeaseRenewableQueueContract` for renewal; atomic DLQ ops via `AtomicDlqQueueContract`.
- **Cache:** `RedisOutboundCache` — `incrementWithTtl` = Lua INCR + EXPIRE NX (TTL only on key creation); multi-worker safe. `KernelCacheAdapter` get-check-set is NOT multi-worker safe.
- **State purity:** only readonly DTO JSON + counters; never connections/closures/behavior objects.

## Rules

Keys are prefixed (`tg_outbound:*`). Poison pills handled upstream (RetryBudget → DLQ). Lazy connect; flush nothing in constructors.

## Phase 2 Reliability Hardening (2026-09-20)

### Stream & Queue

- **ACK Atomicity**: `RedisPartitionStream::ack()` Lua changed `DEL ownershipKey` → `EXPIRE ownershipKey 1`; 24h TTL safety net for client-disconnect edge case.
- **PendingAck TOCTOU**: `cleanupPartition()` converted to atomic Lua script (LUA_CLEANUP); `add()` also atomic via LUA_ADD.
- **Partition Growth**: Handled by Lua cleanup — atomically removes from `stream:pending:partitions` when empty.

### PubSub

- **Error Masking**: `readPubSubMessage()` now falls back to `error_log()` when logger is null; failures always visible.
- **Re-subscribe**: Verified `subscribe()` already called after every failure; reconnection depends on transport layer.

### Redis Fiber Connection

- **Auto-Reconnect**: `read()` and `write()` now auto-reconnect with max 3 attempts on connection loss.

### Job State

- **Zombie Leak**: `LUA_MARK_FAILED`, `LUA_MARK_DEAD_LETTER`, `LUA_MARK_RETRY` now include `ZREM zombieKey jobId` atomically; removed separate PHP-level `zRem` calls.

### Deduplication

- **CRC32 Collision**: Replaced `crc32()` with `hash('xxh3', ...)` for better collision resistance.

### Partitions

- **Penalty O(N)**: Bounded `ZRANGE` in `decayPenalties()` to max 1000 entries per call.
- **Float Precision**: Threshold `< 0.1` already removes near-zero entries; no change needed.
