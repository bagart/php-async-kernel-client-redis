# ASK Redis — SDD

> `bagart/ask-client-redis` — the platform's Redis runtime (queues, DLQ, locks, cache).

## Model (verified 2026-09-17)

- **Fiber-native:** single non-blocking connection shared across fibers; `Connection/` owns reconnect/backoff.
- **Queue:** Redis Streams behind `OutboundQueueContract`; channels `tg-outbound`, DLQ `tg-dlq:{botId}`; visibility timeout = lease; `LeaseRenewableQueueContract` for renewal; atomic DLQ ops via `AtomicDlqQueueContract`.
- **Cache:** `RedisOutboundCache` — `incrementWithTtl` = Lua INCR + EXPIRE NX (TTL only on key creation); multi-worker safe. `KernelCacheAdapter` get-check-set is NOT multi-worker safe.
- **State purity:** only readonly DTO JSON + counters; never connections/closures/behavior objects.

## Rules

Keys are prefixed (`tg_outbound:*`). Poison pills handled upstream (RetryBudget → DLQ). Lazy connect; flush nothing in constructors.
