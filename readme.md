# ASKClient-Redis

Redis client library for the Async Kernel stack.

Provides two parallel client paths:

- **Async** — `ASKRedisClient` sends commands through the ASK pipeline (Operation DTOs → ASKClient → Transport → TCP socket with RESP protocol)
- **Sync** — `PhpRedisAdapter` wraps the phpredis extension directly via `RedisClientContract`

Infrastructure classes (cache, lockers, queue, job state) use the sync path and implement contracts from `bagart/async-kernel` and `bagart/ask-client`.

## Requirements

- PHP 8.2+
- `ext-redis` (phpredis)

## Installation

```bash
composer require bagart/ask-client-redis
```

Depends on `bagart/async-kernel` and `bagart/ask-client`.

## Architecture

```
Async path:  ASKRedisClient → Operation DTO → ASKClient → Middleware → Pipeline → ASKRedisTransport → Redis
Sync path:   PhpRedisClient → phpredis extension → Redis
```

See `agent.md` for full architectural context.
