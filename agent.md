# ASKClient-Redis Architecture Context

We are working on the `bagart/ask-client-redis` library.

Important: this is NOT a standalone async framework.

All async capabilities are already implemented in `bagart/ask-client`.

Before any changes, first fully analyze the existing project code.

Do not invent a new architecture if it can be implemented using ask-client facilities.

---

# Library's place in the ecosystem

The library has two parallel client paths:

**1. Async path** — Redis commands via ASKClient pipeline:

```
application
        │
        ▼
bagart/ask-client-redis  (ASKRedisClient)
        │
        ▼
bagart/ask-client
        │
        ▼
ASKRedisTransport  (TCP socket + RESP protocol)
        │
        ▼
Redis server
```

**2. Sync path** — Direct phpredis wrapper (used by infrastructure classes):

```
application
        │
        ▼
bagart/ask-client-redis  (PhpRedisClient)
        │
        ▼
ext-redis (phpredis)
        │
        ▼
Redis server
```

The sync path exists because queue/cache/locker infrastructure classes need synchronous Redis access. They implement contracts from `bagart/async-kernel` and `bagart/ask-client`.

---

# Async path — ASKRedisClient

Single user entry point for async Redis operations.

```php
$redis = new ASKRedisClient($askClient);

$value = $redis->get('user:15')->await();

$redis->set('user:15', $user)->await();

$redis->publish('events', ['type' => 'created'])->await();
```

All methods turn into Operation DTOs and are passed to ASKClient:

```
ASKRedisClient → Operation DTO → ASKClient::execute() → Middleware → Pipeline → ASKRedisTransport → Future
```

ASKRedisClient never works directly with sockets.

### Implemented operations (7 DTOs)

`ASKRedisGetOperation`, `ASKRedisSetOperation`, `ASKRedisDelOperation`, `ASKRedisExpireOperation`, `ASKRedisIncrOperation`, `ASKRedisPublishOperation`, `ASKRedisSubscribeOperation`.

### Transport layer

- `ASKRedisConnection` — raw TCP socket (`fsockopen`)
- `ASKRedisProtocolEncoder` — RESP protocol encoder
- `ASKRedisProtocolDecoder` — RESP protocol decoder (all types: +, -, :, $, *)
- `ASKRedisTransport` — async transport via ASKDeferred/ASKPromise
- `ASKRedisTransportAdapter` — bridge to ASKClient transport layer

### Contracts

- `ASKRedisTransportContract` — async transport interface
- `ASKRedisOperationContract` — operation DTO interface (`command()`, `arguments()`)
- `ASKRedisPipelineContract` — async pipeline builder interface
- `ASKRedisSubscriberContract` — pub/sub subscriber interface

### Pipeline & PubSub

- `Pipeline/ASKRedisPipeline` — async pipeline accumulator
- `PubSub/ASKRedisSubscriber` — channel-fixed pub/sub subscriber (only stateful object in async path)

---

# Sync path — PhpRedisClient

Synchronous Redis client wrapping the phpredis extension.

Used by infrastructure classes that need direct Redis access without the ASK pipeline.

### Interface

`RedisClientContract` — comprehensive interface (~50 methods: strings, lists, hashes, sets, sorted sets, streams, scripting, scan, pipeline).

### Implementation

- `Redis/Client/PhpRedisClient` — full phpredis wrapper implementing `RedisClientContract` + `ASKWarmableContract`
- `Redis/Client/PhpRedisPipeline` — pipeline wrapper implementing `RedisPipelineContract`
- `Redis/Connector/PhpRedisConnector` — DSN-based connector (`connect(RedisDsn): Redis`)
- `Redis/RedisDsn` — DSN value object (`tcp://` / `redis://` with query params)

---

# Infrastructure layer

Classes implementing contracts from `bagart/async-kernel` and `bagart/ask-client`. All use the sync path (`RedisClientContract`).

### Cache

- `Cache/RedisCache` — PSR-16 cache via `ASKCacheContract`

### Lockers

- `Lockers/RedisLocker` — distributed locking via `ASKLockerContract` (SET NX EX + Lua compare-and-delete)
- `Lockers/PhpRedisLockerTransport` — adapter from `RedisClientContract` to `RedisLockerTransport`

### Queue

- `Queue/Adapters/QueueRedisAdapter` — ASK queue via LPUSH/RPOP + delayed (ZSET)
- `Queue/RedisDeadLetterQueue` — DLQ with TTL and failure classification

### Redis job infrastructure

- `Redis/RedisActivePartitions` — partition scheduling with penalty-based selection (Lua)
- `Redis/RedisDistributedLock` — partition-level locking with fencing (implements `PartitionLockContract`)
- `Redis/RedisJobDeduplicator` — job deduplication (SET NX + TTL)
- `Redis/RedisJobStateStore` — job state machine (claim, complete, fail, dead-letter, retry, fencing, heartbeat, zombie detection)
- `Redis/RedisPartitionStream` — Redis Streams-based job storage (xAdd/xRead/xDel)
- `Redis/RedisPendingAckRegistry` — pending ack tracking (ZSET + HASH)

---

# What must NOT be duplicated

ASKClient already provides: Future, Runtime, Middleware, Pipeline, Context, Retry, Circuit Breaker, Rate Limit.

This functionality must NOT be duplicated in the Redis library.

---

# Current directory structure

```
src/
    ASKRedisClient.php
    Cache/
        RedisCache.php
    Contracts/
        ASKRedisOperationContract.php
        ASKRedisPipelineContract.php
        ASKRedisSubscriberContract.php
        ASKRedisTransportContract.php
        RedisLockerTransport.php
    Exception/
        ASKRedisConnectionException.php
        ASKRedisException.php
    Lockers/
        PhpRedisLockerTransport.php
        RedisLocker.php
    Operations/
        ASKRedisDelOperation.php
        ASKRedisExpireOperation.php
        ASKRedisGetOperation.php
        ASKRedisIncrOperation.php
        ASKRedisPublishOperation.php
        ASKRedisSetOperation.php
        ASKRedisSubscribeOperation.php
    Pipeline/
        ASKRedisPipeline.php
    PubSub/
        ASKRedisSubscriber.php
    Queue/
        Adapters/
            QueueRedisAdapter.php
        RedisDeadLetterQueue.php
    Redis/
        Client/
            PhpRedisClient.php
            PhpRedisPipeline.php
        Connector/
            PhpRedisConnector.php
        Contract/
            RedisClientInterface.php
            RedisConnectorInterface.php
            RedisPipelineInterface.php
        RedisActivePartitions.php
        RedisDistributedLock.php
        RedisDsn.php
        RedisJobDeduplicator.php
        RedisJobStateStore.php
        RedisPartitionStream.php
        RedisPendingAckRegistry.php
    Transport/
        ASKRedisConnection.php
        ASKRedisProtocolDecoder.php
        ASKRedisProtocolEncoder.php
        ASKRedisTransport.php
        ASKRedisTransportAdapter.php
```

---

# When analyzing each file

Before making changes, determine:

1. Is the class used?
2. Can it be replaced by calling ASKClient?
3. Does it duplicate Future/Runtime/Middleware from ask-client?
4. Is it simply an Operation DTO?
5. Should this code live inside Transport?
6. Is it infrastructure (cache/queue/locker) using the sync path?

Any class that duplicates ask-client functionality must be removed.
