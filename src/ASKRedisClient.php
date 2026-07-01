<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis;

use BAGArt\ASKClient\Contracts\ASKFutureContract;
use BAGArt\ASKClient\Contracts\Client\ASKClientContract;
use BAGArt\ASKClientRedis\Contracts\ASKRedisPipelineContract;
use BAGArt\ASKClientRedis\Contracts\ASKRedisSubscriberContract;
use BAGArt\ASKClientRedis\Operations\ASKRedisDelOperation;
use BAGArt\ASKClientRedis\Operations\ASKRedisExpireOperation;
use BAGArt\ASKClientRedis\Operations\ASKRedisGetOperation;
use BAGArt\ASKClientRedis\Operations\ASKRedisIncrOperation;
use BAGArt\ASKClientRedis\Operations\ASKRedisPublishOperation;
use BAGArt\ASKClientRedis\Operations\ASKRedisSetOperation;
use BAGArt\ASKClientRedis\Pipeline\ASKRedisPipeline;
use BAGArt\ASKClientRedis\PubSub\ASKRedisSubscriber;
use BAGArt\ASKClientRedis\Transport\ASKRedisTransportAdapter;

/**
 * ASKRedisClient — thin domain adapter for Redis operations.
 *
 * Execution flow:
 *   Operation → ASKClient → Middleware pipeline → Pipeline → RedisTransportAdapter → ASKFuture → await()
 *
 * This class does NOT know:
 *   - Socket/protocol specifics (delegated to Transport)
 *   - AsyncKernel internals (Fibers, PromiseResolver)
 *   - Runtime scheduling
 *   - Middleware/pipeline internals
 */
final class ASKRedisClient
{
    public function __construct(
        private readonly ASKClientContract $client,
        private readonly ASKRedisTransportAdapter $adapter,
    ) {
    }

    public function get(string $key): ASKFutureContract
    {
        return $this->client->execute(new ASKRedisGetOperation($key));
    }

    public function set(string $key, mixed $value, ?int $ttl = null): ASKFutureContract
    {
        return $this->client->execute(new ASKRedisSetOperation($key, $value, $ttl));
    }

    public function del(string ...$keys): ASKFutureContract
    {
        return $this->client->execute(new ASKRedisDelOperation($keys));
    }

    public function incr(string $key, int $by = 1): ASKFutureContract
    {
        return $this->client->execute(new ASKRedisIncrOperation($key, $by));
    }

    public function expire(string $key, int $seconds): ASKFutureContract
    {
        return $this->client->execute(new ASKRedisExpireOperation($key, $seconds));
    }

    public function pipeline(): ASKRedisPipelineContract
    {
        return new ASKRedisPipeline($this->client);
    }

    public function subscribe(string $channel): ASKRedisSubscriberContract
    {
        return new ASKRedisSubscriber($this->client, $this->adapter, $channel);
    }

    public function publish(string $channel, mixed $payload): ASKFutureContract
    {
        return $this->client->execute(new ASKRedisPublishOperation($channel, $payload));
    }
}
