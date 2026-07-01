<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Lockers;

use BAGArt\ASKClientRedis\Contracts\RedisLockerTransport;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;

/**
 * Adapter from {@see RedisClientContract} → {@see RedisLockerTransport}.
 *
 * Needed because PHP requires explicit `implements` for typed parameters:
 * a typed argument `RedisLockerTransport $redis` won't accept a generic
 * `RedisClientInterface` directly. This class is a thin wrapper with no logic:
 * it simply delegates 2 methods.
 *
 * Production usage:
 *   $locker = new RedisLocker(PhpRedisLockerTransport::fromClient($redisClient));
 *
 * Or equivalently: `new PhpRedisLockerTransport($redisClient)`.
 */
final class PhpRedisLockerTransport implements RedisLockerTransport
{
    public function __construct(
        private readonly RedisClientContract $redis,
    ) {
    }

    /**
     * Factory constructor for conciseness at creation points.
     */
    public static function fromClient(RedisClientContract $redis): self
    {
        return new self($redis);
    }

    public function set(string $key, string $value, array $options = []): bool
    {
        /** @var array<int|string, mixed> $options */
        $result = $this->redis->set($key, $value, $options);

        return $result === true;
    }

    public function eval(string $script, array $args = [], int $numKeys = 0): mixed
    {
        /** @var list<mixed> $args */
        return $this->redis->eval($script, $args, $numKeys);
    }
}
