<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis;

use BAGArt\AskQueue\Contracts\PartitionLockContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;

final class RedisDistributedLock implements PartitionLockContract
{
    private const string SUFFIX_LOCK = 'partition:lock:';

    private const string SUFFIX_FENCING = 'partition:fencing:';

    private const string LUA_RENEW = <<<'LUA'
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("EXPIRE", KEYS[1], ARGV[2])
end
return 0
LUA;

    private const string LUA_RELEASE = <<<'LUA'
if redis.call("GET", KEYS[1]) == ARGV[1] then
    redis.call("DEL", KEYS[1])
    redis.call("DEL", KEYS[2])
    return 1
end
return 0
LUA;

    private const string LUA_TAKEOVER = <<<'LUA'
local val = redis.call("GET", KEYS[1])
if val == false then
    local token = ARGV[3]
    redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2], "NX")
    redis.call("HSET", KEYS[2], "fencingToken", token)
    return 1
end
return 0
LUA;

    private const string LUA_CHECK_AND_ACQUIRE = <<<'LUA'
local key = KEYS[1]
local fencingKey = KEYS[2]
local workerId = ARGV[1]
local ttlSeconds = ARGV[2]
local fencingToken = ARGV[3]

local current = redis.call("GET", key)
local ttl = redis.call("TTL", key)

-- Lock is free (doesn't exist or expired)
if current == false or ttl == -2 then
    redis.call("SET", key, workerId, "EX", ttlSeconds, "NX")
    redis.call("HSET", fencingKey, "fencingToken", fencingToken)
    return 1
end

-- Lock exists but is owned by same worker (re-acquire / extend)
if current == workerId then
    redis.call("EXPIRE", key, ttlSeconds)
    return 1
end

-- Lock is held by another worker
return 0
LUA;

    public function __construct(
        private readonly RedisClientContract $redis,
        private readonly string $prefix = 'ASK:',
    ) {
    }

    private function key(string $partitionKey): string
    {
        return $this->prefix.'{'.$partitionKey.'}'.self::SUFFIX_LOCK.$partitionKey;
    }

    private function fencingKey(string $partitionKey): string
    {
        return $this->prefix.'{'.$partitionKey.'}'.self::SUFFIX_FENCING.$partitionKey;
    }

    private function generateFencingToken(): string
    {
        return (string)microtime(true).'.'.bin2hex(random_bytes(8));
    }

    public function acquire(string $partitionKey, string $workerId, int $ttlSeconds): bool
    {
        $key = $this->key($partitionKey);
        $fencingToken = $this->generateFencingToken();

        $acquired = (bool)$this->redis->set($key, $workerId, ['NX', 'EX' => $ttlSeconds]);

        if ($acquired) {
            $this->redis->hSet($this->fencingKey($partitionKey), 'fencingToken', $fencingToken);
        }

        return $acquired;
    }

    public function renew(string $partitionKey, string $workerId, int $ttlSeconds): bool
    {
        $key = $this->key($partitionKey);

        return (bool)$this->redis->eval(
            self::LUA_RENEW,
            [$key, $workerId, $ttlSeconds],
            1,
        );
    }

    public function release(string $partitionKey, string $workerId): void
    {
        $key = $this->key($partitionKey);

        $this->redis->eval(
            self::LUA_RELEASE,
            [$key, $this->fencingKey($partitionKey), $workerId],
            2,
        );
    }

    public function isOwnedBy(string $partitionKey, string $workerId): bool
    {
        $key = $this->key($partitionKey);

        return $this->redis->get($key) === $workerId;
    }

    public function isExpired(string $partitionKey): bool
    {
        $key = $this->key($partitionKey);

        $ttl = $this->redis->ttl($key);

        return $ttl === -2 || $ttl === -1;
    }

    /**
     * Atomic check: is the lock owned by the given worker AND not expired?
     * Avoids TOCTOU race between isOwnedBy() + isExpired().
     */
    public function isOwnedByAndActive(string $partitionKey, string $workerId): bool
    {
        $key = $this->key($partitionKey);

        $current = $this->redis->get($key);

        return $current === $workerId;
    }

    /**
     * Acquire with automatic background renewal.
     *
     * Returns a renewal closure that must be called periodically (e.g. every
     * ttlSeconds/3) to extend the lock. When the caller stops calling the
     * renewal closure, the lock expires naturally — no explicit release needed.
     *
     * @return \Closure(): void Renewal callback — call periodically to extend the lock.
     */
    public function acquireWithRenewal(string $partitionKey, string $workerId, int $ttlSeconds): \Closure
    {
        $acquired = $this->acquire($partitionKey, $workerId, $ttlSeconds);

        if (!$acquired) {
            return static function (): void {
            };
        }

        $lock = $this;

        return function () use ($lock, $partitionKey, $workerId, $ttlSeconds): void {
            $lock->renew($partitionKey, $workerId, $ttlSeconds);
        };
    }

    public function takeover(string $partitionKey, string $workerId, int $ttlSeconds): bool
    {
        $key = $this->key($partitionKey);
        $fencingToken = $this->generateFencingToken();

        return (bool)$this->redis->eval(
            self::LUA_TAKEOVER,
            [$key, $this->fencingKey($partitionKey), $workerId, $ttlSeconds, $fencingToken],
            2,
        );
    }

    public function getOwner(string $partitionKey): ?string
    {
        $key = $this->key($partitionKey);
        $val = $this->redis->get($key);

        return $val === false ? null : $val;
    }

    public function getFencingToken(string $partitionKey): ?string
    {
        $val = $this->redis->hGet($this->fencingKey($partitionKey), 'fencingToken');

        return $val !== false ? $val : null;
    }
}
