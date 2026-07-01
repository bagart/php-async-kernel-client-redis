<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis;

use BAGArt\ASKClient\Contracts\Queue\PartitionLockContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;

final class RedisDistributedLock implements PartitionLockContract
{
    private const string SUFFIX_LOCK = 'partition:lock:';

    private const string LUA_RENEW = <<<'LUA'
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("EXPIRE", KEYS[1], ARGV[2])
end
return 0
LUA;

    private const string LUA_RELEASE = <<<'LUA'
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
LUA;

    private const string LUA_TAKEOVER = <<<'LUA'
local val = redis.call("GET", KEYS[1])
if val == false then
    return redis.call("SET", KEYS[1], ARGV[1], "EX", ARGV[2], "NX")
end
return 0
LUA;

    public function __construct(
        private readonly RedisClientContract $redis,
        private readonly string $prefix = 'ASK:',
    ) {
    }

    private function key(string $partitionKey): string
    {
        return $this->prefix.self::SUFFIX_LOCK.$partitionKey;
    }

    public function acquire(string $partitionKey, string $workerId, int $ttlSeconds): bool
    {
        $key = $this->key($partitionKey);

        return (bool)$this->redis->set($key, $workerId, ['NX', 'EX' => $ttlSeconds]);
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
            [$key, $workerId],
            1,
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

    public function takeover(string $partitionKey, string $workerId, int $ttlSeconds): bool
    {
        $key = $this->key($partitionKey);

        return (bool)$this->redis->eval(
            self::LUA_TAKEOVER,
            [$key, $workerId, $ttlSeconds],
            1,
        );
    }

    public function getOwner(string $partitionKey): ?string
    {
        $key = $this->key($partitionKey);
        $val = $this->redis->get($key);

        return $val === false ? null : $val;
    }
}
