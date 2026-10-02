<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis;

use BAGArt\AskQueue\Contracts\PendingAckRegistryContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;

final class RedisPendingAckRegistry implements PendingAckRegistryContract
{
    private const string SUFFIX_PENDING = 'stream:pending:';

    private const string SUFFIX_ENTRY = 'stream:entry:';

    private const string SUFFIX_OWNERSHIP = 'stream:ownership:';

    private const string SUFFIX_PARTITIONS = 'stream:pending:partitions';

    private const string LUA_ADD = <<<'LUA'
local pendingKey = KEYS[1]
local entryKey = KEYS[2]
local ownershipKey = KEYS[3]

local jobId = ARGV[1]
local entryId = ARGV[2]
local now = ARGV[3]
local workerId = ARGV[4]
local fencingToken = ARGV[5]

redis.call('ZADD', pendingKey, now, jobId)
redis.call('HSET', entryKey, jobId, entryId)

if workerId ~= '' and fencingToken ~= '' then
    redis.call('HSET', ownershipKey, jobId .. ':workerId', workerId)
    redis.call('HSET', ownershipKey, jobId .. ':fencingToken', fencingToken)
end

return 1
LUA;

    private const string LUA_CLEANUP = <<<'LUA'
local pendingKey = KEYS[1]
local entryKey = KEYS[2]
local ownershipKey = KEYS[3]
local partitionsKey = KEYS[4]
local partitionKey = ARGV[1]

if redis.call('ZCARD', pendingKey) == 0 then
    redis.call('DEL', pendingKey)
    redis.call('DEL', entryKey)
    redis.call('DEL', ownershipKey)
    redis.call('SREM', partitionsKey, partitionKey)
    return 1
end

return 0
LUA;

    public function __construct(
        private readonly RedisClientContract $redis,
        private readonly string $prefix = 'ASK:',
    ) {
    }

    private function pendingKey(string $partitionKey): string
    {
        return $this->prefix.'{'.$partitionKey.'}'.self::SUFFIX_PENDING.$partitionKey;
    }

    private function entryKey(string $partitionKey): string
    {
        return $this->prefix.'{'.$partitionKey.'}'.self::SUFFIX_ENTRY.$partitionKey;
    }

    private function ownershipKey(string $partitionKey): string
    {
        return $this->prefix.'{'.$partitionKey.'}'.self::SUFFIX_OWNERSHIP.$partitionKey;
    }

    private function partitionsKey(): string
    {
        return $this->prefix.self::SUFFIX_PARTITIONS;
    }

    public function add(
        string $partitionKey,
        string $jobId,
        string $entryId,
        string $workerId = '',
        string $fencingToken = ''
    ): void {
        $this->redis->eval(
            self::LUA_ADD,
            [
                $this->pendingKey($partitionKey),
                $this->entryKey($partitionKey),
                $this->ownershipKey($partitionKey),
                $jobId,
                $entryId,
                (string)time(),
                $workerId,
                $fencingToken,
            ],
            3,
        );

        $this->redis->sAdd($this->partitionsKey(), $partitionKey);
    }

    public function remove(string $partitionKey, string $jobId): void
    {
        $this->redis->zRem($this->pendingKey($partitionKey), $jobId);
        $this->redis->hDel($this->entryKey($partitionKey), $jobId);

        $key = $this->ownershipKey($partitionKey);
        $this->redis->hDel($key, $jobId.':workerId');
        $this->redis->hDel($key, $jobId.':fencingToken');
    }

    public function getPending(string $partitionKey): array
    {
        $jobIds = $this->redis->zRange($this->pendingKey($partitionKey), 0, -1);

        if (!$jobIds) {
            return [];
        }

        $entryMap = $this->redis->hGetAll($this->entryKey($partitionKey)) ?: [];

        $result = [];

        foreach ($jobIds as $jobId) {
            if (isset($entryMap[$jobId])) {
                $result[$jobId] = $entryMap[$jobId];
            }
        }

        return $result;
    }

    public function getPendingOwnership(string $partitionKey, string $jobId): ?array
    {
        $key = $this->ownershipKey($partitionKey);

        $workerId = $this->redis->hGet($key, $jobId.':workerId');
        $fencingToken = $this->redis->hGet($key, $jobId.':fencingToken');

        $entryMap = $this->redis->hGetAll($this->entryKey($partitionKey)) ?: [];
        $entryId = $entryMap[$jobId] ?? null;

        if ($entryId === null) {
            return null;
        }

        return [
            'entryId' => $entryId,
            'workerId' => $workerId !== false ? $workerId : '',
            'fencingToken' => $fencingToken !== false ? $fencingToken : '',
        ];
    }

    public function getPartitions(): array
    {
        $parts = $this->redis->sMembers($this->partitionsKey());

        return $parts ?: [];
    }

    public function cleanupPartition(string $partitionKey): void
    {
        $this->redis->eval(
            self::LUA_CLEANUP,
            [
                $this->pendingKey($partitionKey),
                $this->entryKey($partitionKey),
                $this->ownershipKey($partitionKey),
                $this->partitionsKey(),
                $partitionKey,
            ],
            4,
        );
    }
}
