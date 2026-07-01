<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis;

use BAGArt\ASKClient\Contracts\Queue\ActivePartitionsContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;

final class RedisActivePartitions implements ActivePartitionsContract
{
    private const string SUFFIX_ACTIVE = 'active_partitions';

    private const string SUFFIX_PENALTIES = 'partition:penalties';

    public function __construct(
        private readonly RedisClientContract $redis,
        private readonly int $penaltyWeight = 1000,
        private readonly int $selectionLimit = 10,
        private readonly string $prefix = 'ASK:',
    ) {
    }

    private function activeKey(): string
    {
        return $this->prefix.self::SUFFIX_ACTIVE;
    }

    private function penaltyKey(): string
    {
        return $this->prefix.self::SUFFIX_PENALTIES;
    }

    public function markActive(string $partitionKey, int $availableAt): void
    {
        $this->redis->zAdd($this->activeKey(), $availableAt, $partitionKey);
    }

    public function claimNext(): ?string
    {
        $lua = <<<'LUA'
local key = KEYS[1]
local penaltyKey = KEYS[2]
local now = tonumber(ARGV[1])
local penaltyWeight = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

local items = redis.call('ZRANGEBYSCORE', key, '-inf', now, 'LIMIT', 0, limit)

if (#items == 0) then
    return nil
end

local best = nil
local bestScore = nil

for _, partition in ipairs(items) do
    local score = tonumber(redis.call('ZSCORE', key, partition)) or 0
    local penalties = tonumber(redis.call('ZSCORE', penaltyKey, partition)) or 0

    local effective = score + (penalties * penaltyWeight)

    if best == nil or effective < bestScore then
        best = partition
        bestScore = effective
    end
end

if best == nil then
    return nil
end

redis.call('ZREM', key, best)

redis.call('ZINCRBY', penaltyKey, 1, best)

return best
LUA;

        $result = $this->redis->eval(
            $lua,
            [
                $this->activeKey(),
                $this->penaltyKey(),
                time(),
                $this->penaltyWeight,
                $this->selectionLimit,
            ],
            2
        );

        return $result ? (string)$result : null;
    }

    public function claimBatch(int $count): array
    {
        $lua = <<<'LUA'
local key = KEYS[1]
local penaltyKey = KEYS[2]
local now = tonumber(ARGV[1])
local penaltyWeight = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local batch = tonumber(ARGV[4])

local items = redis.call('ZRANGEBYSCORE', key, '-inf', now, 'LIMIT', 0, limit)

if (#items == 0) then
    return {}
end

local scored = {}

for _, p in ipairs(items) do
    local score = tonumber(redis.call('ZSCORE', key, p)) or 0
    local penalty = tonumber(redis.call('ZSCORE', penaltyKey, p)) or 0
    local effective = score + penalty * penaltyWeight
    table.insert(scored, {p, effective})
end

table.sort(scored, function(a, b)
    return a[2] < b[2]
end)

local result = {}

for i=1, math.min(batch, #scored) do
    local partition = scored[i][1]
    redis.call('ZREM', key, partition)
    redis.call('ZINCRBY', penaltyKey, 1, partition)
    table.insert(result, partition)
end

return result
LUA;

        $result = $this->redis->eval(
            $lua,
            [
                $this->activeKey(),
                $this->penaltyKey(),
                time(),
                $this->penaltyWeight,
                $this->selectionLimit,
                $count,
            ],
            2
        );

        return $result ? array_map(strval(...), $result) : [];
    }

    public function claimNextAdaptive(int $streamPressure, int $retryPressure, int $zombieRate): array
    {
        $baseDelay = 0;

        if ($streamPressure > 10000) {
            $baseDelay += 5;
        } elseif ($streamPressure > 5000) {
            $baseDelay += 2;
        }

        if ($retryPressure > 1000) {
            $baseDelay += 3;
        } elseif ($retryPressure > 500) {
            $baseDelay += 1;
        }

        if ($zombieRate > 20) {
            $baseDelay += 5;
        } elseif ($zombieRate > 10) {
            $baseDelay += 2;
        }

        $partition = $this->claimNext();

        return [
            'partition' => $partition,
            'delay' => $baseDelay,
        ];
    }

    public function decayPenalties(float $factor = 0.9): void
    {
        $lua = <<<'LUA'
local key = KEYS[1]
local factor = tonumber(ARGV[1])

local items = redis.call('ZRANGE', key, 0, -1, 'WITHSCORES')

for i=1,#items,2 do
    local member = items[i]
    local score = tonumber(items[i+1]) or 0

    local newScore = score * factor

    if newScore < 0.1 then
        redis.call('ZREM', key, member)
    else
        redis.call('ZADD', key, newScore, member)
    end
end

return 1
LUA;

        $this->redis->eval(
            $lua,
            [
                $this->penaltyKey(),
                $factor,
            ],
            1,
        );
    }

    public function resetPenalty(string $partitionKey): void
    {
        $this->redis->zRem($this->penaltyKey(), $partitionKey);
    }

    public function requeue(string $partitionKey, int $delaySeconds): void
    {
        $this->markActive($partitionKey, time() + $delaySeconds);
    }

    public function remove(string $partitionKey): void
    {
        $this->redis->zRem($this->activeKey(), $partitionKey);
        $this->redis->zRem($this->penaltyKey(), $partitionKey);
    }

    public function count(): int
    {
        return (int)$this->redis->zCard($this->activeKey());
    }

    public function isScheduled(string $partitionKey): bool
    {
        return $this->redis->zScore($this->activeKey(), $partitionKey) !== false;
    }

    public function scan(int $cursor, int $count): array
    {
        $result = $this->redis->zScan($this->activeKey(), $cursor, null, $count);

        if ($result === false || $result === []) {
            return ['cursor' => 0, 'items' => []];
        }

        return [
            'cursor' => $cursor,
            'items' => array_keys($result),
        ];
    }
}
