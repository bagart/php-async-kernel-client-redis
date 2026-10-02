<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis;

use BAGArt\AskQueue\Contracts\JobSerializerContract;
use BAGArt\AskQueue\Contracts\PartitionStreamContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;
use BAGArt\AsyncKernel\Job\AsyncJob;

final class RedisPartitionStream implements PartitionStreamContract
{
    private const string SUFFIX_PARTITION = 'partition:';

    private const string SUFFIX_OWNERSHIP = 'stream:ownership:';

    private const string LUA_ACK = <<<'LUA'
local streamKey = KEYS[1]
local ownershipKey = KEYS[2]
local entryId = ARGV[1]
local workerId = ARGV[2]

if workerId ~= '' then
    local storedWorker = redis.call('HGET', ownershipKey, 'workerId')
    if storedWorker ~= workerId then
        return 0
    end
end

redis.call('XDEL', streamKey, entryId)
redis.call('EXPIRE', ownershipKey, 1)

return 1
LUA;

    private const int OWNERSHIP_TTL_SECONDS = 86400;

    public function __construct(
        private readonly RedisClientContract $redis,
        private readonly JobSerializerContract $serializer,
        private readonly string $prefix = 'ASK:',
    ) {
    }

    private function streamKey(string $partitionKey): string
    {
        return $this->prefix.'{'.$partitionKey.'}'.self::SUFFIX_PARTITION.$partitionKey;
    }

    private function ownershipKey(string $partitionKey, string $entryId): string
    {
        return $this->prefix.'{'.$partitionKey.'}'.self::SUFFIX_OWNERSHIP.$partitionKey.':'.$entryId;
    }

    public function push(string $partitionKey, AsyncJob $job): string
    {
        $key = $this->streamKey($partitionKey);

        return $this->redis->xAdd(
            $key,
            '*',
            [
                'jobId' => $job->jobId,
                'payload' => $this->serializer->serialize($job),
                'attempt' => $job->attempt,
                'createdAt' => $job->createdAt,
            ],
        );
    }

    public function read(string $partitionKey, string $lastId, int $count = 1): array
    {
        $key = $this->streamKey($partitionKey);

        $raw = $this->redis->xRead(
            [$key => $lastId],
            $count,
        );

        if ($raw === false || $raw === null) {
            return [];
        }

        $jobs = [];
        foreach ($raw[$key] ?? [] as $entryId => $data) {
            $payload = $data['payload'] ?? null;
            if ($payload !== null) {
                try {
                    $job = $this->serializer->deserialize($payload);
                    $jobs[$entryId] = $job;
                } catch (\Throwable) {
                    continue;
                }
            }
        }

        return $jobs;
    }

    public function ack(string $partitionKey, string $entryId, ?string $workerId = null): void
    {
        $this->redis->eval(
            self::LUA_ACK,
            [
                $this->streamKey($partitionKey),
                $this->ownershipKey($partitionKey, $entryId),
                $entryId,
                $workerId ?? '',
            ],
            2,
        );
    }

    public function claimOwnership(string $partitionKey, string $entryId, string $workerId, string $fencingToken): bool
    {
        $key = $this->ownershipKey($partitionKey, $entryId);

        $result = (bool)$this->redis->hSetNx($key, 'workerId', $workerId)
            && $this->redis->hSetNx($key, 'fencingToken', $fencingToken);

        if ($result) {
            $this->redis->expire($key, self::OWNERSHIP_TTL_SECONDS);
        }

        return $result;
    }

    public function verifyOwnership(string $partitionKey, string $entryId, string $workerId, string $fencingToken): bool
    {
        $key = $this->ownershipKey($partitionKey, $entryId);

        $storedWorker = $this->redis->hGet($key, 'workerId');
        $storedToken = $this->redis->hGet($key, 'fencingToken');

        return $storedWorker === $workerId && ($fencingToken === '' || $storedToken === $fencingToken);
    }

    public function trim(string $partitionKey, int $maxLen): void
    {
        $this->redis->trim($this->streamKey($partitionKey), $maxLen);
    }

    public function length(string $partitionKey): int
    {
        return (int)$this->redis->xLen($this->streamKey($partitionKey));
    }
}
