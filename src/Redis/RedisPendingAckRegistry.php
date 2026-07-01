<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis;

use BAGArt\ASKClient\Contracts\Queue\PendingAckRegistryContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;

final class RedisPendingAckRegistry implements PendingAckRegistryContract
{
    private const string SUFFIX_PENDING = 'stream:pending:';

    private const string SUFFIX_ENTRY = 'stream:entry:';

    private const string SUFFIX_OWNERSHIP = 'stream:ownership:';

    private const string SUFFIX_PARTITIONS = 'stream:pending:partitions';

    public function __construct(
        private readonly RedisClientContract $redis,
        private readonly string $prefix = 'ASK:',
    ) {
    }

    private function pendingKey(string $partitionKey): string
    {
        return $this->prefix.self::SUFFIX_PENDING.$partitionKey;
    }

    private function entryKey(string $partitionKey): string
    {
        return $this->prefix.self::SUFFIX_ENTRY.$partitionKey;
    }

    private function ownershipKey(string $partitionKey): string
    {
        return $this->prefix.self::SUFFIX_OWNERSHIP.$partitionKey;
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
        $this->redis->zAdd($this->pendingKey($partitionKey), time(), $jobId);
        $this->redis->hSet($this->entryKey($partitionKey), $jobId, $entryId);

        if ($workerId !== '' && $fencingToken !== '') {
            $key = $this->ownershipKey($partitionKey);

            $this->redis->hSet($key, $jobId.':workerId', $workerId);
            $this->redis->hSet($key, $jobId.':fencingToken', $fencingToken);
        }

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
        if ($this->redis->zCard($this->pendingKey($partitionKey)) === 0) {
            $this->redis->del($this->pendingKey($partitionKey));
            $this->redis->del($this->entryKey($partitionKey));
            $this->redis->del($this->ownershipKey($partitionKey));
            $this->redis->sRem($this->partitionsKey(), $partitionKey);
        }
    }
}
