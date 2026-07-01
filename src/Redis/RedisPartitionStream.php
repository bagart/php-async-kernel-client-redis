<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis;

use BAGArt\ASKClient\Contracts\Queue\JobSerializerContract;
use BAGArt\ASKClient\Contracts\Queue\PartitionStreamContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;
use BAGArt\AsyncKernel\Job\AsyncJob;

final class RedisPartitionStream implements PartitionStreamContract
{
    private const string SUFFIX_PARTITION = 'partition:';

    private const string SUFFIX_OWNERSHIP = 'stream:ownership:';

    public function __construct(
        private readonly RedisClientContract $redis,
        private readonly JobSerializerContract $serializer,
        private readonly string $prefix = 'ASK:',
    ) {
    }

    private function streamKey(string $partitionKey): string
    {
        return $this->prefix.self::SUFFIX_PARTITION.$partitionKey;
    }

    private function ownershipKey(string $partitionKey, string $entryId): string
    {
        return $this->prefix.self::SUFFIX_OWNERSHIP.$partitionKey.':'.$entryId;
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
        if ($workerId !== null) {
            if (!$this->verifyOwnership($partitionKey, $entryId, $workerId, '')) {
                return;
            }
        }

        $this->redis->xDel($this->streamKey($partitionKey), $entryId);

        $key = $this->ownershipKey($partitionKey, $entryId);
        $this->redis->del($key);
    }

    public function claimOwnership(string $partitionKey, string $entryId, string $workerId, string $fencingToken): bool
    {
        $key = $this->ownershipKey($partitionKey, $entryId);

        return (bool)$this->redis->hSetNx($key, 'workerId', $workerId)
            && $this->redis->hSetNx($key, 'fencingToken', $fencingToken);
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
