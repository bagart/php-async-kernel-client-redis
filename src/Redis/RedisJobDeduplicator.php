<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis;

use BAGArt\ASKClient\Contracts\Queue\JobDeduplicatorContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;

final class RedisJobDeduplicator implements JobDeduplicatorContract
{
    private const string SUFFIX_DEDUP = 'job:dedup:';

    private const string SUFFIX_PERMANENT = 'job:processed:';

    private const string SUFFIX_COMPOUND = 'job:dedup:compound:';

    public function __construct(
        private readonly RedisClientContract $redis,
        private readonly int $ttlSeconds = 86400,
        private readonly string $prefix = 'ASK:',
    ) {
    }

    private function dedupKey(string $jobId): string
    {
        return $this->prefix.self::SUFFIX_DEDUP.$jobId;
    }

    private function permanentKey(string $jobId): string
    {
        return $this->prefix.self::SUFFIX_PERMANENT.$jobId;
    }

    private function compoundKey(string $jobId, string $partitionKey, int $hash): string
    {
        return $this->prefix.self::SUFFIX_COMPOUND.$jobId.':'.$partitionKey.':'.$hash;
    }

    public function tryMark(string $jobId): bool
    {
        return (bool)$this->redis->set(
            $this->dedupKey($jobId),
            '1',
            ['NX', 'EX' => $this->ttlSeconds],
        );
    }

    public function tryMarkCompound(string $jobId, string $partitionKey, string $payload): bool
    {
        $hash = crc32($payload);

        $key = $this->compoundKey($jobId, $partitionKey, $hash);

        return (bool)$this->redis->set(
            $key,
            '1',
            ['NX', 'EX' => $this->ttlSeconds],
        );
    }

    public function markProcessedPermanent(string $jobId): void
    {
        $this->redis->set(
            $this->permanentKey($jobId),
            '1',
        );
    }

    public function isPermanentlyProcessed(string $jobId): bool
    {
        return (bool)$this->redis->exists($this->permanentKey($jobId));
    }

    public function isProcessed(string $jobId): bool
    {
        return (bool)$this->redis->exists($this->dedupKey($jobId))
            || (bool)$this->redis->exists($this->permanentKey($jobId));
    }

    public function forget(string $jobId): void
    {
        $this->redis->del($this->dedupKey($jobId));
        $this->redis->del($this->permanentKey($jobId));
    }
}
