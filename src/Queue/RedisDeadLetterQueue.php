<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Queue;

use BAGArt\ASKClient\Contracts\Queue\DeadLetterQueueContract;
use BAGArt\ASKClient\Contracts\Queue\JobSerializerContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;
use BAGArt\AsyncKernel\Job\AsyncJob;
use BAGArt\AsyncKernel\Wrappers\ASKLogWrapper;
use Throwable;

final class RedisDeadLetterQueue implements DeadLetterQueueContract
{
    private const string SUFFIX_JOB = 'dead_letter:job:';

    private const string SUFFIX_INDEX = 'dead_letter:index';

    private const string SUFFIX_HISTORY = 'dead_letter:history:';

    public function __construct(
        private readonly RedisClientContract $redis,
        private readonly JobSerializerContract $serializer,
        private readonly ?ASKLogWrapper $logger = null,
        private readonly int $ttlSeconds = 604800,
        private readonly string $prefix = 'ASK:',
    ) {
    }

    private function jobKey(string $jobId): string
    {
        return $this->prefix.self::SUFFIX_JOB.$jobId;
    }

    private function indexKey(): string
    {
        return $this->prefix.self::SUFFIX_INDEX;
    }

    private function historyKey(string $jobId): string
    {
        return $this->prefix.self::SUFFIX_HISTORY.$jobId;
    }

    public function push(AsyncJob $job, Throwable $error, array $history = []): void
    {
        $key = $this->jobKey($job->jobId);

        $payload = $this->serializer->serialize($job);

        $errorClass = $error::class;

        $category = $this->classifyFailure($error, $history);

        $data = [
            'jobId' => $job->jobId,
            'payload' => $payload,
            'error' => $error->getMessage(),
            'errorClass' => $errorClass,
            'category' => $category,
            'attempt' => (string)$job->attempt,
            'partition' => $job->partitionKey ?? '_global',
            'failedAt' => (string)time(),
        ];

        $this->redis->hMSet($key, $data);
        $this->redis->expire($key, $this->ttlSeconds);

        $this->redis->zAdd($this->indexKey(), time(), $job->jobId);

        $historyData = [
            'attempts' => json_encode($history['attempts'] ?? []),
            'partitions' => json_encode($history['partitions'] ?? []),
            'retryChain' => json_encode($history['retryChain'] ?? []),
            'workerIds' => json_encode($history['workerIds'] ?? []),
        ];

        $histKey = $this->historyKey($job->jobId);

        $this->redis->hMSet($histKey, $historyData);
        $this->redis->expire($histKey, $this->ttlSeconds);

        $this->logger?->error(
            '[DeadLetterQueue] job {jobId} moved to DLQ (category: {category}): {error}',
            [
                'jobId' => $job->jobId,
                'category' => $category,
                'error' => $error->getMessage(),
                'attempt' => $job->attempt,
            ],
        );
    }

    private function classifyFailure(Throwable $error, array $history): string
    {
        $message = $error->getMessage();
        $class = $error::class;

        if (str_contains($message, 'timed out') || str_contains($class, 'Timeout')) {
            return 'timeout';
        }

        if (str_contains($class, 'Poison') || str_contains($message, 'poison')) {
            return 'poison_message';
        }

        $maxAttempts = $history['maxAttempts'] ?? 5;
        $attempts = $history['attempts'] ?? [];

        if (count($attempts) >= $maxAttempts) {
            return 'retry_exhausted';
        }

        return 'exception';
    }

    public function get(string $jobId): ?array
    {
        $key = $this->jobKey($jobId);

        $data = $this->redis->hGetAll($key);

        if ($data === false || $data === []) {
            return null;
        }

        $history = $this->redis->hGetAll($this->historyKey($jobId));

        if ($history) {
            $data['history'] = [
                'attempts' => json_decode($history['attempts'] ?? '[]', true),
                'partitions' => json_decode($history['partitions'] ?? '[]', true),
                'retryChain' => json_decode($history['retryChain'] ?? '[]', true),
                'workerIds' => json_decode($history['workerIds'] ?? '[]', true),
            ];
        }

        return $data;
    }

    public function list(int $limit = 100): array
    {
        $ids = $this->redis->zRevRange($this->indexKey(), 0, $limit - 1);

        if ($ids === false) {
            return [];
        }

        return array_values($ids);
    }

    public function forget(string $jobId): void
    {
        $this->redis->del($this->jobKey($jobId));
        $this->redis->zRem($this->indexKey(), $jobId);
        $this->redis->del($this->historyKey($jobId));
    }

    public function restore(string $jobId): ?AsyncJob
    {
        $data = $this->get($jobId);

        if ($data === null) {
            return null;
        }

        try {
            return $this->serializer->deserialize($data['payload']);
        } catch (Throwable) {
            return null;
        }
    }
}
