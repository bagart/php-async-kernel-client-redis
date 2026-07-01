<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Queue\Adapters;

use BAGArt\ASKClient\Contracts\Queue\ASKQueueAdapterContract;
use BAGArt\ASKClientRedis\Redis\Client\PhpRedisAdapter;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;
use BAGArt\ASKClientRedis\Redis\RedisDsn;

final class QueueRedisAdapter implements ASKQueueAdapterContract
{
    public const string TYPE = 'redis';

    private const string PARTITION_INDEX_SUFFIX = ':partitions';

    public function __construct(
        private readonly RedisClientContract $redis,
    ) {
    }

    public static function build(
        ?string $dsn = null,
    ): self {
        if (!$dsn) {
            throw new \InvalidArgumentException('DSN is required for QueueRedisAdapter');
        }

        return new self(
            new PhpRedisAdapter(
                RedisDsn::parse($dsn)
            ),
        );
    }

    public function push(string $queueName, string $payload): void
    {
        $this->redis->rPush($queueName, $payload);
    }

    public function pop(string $queueName): ?string
    {
        $payload = $this->redis->lPop($queueName);

        return $payload ?: null;
    }

    public function pushDelayed(
        string $queueName,
        string $payload,
        int $availableAt,
        ?string $partitionKey = null,
    ): void {
        $targetQueue = $queueName.':'.($partitionKey ?? 'default');
        $this->redis->zAdd($targetQueue, [], $availableAt, $payload);

        if ($partitionKey !== null) {
            $this->redis->sAdd($queueName.self::PARTITION_INDEX_SUFFIX, $partitionKey);
        }
    }

    public function popDue(string $queueName, ?string $partitionKey = null): ?string
    {
        $targetQueue = $queueName.':'.($partitionKey ?? 'default');

        $script = '
            local val = redis.call("zrangebyscore", KEYS[1], 0, ARGV[1], "LIMIT", 0, 1)
            if val[1] then
                redis.call("zrem", KEYS[1], val[1])
                return val[1]
            end
            return nil
        ';

        $result = $this->redis->eval($script, [$targetQueue, time()], 1);

        if ($result === false || $result === null) {
            if ($partitionKey !== null && $this->redis->zCard($targetQueue) === 0) {
                $this->redis->sRem($queueName.self::PARTITION_INDEX_SUFFIX, $partitionKey);
            }

            return null;
        }

        return (string) $result;
    }

    public function getPartitions(string $queueName, int $limit = 10, bool $random = true): array
    {
        $key = $queueName . self::PARTITION_INDEX_SUFFIX;

        if ($random) {
            return $this->redis->sRandMember($key, $limit) ?: [];
        }

        return $this->redis->sMembers($key) ?: [];
    }

    public function acquirePartition(string $partitionKey, int $ttl): bool
    {
        return (bool) $this->redis->set('lock:partition:'.$partitionKey, '1', ['nx', 'ex' => $ttl]);
    }

    public function releasePartition(string $partitionKey): void
    {
        $this->redis->del('lock:partition:'.$partitionKey);
    }

    public function size(string $queueName): int
    {
        $total = (int) $this->redis->lLen($queueName);
        $total += (int) $this->redis->zCard($queueName.':default');

        foreach ($this->getPartitions($queueName) as $partition) {
            $total += (int) $this->redis->zCard($queueName.':'.$partition);
        }

        return $total;
    }
}
