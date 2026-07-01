<?php

declare(strict_types=1);

use BAGArt\ASKClient\Contracts\Queue\DeadLetterQueueContract;
use BAGArt\ASKClient\Contracts\Queue\JobSerializerContract;
use BAGArt\ASKClientRedis\Queue\RedisDeadLetterQueue;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;
use BAGArt\AsyncKernel\Job\AsyncJob;

/**
 * Hand-rolled fake {@see RedisClientContract} for RedisDeadLetterQueue tests.
 */
class FakeRedisClientForDlq implements RedisClientContract
{
    /** @var array<string, array<string, string>> Hash storage (keyed by hash key). */
    public array $hashes = [];

    /** @var array<string, array{score: int, member: string}> Sorted set storage (keyed by set key). */
    public array $sortedSets = [];

    /** @var list<string> Recorded method calls. */
    public array $calls = [];

    public function hMSet(string $key, array $keyValues): RedisClientContract|bool
    {
        $this->calls[] = "hMSet:{$key}";
        $this->hashes[$key] = $keyValues + ($this->hashes[$key] ?? []);

        return true;
    }

    public function expire(string $key, int $seconds): RedisClientContract|bool
    {
        $this->calls[] = "expire:{$key}:{$seconds}";

        return true;
    }

    public function zAdd(string $key, array $options, float $score, string $member, mixed ...$more): int|false
    {
        $this->calls[] = "zAdd:{$key}";
        $this->sortedSets[$key][$member] = ['score' => (int) $score, 'member' => $member];

        return 1;
    }

    public function hGetAll(string $key): array|false
    {
        $this->calls[] = "hGetAll:{$key}";

        return $this->hashes[$key] ?? [];
    }

    public function zRevRange(string $key, int $start, int $end, ?array $options = null): array|false
    {
        $this->calls[] = "zRevRange:{$key}";

        if (!isset($this->sortedSets[$key])) {
            return [];
        }

        $members = array_column($this->sortedSets[$key], 'member');
        usort($members, fn (string $a, string $b) => $this->sortedSets[$key][$b]['score'] <=> $this->sortedSets[$key][$a]['score']);

        return array_slice($members, $start, $end - $start + 1);
    }

    public function del(array|string $key, string ...$other_keys): int|false
    {
        $keys = is_array($key) ? $key : [$key, ...$other_keys];
        $count = 0;
        foreach ($keys as $k) {
            unset($this->hashes[$k]);
            unset($this->sortedSets[$k]);
            $count++;
        }

        return $count;
    }

    public function zRem(string $key, mixed ...$member): int|false
    {
        $this->calls[] = "zRem:{$key}";
        foreach ($member as $m) {
            unset($this->sortedSets[$key][$m]);
        }

        return count($member);
    }

    public function get(string $key): string|false
    {
        return false;
    }
    public function set(string $key, mixed $value, mixed ...$options): RedisClientContract|bool
    {
        return true;
    }
    public function setex(string $key, int $seconds, string $value): RedisClientContract|bool
    {
        return true;
    }
    public function exists(string $key, string ...$other_keys): int|false
    {
        return 0;
    }
    public function incrBy(string $key, int $value): int|false
    {
        return 0;
    }
    public function decrBy(string $key, int $value): int|false
    {
        return 0;
    }
    public function ttl(string $key): int|false
    {
        return -1;
    }
    public function mget(array $keys): array|false
    {
        return array_fill(0, count($keys), false);
    }
    public function mset(array $keyValues): RedisClientContract|bool
    {
        return true;
    }
    public function flushDB(): bool
    {
        return true;
    }
    public function lPop(string $key): string|false
    {
        return false;
    }
    public function rPush(string $key, mixed ...$values): int|false
    {
        return 0;
    }
    public function lLen(string $key): int|false
    {
        return 0;
    }
    public function lPush(string $key, mixed ...$values): int|false
    {
        return 0;
    }
    public function lIndex(string $key, int $index): string|false
    {
        return false;
    }
    public function hGet(string $key, string $field): string|false
    {
        return false;
    }
    public function hSet(string $key, string $field, mixed $value): int|false
    {
        return 0;
    }
    public function hDel(string $key, string $field, string ...$other_fields): int|false
    {
        return 0;
    }
    public function hSetNx(string $key, string $field, mixed $value): int|false
    {
        return 0;
    }
    public function hIncrBy(string $key, string $field, int $value): int|false
    {
        return 0;
    }
    public function hLen(string $key): int|false
    {
        return 0;
    }
    public function hScan(string $key, ?int &$iterator, ?string $pattern = null, int $count = 10): array|false
    {
        return [];
    }
    public function sAdd(string $key, mixed ...$values): int|false
    {
        return 0;
    }
    public function sRem(string $key, mixed ...$values): int|false
    {
        return 0;
    }
    public function sMembers(string $key): array|false
    {
        return [];
    }
    public function sRandMember(string $key, int $count = 1): string|array|false
    {
        return false;
    }
    public function zCard(string $key): int|false
    {
        return 0;
    }
    public function zRangeByScore(string $key, string $start, string $end, array $options = []): array|false
    {
        return [];
    }
    public function zRange(string $key, int $start, int $end, ?array $options = null): array|false
    {
        return [];
    }
    public function zScore(string $key, string $member): float|false
    {
        return 0.0;
    }
    public function zScan(string $key, ?int &$iterator, ?string $pattern = null, int $count = 10): array|false
    {
        return [];
    }
    public function zPopMax(string $key, int $count = 1): array|false
    {
        return [];
    }
    public function xAdd(string $key, string $id, array $fields, int $maxlen = 0, bool $approx = false): string|false
    {
        return '*';
    }
    public function xRead(array $streams, int $count = -1, int $block = 0): array|false
    {
        return [];
    }
    public function xDel(string $key, string ...$ids): int|false
    {
        return 0;
    }
    public function xTrim(string $key, array $options): int|false
    {
        return 0;
    }
    public function xLen(string $key): int|false
    {
        return 0;
    }
    public function eval(string $script, array $args = [], int $numKeys = 0): mixed
    {
        return null;
    }
    public function scan(?int &$iterator, ?string $pattern = null, int $count = 0): array|false
    {
        return [];
    }
    public function pipeline(): \BAGArt\ASKClientRedis\Redis\Contract\RedisPipelineContract
    {
        throw new \LogicException('Not implemented in fake');
    }

    public function trim(string $partitionKey, int $maxLen): void
    {
    }
}

/**
 * Hand-rolled fake serializer.
 */
class FakeJobSerializer implements JobSerializerContract
{
    /** @var string Serialized payload to return. */
    public string $serializedPayload = '{"job":"data"}';

    public function serialize(AsyncJob $job): string
    {
        return $this->serializedPayload;
    }

    public function deserialize(string $payload): AsyncJob
    {
        return new AsyncJob(
            jobId: 'restored-job',
            partitionKey: null,
            processor: 'test',
            executionKey: null,
            createdAt: time(),
        );
    }

    public function serializeToMeta(AsyncJob $job): string
    {
        return '';
    }

    public function deserializeFromMeta(string $jobId, array $meta): ?AsyncJob
    {
        return null;
    }

    public function serializeToRecoveryPayload(AsyncJob $job, string $fencingToken = ''): string
    {
        return '';
    }
}

describe('RedisDeadLetterQueue', function () {
    beforeEach(function () {
        $this->fake = new FakeRedisClientForDlq();
        $this->serializer = new FakeJobSerializer();
        $this->dlq = new RedisDeadLetterQueue($this->fake, $this->serializer);
    });

    it('implements DeadLetterQueueContract', function () {
        expect(RedisDeadLetterQueue::class)
            ->toImplement(DeadLetterQueueContract::class);
    });

    it('pushes a job to DLQ and stores job data in hash', function () {
        $job = new AsyncJob(
            jobId: 'job-1',
            partitionKey: 'chat:1',
            processor: 'handler',
            executionKey: null,
            createdAt: time(),
            attempt: 3,
        );

        $this->dlq->push($job, new RuntimeException('Something went wrong'));

        $stored = $this->fake->hashes['ASK:dead_letter:job:job-1'] ?? [];

        expect($stored)->not->toBeEmpty()
            ->and($stored['jobId'])->toBe('job-1')
            ->and($stored['error'])->toBe('Something went wrong')
            ->and($stored['errorClass'])->toBe(RuntimeException::class)
            ->and($stored['partition'])->toBe('chat:1');
    });

    it('adds job ID to the sorted set index', function () {
        $job = new AsyncJob(
            jobId: 'job-2',
            partitionKey: null,
            processor: 'handler',
            executionKey: null,
            createdAt: time(),
        );

        $this->dlq->push($job, new RuntimeException('fail'));

        expect($this->fake->sortedSets['ASK:dead_letter:index'])
            ->toHaveKey('job-2');
    });

    it('classifies timeout errors correctly', function () {
        $job = new AsyncJob(
            jobId: 'job-t',
            partitionKey: null,
            processor: 'handler',
            executionKey: null,
            createdAt: time(),
        );

        $this->dlq->push($job, new TimeoutException('Connection timed out'));

        $stored = $this->fake->hashes['ASK:dead_letter:job:job-t'] ?? [];

        expect($stored['category'])->toBe('timeout');
    });

    it('classifies poison messages correctly', function () {
        $job = new AsyncJob(
            jobId: 'job-p',
            partitionKey: null,
            processor: 'handler',
            executionKey: null,
            createdAt: time(),
        );

        $this->dlq->push($job, new RuntimeException('poison pill detected'));

        $stored = $this->fake->hashes['ASK:dead_letter:job:job-p'] ?? [];

        expect($stored['category'])->toBe('poison_message');
    });

    it('classifies retry-exhausted correctly', function () {
        $job = new AsyncJob(
            jobId: 'job-r',
            partitionKey: null,
            processor: 'handler',
            executionKey: null,
            createdAt: time(),
            attempt: 5,
        );

        $this->dlq->push($job, new RuntimeException('fail'), [
            'attempts' => [1, 2, 3, 4, 5],
            'maxAttempts' => 5,
        ]);

        $stored = $this->fake->hashes['ASK:dead_letter:job:job-r'] ?? [];

        expect($stored['category'])->toBe('retry_exhausted');
    });

    it('classifies generic exception correctly', function () {
        $job = new AsyncJob(
            jobId: 'job-e',
            partitionKey: null,
            processor: 'handler',
            executionKey: null,
            createdAt: time(),
            attempt: 1,
        );

        $this->dlq->push($job, new RuntimeException('generic fail'), [
            'attempts' => [1],
            'maxAttempts' => 5,
        ]);

        $stored = $this->fake->hashes['ASK:dead_letter:job:job-e'] ?? [];

        expect($stored['category'])->toBe('exception');
    });

    it('retrieves a job by ID', function () {
        $job = new AsyncJob(
            jobId: 'job-get',
            partitionKey: 'chat:1',
            processor: 'handler',
            executionKey: null,
            createdAt: time(),
            attempt: 2,
        );

        $this->dlq->push($job, new RuntimeException('fail'));

        $data = $this->dlq->get('job-get');

        expect($data)->not->toBeNull()
            ->and($data['jobId'])->toBe('job-get')
            ->and($data['partition'])->toBe('chat:1');
    });

    it('returns null for non-existent job', function () {
        expect($this->dlq->get('non-existent'))->toBeNull();
    });

    it('lists job IDs from the index', function () {
        $jobs = [];
        for ($i = 1; $i <= 3; $i++) {
            $jobs[] = new AsyncJob(
                jobId: "job-{$i}",
                partitionKey: null,
                processor: 'handler',
                executionKey: null,
                createdAt: time(),
            );
        }

        foreach ($jobs as $job) {
            $this->dlq->push($job, new RuntimeException('fail'));
        }

        $list = $this->dlq->list();

        expect($list)->toHaveCount(3)
            ->and($list)->toContain('job-1', 'job-2', 'job-3');
    });

    it('forgets a job by removing all associated keys', function () {
        $job = new AsyncJob(
            jobId: 'job-del',
            partitionKey: null,
            processor: 'handler',
            executionKey: null,
            createdAt: time(),
        );

        $this->dlq->push($job, new RuntimeException('fail'));
        $this->dlq->forget('job-del');

        expect($this->fake->hashes)->not->toHaveKey('ASK:dead_letter:job:job-del')
            ->and($this->fake->sortedSets['ASK:dead_letter:index'])->not->toHaveKey('job-del');
    });

    it('restores a job from the DLQ', function () {
        $job = new AsyncJob(
            jobId: 'job-restore',
            partitionKey: null,
            processor: 'handler',
            executionKey: null,
            createdAt: time(),
        );

        $this->dlq->push($job, new RuntimeException('fail'));

        $restored = $this->dlq->restore('job-restore');

        expect($restored)->toBeInstanceOf(AsyncJob::class)
            ->and($restored->jobId)->toBe('restored-job');
    });

    it('returns null when restoring non-existent job', function () {
        expect($this->dlq->restore('non-existent'))->toBeNull();
    });
});
