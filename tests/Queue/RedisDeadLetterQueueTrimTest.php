<?php

declare(strict_types=1);

use BAGArt\AskQueue\Contracts\JobSerializerContract;
use BAGArt\ASKClientRedis\Queue\RedisDeadLetterQueue;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisPipelineContract;
use BAGArt\AsyncKernel\Job\AsyncJob;

class FakeRedisForDlqTrim implements RedisClientContract
{
    /** @var array<string, array<string, string>> */
    public array $hashes = [];

    /** @var array<string, array<int, array{score: int, member: string}>> */
    public array $sortedSets = [];

    /** @var array<int, array{key: string, start: int, end: int}> */
    public array $zRangeCalls = [];

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

    public function del(array|string $key, string ...$other_keys): int|false
    {
        $keys = is_array($key) ? $key : array_merge([$key], $other_keys);
        $count = 0;
        foreach ($keys as $k) {
            if (isset($this->hashes[$k])) {
                unset($this->hashes[$k]);
                $count++;
            }
            unset($this->sortedSets[$k]);
        }
        return $count;
    }

    public function exists(string $key, string ...$other_keys): int|false
    {
        return array_key_exists($key, $this->hashes) ? 1 : 0;
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
        return -2;
    }

    public function expire(string $key, int $seconds): RedisClientContract|bool
    {
        return true;
    }

    public function mget(array $keys): array|false
    {
        return [];
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
        $this->hashes[$key][$field] = (string)$value;
        return 1;
    }

    public function hDel(string $key, string $field, string ...$other_fields): int|false
    {
        return 0;
    }

    public function hMSet(string $key, array $keyValues): RedisClientContract|bool
    {
        $this->hashes[$key] = array_merge($this->hashes[$key] ?? [], $keyValues);
        return true;
    }

    public function hGetAll(string $key): array|false
    {
        return $this->hashes[$key] ?? [];
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

    public function zAdd(string $key, array $options, float $score, string $member, mixed ...$more): int|false
    {
        $this->sortedSets[$key][] = ['score' => (int)$score, 'member' => $member];
        return 1;
    }

    public function zRem(string $key, mixed ...$member): int|false
    {
        if (!isset($this->sortedSets[$key])) {
            return 0;
        }
        $removed = 0;
        foreach ($member as $m) {
            foreach ($this->sortedSets[$key] as $i => $entry) {
                if ($entry['member'] === $m) {
                    unset($this->sortedSets[$key][$i]);
                    $removed++;
                }
            }
        }
        return $removed;
    }

    public function zCard(string $key): int|false
    {
        return count($this->sortedSets[$key] ?? []);
    }

    public function zRangeByScore(string $key, string $start, string $end, array $options = []): array|false
    {
        return [];
    }

    public function zRange(string $key, int $start, int $end, ?array $options = null): array|false
    {
        $this->zRangeCalls[] = ['key' => $key, 'start' => $start, 'end' => $end];

        $set = array_values($this->sortedSets[$key] ?? []);

        // Scores are inserted in monotonic time() order in these tests — only
        // pay for a sort when a member really is out of order.
        $prevScore = 0;
        foreach ($set as $entry) {
            if ($entry['score'] < $prevScore) {
                usort($set, fn ($a, $b) => $a['score'] <=> $b['score']);
                break;
            }
            $prevScore = $entry['score'];
        }

        $members = array_column($set, 'member');

        return array_slice($members, $start, $end - $start + 1);
    }

    public function zRevRange(string $key, int $start, int $end, ?array $options = null): array|false
    {
        $set = $this->sortedSets[$key] ?? [];
        usort($set, fn ($a, $b) => $b['score'] <=> $a['score']);
        $members = array_column($set, 'member');

        return array_slice($members, $start, $end - $start + 1);
    }

    public function zScore(string $key, string $member): float|false
    {
        return false;
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
        return '*0-0';
    }
    public function xRead(array $streams, int $count = -1, int $block = 0): array|false
    {
        return [];
    }
    public function xDel(string $key, string ...$ids): int|false
    {
        return 0;
    }
    public function trim(string $partitionKey, int $maxLen): void
    {
    }
    public function xTrim(string $key, array $options): int|false
    {
        return 0;
    }
    public function xLen(string $key): int|false
    {
        return 0;
    }
    public function scan(?int &$iterator, ?string $pattern = null, int $count = 0): array|false
    {
        return [];
    }
    public function pipeline(): RedisPipelineContract
    {
        throw new \LogicException('Not implemented');
    }
    public function eval(string $script, array $args = [], int $numKeys = 0): mixed
    {
        return null;
    }
}

class FakeJobSerializerForTrim implements JobSerializerContract
{
    public function serialize(\BAGArt\AsyncKernel\Job\AsyncJob $job): string
    {
        return '{}';
    }

    public function deserialize(string $payload): \BAGArt\AsyncKernel\Job\AsyncJob
    {
        return new AsyncJob(jobId: 'x', partitionKey: null, processor: 't', executionKey: null, createdAt: 0);
    }

    public function serializeToMeta(\BAGArt\AsyncKernel\Job\AsyncJob $job): string
    {
        return '';
    }
    public function deserializeFromMeta(string $jobId, array $meta): ?\BAGArt\AsyncKernel\Job\AsyncJob
    {
        return null;
    }
    public function serializeToRecoveryPayload(\BAGArt\AsyncKernel\Job\AsyncJob $job, string $fencingToken = ''): string
    {
        return '';
    }
}

describe('RedisDeadLetterQueue index trimming', function () {
    it('trims index when it exceeds MAX_INDEX_SIZE', function () {
        $fake = new FakeRedisForDlqTrim();
        $serializer = new FakeJobSerializerForTrim();
        $dlq = new RedisDeadLetterQueue($fake, $serializer, prefix: 'TEST:');

        for ($i = 1; $i <= 10_001; $i++) {
            $job = new AsyncJob(
                jobId: "job-{$i}",
                partitionKey: null,
                processor: 'handler',
                executionKey: null,
                createdAt: time(),
            );
            $dlq->push($job, new RuntimeException('fail'));
        }

        $indexCount = count($fake->sortedSets['TEST:dead_letter:index'] ?? []);
        expect($indexCount)->toBeLessThanOrEqual(10_000);
    });

    it('does NOT trim when index is at or below MAX_INDEX_SIZE', function () {
        $fake = new FakeRedisForDlqTrim();
        $serializer = new FakeJobSerializerForTrim();
        $dlq = new RedisDeadLetterQueue($fake, $serializer, prefix: 'TEST:');

        for ($i = 1; $i <= 5; $i++) {
            $job = new AsyncJob(
                jobId: "job-{$i}",
                partitionKey: null,
                processor: 'handler',
                executionKey: null,
                createdAt: time(),
            );
            $dlq->push($job, new RuntimeException('fail'));
        }

        $indexCount = count($fake->sortedSets['TEST:dead_letter:index'] ?? []);

        // push() runs one bounded stale-entry sweep scan (0..49); trimIndex must
        // not touch the index while it is below MAX_INDEX_SIZE.
        expect($indexCount)->toBe(5)
            ->and($fake->zRangeCalls)->toHaveCount(5)
            ->and(array_unique(array_column($fake->zRangeCalls, 'end')))->toBe([49]);
    });

    it('trims oldest entries when over limit', function () {
        $fake = new FakeRedisForDlqTrim();
        $serializer = new FakeJobSerializerForTrim();
        $dlq = new RedisDeadLetterQueue($fake, $serializer, prefix: 'TEST:');

        for ($i = 1; $i <= 10_002; $i++) {
            $job = new AsyncJob(
                jobId: "job-{$i}",
                partitionKey: null,
                processor: 'handler',
                executionKey: null,
                createdAt: time(),
            );
            $dlq->push($job, new RuntimeException('fail'));
        }

        $members = array_column($fake->sortedSets['TEST:dead_letter:index'] ?? [], 'member');
        expect($members)->not->toContain('job-1')
            ->and($members)->toContain('job-10002');
    });
});
