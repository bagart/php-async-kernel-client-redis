<?php

declare(strict_types=1);

use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisPipelineContract;
use BAGArt\ASKClientRedis\Redis\RedisPendingAckRegistry;

class FakeRedisForPendingAck implements RedisClientContract
{
    /** @var array<string, string> */
    public array $strings = [];

    /** @var array<string, array<string, string>> */
    public array $hashes = [];

    /** @var array<string, array<int, array{score: int, member: string}>> */
    public array $sortedSets = [];

    /** @var array<string, array<string, bool>> */
    public array $sets = [];

    /** @var int */
    public int $evalCount = 0;

    public function get(string $key): string|false
    {
        return $this->strings[$key] ?? false;
    }

    public function set(string $key, mixed $value, mixed ...$options): RedisClientContract|bool
    {
        $this->strings[$key] = (string)$value;
        return true;
    }

    public function setex(string $key, int $seconds, string $value): RedisClientContract|bool
    {
        $this->strings[$key] = $value;
        return true;
    }

    public function del(array|string $key, string ...$other_keys): int|false
    {
        $keys = is_array($key) ? $key : array_merge([$key], $other_keys);
        $count = 0;
        foreach ($keys as $k) {
            if (isset($this->strings[$k])) {
                unset($this->strings[$k]);
                $count++;
            }
            unset($this->hashes[$k], $this->sortedSets[$k], $this->sets[$k]);
        }
        return $count;
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
        return $this->hashes[$key][$field] ?? false;
    }

    public function hSet(string $key, string $field, mixed $value): int|false
    {
        $this->hashes[$key][$field] = (string)$value;
        return 1;
    }

    public function hDel(string $key, string $field, string ...$other_fields): int|false
    {
        $count = 0;
        foreach (array_merge([$field], $other_fields) as $f) {
            if (isset($this->hashes[$key][$f])) {
                unset($this->hashes[$key][$f]);
                $count++;
            }
        }
        return $count;
    }

    public function hMSet(string $key, array $keyValues): RedisClientContract|bool
    {
        $this->hashes[$key] = array_merge($this->hashes[$key] ?? [], $keyValues);
        return true;
    }

    public function hGetAll(string $key): array|false
    {
        return $this->hashes[$key] ?? false;
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
        foreach ($values as $v) {
            $this->sets[$key][(string)$v] = true;
        }
        return count($values);
    }

    public function sRem(string $key, mixed ...$values): int|false
    {
        $count = 0;
        foreach ($values as $v) {
            if (isset($this->sets[$key][(string)$v])) {
                unset($this->sets[$key][(string)$v]);
                $count++;
            }
        }
        return $count;
    }

    public function sMembers(string $key): array|false
    {
        return array_keys($this->sets[$key] ?? []);
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
        return [];
    }
    public function zRevRange(string $key, int $start, int $end, ?array $options = null): array|false
    {
        return [];
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
        $this->evalCount++;

        if (str_contains($script, 'ZADD')) {
            $pendingKey = $args[0] ?? '';
            $entryKey = $args[1] ?? '';
            $ownershipKey = $args[2] ?? '';
            $jobId = $args[3] ?? '';
            $entryId = $args[4] ?? '';
            $now = $args[5] ?? '';
            $workerId = $args[6] ?? '';
            $fencingToken = $args[7] ?? '';

            $this->sortedSets[$pendingKey][] = ['score' => (int)$now, 'member' => $jobId];
            $this->hashes[$entryKey][$jobId] = $entryId;

            if ($workerId !== '' && $fencingToken !== '') {
                $this->hashes[$ownershipKey][$jobId . ':workerId'] = $workerId;
                $this->hashes[$ownershipKey][$jobId . ':fencingToken'] = $fencingToken;
            }

            return 1;
        }

        return 0;
    }
}

describe('RedisPendingAckRegistry atomic add', function () {
    it('add uses a single Lua eval call', function () {
        $fake = new FakeRedisForPendingAck();
        $registry = new RedisPendingAckRegistry($fake, prefix: 'TEST:');

        $registry->add('partition-A', 'job-1', 'entry-001', 'worker-1', 'token-abc');

        expect($fake->evalCount)->toBe(1);
    });

    it('add stores pending, entry, ownership, and partitions atomically', function () {
        $fake = new FakeRedisForPendingAck();
        $registry = new RedisPendingAckRegistry($fake, prefix: 'TEST:');

        $registry->add('partition-A', 'job-1', 'entry-001', 'worker-1', 'token-abc');

        $pendingMembers = array_column($fake->sortedSets['TEST:{partition-A}stream:pending:partition-A'] ?? [], 'member');
        expect($pendingMembers)->toContain('job-1');

        expect($fake->hashes['TEST:{partition-A}stream:entry:partition-A']['job-1'])->toBe('entry-001')
            ->and($fake->hashes['TEST:{partition-A}stream:ownership:partition-A']['job-1:workerId'])->toBe('worker-1')
            ->and($fake->hashes['TEST:{partition-A}stream:ownership:partition-A']['job-1:fencingToken'])->toBe('token-abc');

        expect($fake->sets['TEST:stream:pending:partitions'])->toHaveKey('partition-A');
    });

    it('add without ownership still stores pending and entry', function () {
        $fake = new FakeRedisForPendingAck();
        $registry = new RedisPendingAckRegistry($fake, prefix: 'TEST:');

        $registry->add('partition-B', 'job-2', 'entry-002');

        $pendingMembers = array_column($fake->sortedSets['TEST:{partition-B}stream:pending:partition-B'] ?? [], 'member');
        expect($pendingMembers)->toContain('job-2')
            ->and($fake->hashes['TEST:{partition-B}stream:entry:partition-B']['job-2'])->toBe('entry-002');
    });
});
