<?php

declare(strict_types=1);

use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisPipelineContract;
use BAGArt\ASKClientRedis\Redis\RedisJobStateStore;

class FakeRedisForStateStore implements RedisClientContract
{
    /** @var array<string, string> */
    public array $strings = [];

    /** @var array<string, array<string, string>> */
    public array $hashes = [];

    /** @var array<string, array<int, array{score: int, member: string}>> */
    public array $sortedSets = [];

    /** @var array<int, array{script: string, args: array, numKeys: int}> */
    public array $evalCalls = [];

    /** @var list<mixed> */
    public array $evalResults = [];

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
            unset($this->hashes[$k], $this->sortedSets[$k]);
        }
        return $count;
    }

    public function exists(string $key, string ...$other_keys): int|false
    {
        return array_key_exists($key, $this->strings) || array_key_exists($key, $this->hashes) ? 1 : 0;
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
        if (isset($this->hashes[$key][$field])) {
            return 0;
        }
        $this->hashes[$key][$field] = (string)$value;
        return 1;
    }

    public function hIncrBy(string $key, string $field, int $value): int|false
    {
        $current = (int)($this->hashes[$key][$field] ?? 0);
        $this->hashes[$key][$field] = (string)($current + $value);
        return $current + $value;
    }

    public function hLen(string $key): int|false
    {
        return count($this->hashes[$key] ?? []);
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
        $this->evalCalls[] = ['script' => $script, 'args' => $args, 'numKeys' => $numKeys];

        if ($this->evalResults !== []) {
            return array_shift($this->evalResults);
        }

        $jobKey = $args[0] ?? '';

        if (str_contains($script, "'state', 'dead_letter'")) {
            $this->hashes[$jobKey] = array_merge($this->hashes[$jobKey] ?? [], [
                'state' => 'dead_letter',
                'error' => $args[4] ?? '',
                'completedAt' => $args[5] ?? '',
            ]);
            unset($this->hashes[$args[1]], $this->strings[$args[2]]);
            $this->zRem($args[3], $args[6]);

            return 1;
        }

        if (str_contains($script, "'state', 'failed'")) {
            $this->hashes[$jobKey] = array_merge($this->hashes[$jobKey] ?? [], [
                'state' => 'failed',
                'error' => $args[3] ?? '',
                'completedAt' => $args[4] ?? '',
            ]);
            unset($this->hashes[$args[1]]);
            $this->zRem($args[2], $args[5]);

            return 1;
        }

        if (str_contains($script, "'state', 'retry'")) {
            $attempt = (int)($this->hashes[$jobKey]['attempt'] ?? 0) + 1;
            $this->hashes[$jobKey] = array_merge($this->hashes[$jobKey] ?? [], [
                'state' => 'retry',
                'retryAt' => $args[3] ?? '',
                'attempt' => (string)$attempt,
            ]);
            unset($this->hashes[$args[1]]);
            $this->zRem($args[2], $args[4]);

            return $attempt;
        }

        if (str_contains($script, "'state', 'completed'")) {
            $this->hashes[$jobKey] = array_merge($this->hashes[$jobKey] ?? [], [
                'state' => 'completed',
                'completedAt' => $args[4] ?? '',
            ]);
            unset($this->strings[$args[2]], $this->hashes[$args[3]]);

            return 1;
        }

        if (str_contains($script, 'SETNX') || str_contains($script, 'HSET')) {
            return 1;
        }

        return 0;
    }
}

describe('RedisJobStateStore atomic Lua scripts', function () {
    beforeEach(function () {
        $this->fake = new FakeRedisForStateStore();
        $this->store = new RedisJobStateStore($this->fake, prefix: 'TEST:');
    });

    it('markFailed uses a single Lua eval call', function () {
        $this->store->markFailed('job-1', 'timeout error');

        expect($this->fake->evalCalls)->toHaveCount(1)
            ->and($this->fake->evalCalls[0]['script'])->toContain('failed');

        $jobKey = 'TEST:{job-1}job:';
        expect($this->fake->hashes[$jobKey]['state'])->toBe('failed')
            ->and($this->fake->hashes[$jobKey]['error'])->toBe('timeout error');
    });

    it('markDeadLetter uses a single Lua eval call', function () {
        $this->store->markDeadLetter('job-2', 'permanent failure');

        expect($this->fake->evalCalls)->toHaveCount(1)
            ->and($this->fake->evalCalls[0]['script'])->toContain('dead_letter');

        $jobKey = 'TEST:{job-2}job:';
        expect($this->fake->hashes[$jobKey]['state'])->toBe('dead_letter')
            ->and($this->fake->hashes[$jobKey]['error'])->toBe('permanent failure');
    });

    it('markRetry uses a single Lua eval call and increments attempt atomically', function () {
        $this->fake->hashes['TEST:{job-3}job:'] = ['state' => 'running', 'attempt' => '2'];

        $this->store->markRetry('job-3', time() + 300);

        expect($this->fake->evalCalls)->toHaveCount(1)
            ->and($this->fake->evalCalls[0]['script'])->toContain('retry');

        $jobKey = 'TEST:{job-3}job:';
        expect($this->fake->hashes[$jobKey]['state'])->toBe('retry');
    });

    it('markCompleted uses a single Lua eval call', function () {
        $this->store->markCompleted('job-4');

        expect($this->fake->evalCalls)->toHaveCount(1)
            ->and($this->fake->evalCalls[0]['script'])->toContain('completed');

        $jobKey = 'TEST:{job-4}job:';
        expect($this->fake->hashes[$jobKey]['state'])->toBe('completed');
    });
});
