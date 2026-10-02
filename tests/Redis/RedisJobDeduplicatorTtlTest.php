<?php

declare(strict_types=1);

use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisPipelineContract;
use BAGArt\ASKClientRedis\Redis\RedisJobDeduplicator;

class FakeRedisForDedup implements RedisClientContract
{
    /** @var array<string, array{value: string, expiresAt: int|null}> */
    public array $strings = [];

    /** @var int */
    public int $now;

    public function __construct()
    {
        $this->now = time();
    }

    public function get(string $key): string|false
    {
        return false;
    }

    public function set(string $key, mixed $value, mixed ...$options): RedisClientContract|bool
    {
        $opts = [];
        foreach ($options as $opt) {
            if (is_array($opt)) {
                $opts = array_merge($opts, $opt);
            } else {
                $opts[] = $opt;
            }
        }

        $ttl = null;
        foreach ($opts as $i => $opt) {
            if (is_string($i)) {
                if (strtoupper($i) === 'EX') {
                    $ttl = (int)$opt;
                }

                continue;
            }

            if (is_string($opt) && strtoupper($opt) === 'EX' && isset($opts[$i + 1])) {
                $ttl = (int)$opts[$i + 1];
            }
        }

        $expiresAt = $ttl !== null ? $this->now + $ttl : null;

        $this->strings[$key] = [
            'value' => (string)$value,
            'expiresAt' => $expiresAt,
        ];

        return true;
    }

    public function setex(string $key, int $seconds, string $value): RedisClientContract|bool
    {
        $this->strings[$key] = [
            'value' => $value,
            'expiresAt' => $this->now + $seconds,
        ];
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
        }
        return $count;
    }

    public function exists(string $key, string ...$other_keys): int|false
    {
        $entry = $this->strings[$key] ?? null;
        if ($entry === null) {
            return 0;
        }
        if ($entry['expiresAt'] !== null && $entry['expiresAt'] < $this->now) {
            return 0;
        }
        return 1;
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
        return 0;
    }
    public function hDel(string $key, string $field, string ...$other_fields): int|false
    {
        return 0;
    }
    public function hMSet(string $key, array $keyValues): RedisClientContract|bool
    {
        return true;
    }
    public function hGetAll(string $key): array|false
    {
        return [];
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
        return 0;
    }
    public function zRem(string $key, mixed ...$member): int|false
    {
        return 0;
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
        return null;
    }
}

describe('RedisJobDeduplicator permanent key TTL', function () {
    it('markProcessedPermanent sets key with TTL', function () {
        $fake = new FakeRedisForDedup();
        $dedup = new RedisJobDeduplicator($fake, ttlSeconds: 86400, prefix: 'TEST:');

        $dedup->markProcessedPermanent('job-123');

        $key = 'TEST:job:processed:job-123';
        expect($fake->strings)->toHaveKey($key)
            ->and($fake->strings[$key]['expiresAt'])->not->toBeNull()
            ->and($fake->strings[$key]['expiresAt'])->toBeGreaterThan(time());
    });

    it('permanent key expires after configured TTL', function () {
        $fake = new FakeRedisForDedup();
        $dedup = new RedisJobDeduplicator($fake, ttlSeconds: 300, prefix: 'TEST:');

        $dedup->markProcessedPermanent('job-456');

        $key = 'TEST:job:processed:job-456';
        expect($fake->strings[$key]['expiresAt'])->toBe($fake->now + 300);
    });

    it('tryMark sets key with TTL', function () {
        $fake = new FakeRedisForDedup();
        $dedup = new RedisJobDeduplicator($fake, ttlSeconds: 60, prefix: 'TEST:');

        $dedup->tryMark('job-789');

        $key = 'TEST:job:dedup:job-789';
        expect($fake->strings)->toHaveKey($key)
            ->and($fake->strings[$key]['expiresAt'])->toBe($fake->now + 60);
    });

    it('dedup keys have bounded TTL', function () {
        $fake = new FakeRedisForDedup();
        $dedup = new RedisJobDeduplicator($fake, ttlSeconds: 120, prefix: 'TEST:');

        $dedup->tryMark('job-a');
        $dedup->markProcessedPermanent('job-b');

        $dedupKey = $fake->strings['TEST:job:dedup:job-a'];
        $permKey = $fake->strings['TEST:job:processed:job-b'];

        expect($dedupKey['expiresAt'])->toBe($fake->now + 120)
            ->and($permKey['expiresAt'])->toBe($fake->now + 120);
    });
});
