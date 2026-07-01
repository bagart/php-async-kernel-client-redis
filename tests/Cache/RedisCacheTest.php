<?php

declare(strict_types=1);

use BAGArt\ASKClientRedis\Cache\RedisCache;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;
use BAGArt\AsyncKernel\Contracts\ASKClockContract;

/**
 * Hand-rolled fake {@see RedisClientContract} for RedisCache tests.
 * Records calls and returns pre-recorded results.
 */
class FakeRedisClientForCache implements RedisClientContract
{
    /** @var array<string, string|false> Key-value store for get/set simulation. */
    public array $store = [];

    /** @var list<string> Recorded method calls for assertions. */
    public array $calls = [];

    /** @var int Simulated TTL per key (set via setex). */
    public array $ttls = [];

    public function get(string $key): string|false
    {
        $this->calls[] = "get:{$key}";

        return $this->store[$key] ?? false;
    }

    public function set(string $key, mixed $value, mixed ...$options): RedisClientContract|bool
    {
        $this->calls[] = "set:{$key}";
        $this->store[$key] = $value;

        return true;
    }

    public function setex(string $key, int $seconds, string $value): RedisClientContract|bool
    {
        $this->calls[] = "setex:{$key}:{$seconds}";
        $this->store[$key] = $value;
        $this->ttls[$key] = $seconds;

        return true;
    }

    public function del(array|string $key, string ...$other_keys): int|false
    {
        $keys = is_array($key) ? $key : [$key, ...$other_keys];
        foreach ($keys as $k) {
            unset($this->store[$k]);
            unset($this->ttls[$k]);
        }

        return count($keys);
    }

    public function exists(string $key, string ...$other_keys): int|false
    {
        return array_key_exists($key, $this->store) ? 1 : 0;
    }

    public function incrBy(string $key, int $value): int|false
    {
        $this->calls[] = "incrBy:{$key}:{$value}";
        $current = (int) ($this->store[$key] ?? '0');

        $result = $current + $value;
        $this->store[$key] = (string) $result;

        return $result;
    }

    public function decrBy(string $key, int $value): int|false
    {
        $this->calls[] = "decrBy:{$key}:{$value}";
        $current = (int) ($this->store[$key] ?? '0');

        $result = $current - $value;
        $this->store[$key] = (string) $result;

        return $result;
    }

    public function ttl(string $key): int|false
    {
        return $this->ttls[$key] ?? -1;
    }

    public function expire(string $key, int $seconds): RedisClientContract|bool
    {
        $this->ttls[$key] = $seconds;

        return true;
    }

    public function mget(array $keys): array|false
    {
        $results = [];
        foreach ($keys as $key) {
            $results[] = $this->store[$key] ?? false;
        }

        return $results;
    }

    public function mset(array $keyValues): RedisClientContract|bool
    {
        foreach ($keyValues as $key => $value) {
            $this->store[$key] = $value;
        }

        return true;
    }

    public function flushDB(): bool
    {
        $this->store = [];
        $this->ttls = [];

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

    public function trim(string $partitionKey, int $maxLen): void
    {
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
}

/**
 * Hand-rolled fake clock for RedisCache tests.
 */
class FakeClockForCache implements ASKClockContract
{
    public function microtime(): float
    {
        return 0.0;
    }
    public function time(): int
    {
        return 1_700_000_000;
    }
    public function timeMs(): int
    {
        return 1_700_000_000_000;
    }
    public function hrtime(): int
    {
        return 1_700_000_000_000_000_000;
    }
    public function sleep(int $microseconds): void
    {
    }
    public function getSecondsFromInterval(DateInterval $interval): int
    {
        $total = 0;
        if ($interval->y) {
            $total += $interval->y * 365 * 86400;
        }
        if ($interval->m) {
            $total += $interval->m * 30 * 86400;
        }
        if ($interval->d) {
            $total += $interval->d * 86400;
        }
        if ($interval->h) {
            $total += $interval->h * 3600;
        }
        if ($interval->i) {
            $total += $interval->i * 60;
        }
        $total += $interval->s;

        return $total;
    }
}

describe('RedisCache', function () {
    beforeEach(function () {
        $this->fake = new FakeRedisClientForCache();
        $this->clock = new FakeClockForCache();
        $this->cache = new RedisCache($this->fake, $this->clock);
    });

    it('stores and retrieves a value', function () {
        $this->cache->set('key', 'value');

        expect($this->cache->get('key'))->toBe('value');
    });

    it('returns default when key does not exist', function () {
        expect($this->cache->get('missing', 'fallback'))->toBe('fallback');
    });

    it('stores with TTL via setex', function () {
        $this->cache->set('key', 'value', 60);

        expect($this->fake->calls)
            ->toContain('setex:key:60');
    });

    it('stores without TTL via set', function () {
        $this->cache->set('key', 'value', null);

        expect($this->fake->calls)
            ->toContain('set:key');
    });

    it('deletes a key', function () {
        $this->cache->set('key', 'value');
        $this->cache->delete('key');

        expect($this->cache->get('key'))->toBeNull();
    });

    it('checks if key exists', function () {
        expect($this->cache->has('missing'))->toBeFalse();

        $this->cache->set('key', 'value');

        expect($this->cache->has('key'))->toBeTrue();
    });

    it('clears all keys', function () {
        $this->cache->set('a', 1);
        $this->cache->set('b', 2);
        $this->cache->clear();

        expect($this->fake->store)->toBeEmpty();
    });

    it('serializes complex values', function () {
        $data = ['nested' => ['array' => true], 'int' => 42];
        $this->cache->set('key', $data);

        expect($this->cache->get('key'))->toBe($data);
    });

    it('handles getMultiple', function () {
        $this->cache->set('a', 1);
        $this->cache->set('b', 2);

        expect($this->cache->getMultiple(['a', 'b', 'missing']))
            ->toBe(['a' => 1, 'b' => 2, 'missing' => null]);
    });

    it('handles setMultiple', function () {
        $this->cache->setMultiple(['a' => 1, 'b' => 2]);

        expect($this->cache->get('a'))->toBe(1)
            ->and($this->cache->get('b'))->toBe(2);
    });

    it('deletes multiple keys', function () {
        $this->cache->set('a', 1);
        $this->cache->set('b', 2);

        $this->cache->deleteMultiple(['a', 'b']);

        expect($this->cache->get('a'))->toBeNull()
            ->and($this->cache->get('b'))->toBeNull();
    });

    it('increments and decrements', function () {
        $this->cache->set('counter', 5);

        expect($this->cache->increment('counter', 3))->toBe(8)
            ->and($this->cache->decrement('counter', 2))->toBe(6);
    });

    it('stores with DateInterval TTL', function () {
        $interval = new DateInterval('PT30S');
        $this->cache->set('key', 'value', $interval);

        expect($this->fake->calls)
            ->toContain('setex:key:30');
    });
});
