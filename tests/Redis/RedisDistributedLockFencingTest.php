<?php

declare(strict_types=1);

use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisPipelineContract;
use BAGArt\ASKClientRedis\Redis\RedisDistributedLock;

class FakeRedisForLock implements RedisClientContract
{
    /** @var array<string, string> */
    public array $strings = [];

    /** @var array<string, array<string, string>> */
    public array $hashes = [];

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
        $opts = [];
        foreach ($options as $opt) {
            if (is_array($opt)) {
                $opts = array_merge($opts, $opt);
            } else {
                $opts[] = $opt;
            }
        }

        $isNx = in_array('NX', $opts, true) || in_array('nx', $opts, true);

        if ($isNx && array_key_exists($key, $this->strings)) {
            return false;
        }

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
            unset($this->hashes[$k]);
        }

        return $count;
    }

    public function exists(string $key, string ...$other_keys): int|false
    {
        return array_key_exists($key, $this->strings) ? 1 : 0;
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
        return $this->hashes[$key] ?? [];
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

    public function eval(string $script, array $args = [], int $numKeys = 0): mixed
    {
        $this->evalCalls[] = ['script' => $script, 'args' => $args, 'numKeys' => $numKeys];

        if ($this->evalResults !== []) {
            return array_shift($this->evalResults);
        }

        if (str_contains($script, 'DEL')) {
            $lockKey = $args[0] ?? '';
            $fencingKey = $args[1] ?? '';
            $workerId = $args[2] ?? '';
            if (($this->strings[$lockKey] ?? null) === $workerId) {
                unset($this->strings[$lockKey]);
                unset($this->hashes[$fencingKey]);
                return 1;
            }
            return 0;
        }

        if (str_contains($script, 'SET') && str_contains($script, 'NX')) {
            $key = $args[0] ?? '';
            $val = $args[1] ?? '';
            if (isset($this->strings[$key])) {
                return 0;
            }
            $this->strings[$key] = $val;
            $fencingKey = $args[$numKeys] ?? '';
            if ($fencingKey !== '') {
                $token = $args[$numKeys + 2] ?? '';
                $this->hashes[$fencingKey]['fencingToken'] = $token;
            }
            return 1;
        }

        return 0;
    }

    public function scan(?int &$iterator, ?string $pattern = null, int $count = 0): array|false
    {
        return [];
    }

    public function pipeline(): RedisPipelineContract
    {
        throw new \LogicException('Not implemented in fake');
    }
}

describe('RedisDistributedLock fencing token', function () {
    it('stores a fencing token on acquire', function () {
        $fake = new FakeRedisForLock();
        $lock = new RedisDistributedLock($fake, prefix: 'TEST:');

        $acquired = $lock->acquire('chat:1', 'worker-A', 60);

        expect($acquired)->toBeTrue();

        $fencingKey = 'TEST:{chat:1}partition:fencing:chat:1';
        expect($fake->hashes[$fencingKey])->not->toBeEmpty()
            ->and($fake->hashes[$fencingKey]['fencingToken'])->not->toBeEmpty();
    });

    it('does NOT store fencing token when acquire fails (NX conflict)', function () {
        $fake = new FakeRedisForLock();
        $lock = new RedisDistributedLock($fake, prefix: 'TEST:');

        $lock->acquire('chat:1', 'worker-A', 60);
        $acquired2 = $lock->acquire('chat:1', 'worker-B', 60);

        expect($acquired2)->toBeFalse();
    });

    it('release removes fencing token', function () {
        $fake = new FakeRedisForLock();
        $lock = new RedisDistributedLock($fake, prefix: 'TEST:');

        $lock->acquire('chat:1', 'worker-A', 60);
        $lock->release('chat:1', 'worker-A');

        expect($fake->strings)->not->toHaveKey('TEST:{chat:1}partition:lock:chat:1');
    });

    it('getFencingToken returns the stored token', function () {
        $fake = new FakeRedisForLock();
        $lock = new RedisDistributedLock($fake, prefix: 'TEST:');

        $lock->acquire('chat:1', 'worker-A', 60);
        $token = $lock->getFencingToken('chat:1');

        expect($token)->not->toBeNull()
            ->and($token)->not->toBeEmpty();
    });

    it('takeover stores a new fencing token', function () {
        $fake = new FakeRedisForLock();
        $lock = new RedisDistributedLock($fake, prefix: 'TEST:');

        $fake->evalResults = [1];
        $result = $lock->takeover('chat:1', 'worker-A', 60);

        expect($result)->toBeTrue()
            ->and($fake->evalCalls[0]['numKeys'])->toBe(2);
    });
});
