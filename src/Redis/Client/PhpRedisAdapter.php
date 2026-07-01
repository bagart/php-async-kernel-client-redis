<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis\Client;

use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisPipelineContract;
use BAGArt\ASKClientRedis\Redis\RedisDsn;
use BAGArt\AsyncKernel\Contracts\Daemons\ASKWarmableContract;
use Redis;

final class PhpRedisAdapter implements RedisClientContract, ASKWarmableContract
{
    public function __construct(
        private readonly RedisDsn $redisDsn,
        private ?Redis $redis = null,
    ) {
    }

    /** Ensures the native Redis handle is connected (lazy connect). */
    private function ensureConnected(): void
    {
        if ($this->redis === null) {
            $this->warm();
        }
    }

    // ----- String operations -----

    public function get(string $key): string|false
    {
        $this->ensureConnected();

        return $this->redis->get($key);
    }

    public function set(string $key, mixed $value, mixed ...$options): RedisClientContract|bool
    {
        $this->ensureConnected();

        return $this->redis->set($key, $value, ...$options);
    }

    public function setex(string $key, int $seconds, string $value): RedisClientContract|bool
    {
        $this->ensureConnected();

        return $this->redis->setex($key, $seconds, $value);
    }

    public function del(array|string $key, string ...$other_keys): int|false
    {
        $this->ensureConnected();

        return $this->redis->del($key, ...$other_keys);
    }

    public function exists(string $key, string ...$other_keys): int|false
    {
        $this->ensureConnected();

        return $this->redis->exists($key, ...$other_keys);
    }

    public function incrBy(string $key, int $value): int|false
    {
        $this->ensureConnected();

        return $this->redis->incrBy($key, $value);
    }

    public function decrBy(string $key, int $value): int|false
    {
        $this->ensureConnected();

        return $this->redis->decrBy($key, $value);
    }

    public function ttl(string $key): int|false
    {
        $this->ensureConnected();

        return $this->redis->ttl($key);
    }

    public function expire(string $key, int $seconds): RedisClientContract|bool
    {
        $this->ensureConnected();

        return $this->redis->expire($key, $seconds);
    }

    public function mget(array $keys): array|false
    {
        $this->ensureConnected();

        return $this->redis->mget($keys);
    }

    public function mset(array $keyValues): RedisClientContract|bool
    {
        $this->ensureConnected();

        return $this->redis->mset($keyValues);
    }

    public function flushDB(): bool
    {
        $this->ensureConnected();

        return $this->redis->flushDB();
    }

    // ----- List operations -----

    public function lPop(string $key): string|false
    {
        $this->ensureConnected();

        return $this->redis->lPop($key);
    }

    public function rPush(string $key, mixed ...$values): int|false
    {
        $this->ensureConnected();

        return $this->redis->rPush($key, ...$values);
    }

    public function lLen(string $key): int|false
    {
        $this->ensureConnected();

        return $this->redis->lLen($key);
    }

    public function lPush(string $key, mixed ...$values): int|false
    {
        $this->ensureConnected();

        return $this->redis->lPush($key, ...$values);
    }

    public function lIndex(string $key, int $index): string|false
    {
        $this->ensureConnected();

        return $this->redis->lIndex($key, $index);
    }

    // ----- Hash operations -----

    public function hGet(string $key, string $field): string|false
    {
        $this->ensureConnected();

        return $this->redis->hGet($key, $field);
    }

    public function hSet(string $key, string $field, mixed $value): int|false
    {
        $this->ensureConnected();

        return $this->redis->hSet($key, $field, $value);
    }

    public function hDel(string $key, string $field, string ...$other_fields): int|false
    {
        $this->ensureConnected();

        return $this->redis->hDel($key, $field, ...$other_fields);
    }

    public function hMSet(string $key, array $keyValues): RedisClientContract|bool
    {
        $this->ensureConnected();

        return $this->redis->hMSet($key, $keyValues);
    }

    public function hGetAll(string $key): array|false
    {
        $this->ensureConnected();

        return $this->redis->hGetAll($key);
    }

    public function hSetNx(string $key, string $field, mixed $value): int|false
    {
        $this->ensureConnected();

        return $this->redis->hSetNx($key, $field, $value);
    }

    public function hIncrBy(string $key, string $field, int $value): int|false
    {
        $this->ensureConnected();

        return $this->redis->hIncrBy($key, $field, $value);
    }

    public function hLen(string $key): int|false
    {
        $this->ensureConnected();

        return $this->redis->hLen($key);
    }

    public function hScan(string $key, ?int &$iterator, ?string $pattern = null, int $count = 10): array|false
    {
        $this->ensureConnected();

        return $this->redis->hScan($key, $iterator, $pattern, $count);
    }

    // ----- Set operations -----

    public function sAdd(string $key, mixed ...$values): int|false
    {
        $this->ensureConnected();

        return $this->redis->sAdd($key, ...$values);
    }

    public function sRem(string $key, mixed ...$values): int|false
    {
        $this->ensureConnected();

        return $this->redis->sRem($key, ...$values);
    }

    public function sMembers(string $key): array|false
    {
        $this->ensureConnected();

        return $this->redis->sMembers($key);
    }

    public function sRandMember(string $key, int $count = 1): string|array|false
    {
        $this->ensureConnected();

        return $this->redis->sRandMember($key, $count);
    }

    // ----- Sorted set operations -----

    public function zAdd(string $key, array $options, float $score, string $member, mixed ...$more): int|false
    {
        $this->ensureConnected();

        return $this->redis->zAdd($key, $options, $score, $member, ...$more);
    }

    public function zRem(string $key, mixed ...$member): int|false
    {
        $this->ensureConnected();

        return $this->redis->zRem($key, ...$member);
    }

    public function zCard(string $key): int|false
    {
        $this->ensureConnected();

        return $this->redis->zCard($key);
    }

    public function zRangeByScore(string $key, string $start, string $end, array $options = []): array|false
    {
        $this->ensureConnected();

        return $this->redis->zRangeByScore($key, $start, $end, $options);
    }

    public function zRange(string $key, int $start, int $end, ?array $options = null): array|false
    {
        $this->ensureConnected();

        return $this->redis->zRange($key, $start, $end, $options);
    }

    public function zRevRange(string $key, int $start, int $end, ?array $options = null): array|false
    {
        $this->ensureConnected();

        return $this->redis->zRevRange($key, $start, $end, $options);
    }

    public function zScore(string $key, string $member): float|false
    {
        $this->ensureConnected();

        return $this->redis->zScore($key, $member);
    }

    public function zScan(string $key, ?int &$iterator, ?string $pattern = null, int $count = 10): array|false
    {
        $this->ensureConnected();

        return $this->redis->zScan($key, $iterator, $pattern, $count);
    }

    public function zPopMax(string $key, int $count = 1): array|false
    {
        $this->ensureConnected();

        return $this->redis->zPopMax($key, $count);
    }

    // ----- Stream operations -----

    public function xAdd(string $key, string $id, array $fields, int $maxlen = 0, bool $approx = false): string|false
    {
        $this->ensureConnected();

        return $this->redis->xAdd($key, $id, $fields, $maxlen, $approx);
    }

    public function xRead(array $streams, int $count = -1, int $block = 0): array|false
    {
        $this->ensureConnected();

        return $this->redis->xRead($streams, $count, $block);
    }

    public function xDel(string $key, string ...$ids): int|false
    {
        $this->ensureConnected();

        return $this->redis->xDel($key, ...$ids);
    }

    public function trim(string $partitionKey, int $maxLen): void
    {
        $this->ensureConnected();

        $this->redis->xTrim(
            $partitionKey,
            (string)$maxLen,
            true,
        );
    }

    public function xTrim(string $key, array $options): int|false
    {
        $this->ensureConnected();

        $threshold = '0';
        $approx = false;
        $minid = false;
        $limit = -1;
        $skipNext = false;

        foreach ($options as $index => $val) {
            if ($skipNext) {
                $skipNext = false;
                continue;
            }

            $upperVal = is_string($val) ? strtoupper($val) : '';

            if ($upperVal === 'MAXLEN') {
                $minid = false;
            } elseif ($upperVal === 'MINID') {
                $minid = true;
            } elseif ($val === '~') {
                $approx = true;
            } elseif ($upperVal === 'LIMIT') {
                if (isset($options[$index + 1])) {
                    $limit = (int)$options[$index + 1];
                    $skipNext = true;
                }
            } else {
                $threshold = (string)$val;
            }
        }

        return $this->redis->xTrim($key, $threshold, $approx, $minid, $limit);
    }

    public function xLen(string $key): int|false
    {
        $this->ensureConnected();

        return $this->redis->xLen($key);
    }

    // ----- Scripting -----

    public function eval(string $script, array $args = [], int $numKeys = 0): mixed
    {
        $this->ensureConnected();

        return $this->redis->eval($script, $args, $numKeys);
    }

    // ----- Keyspace / Scan -----

    public function scan(?int &$iterator, ?string $pattern = null, int $count = 0): array|false
    {
        $this->ensureConnected();

        return $this->redis->scan($iterator, $pattern, $count);
    }

    // ----- Pipeline -----

    public function pipeline(): RedisPipelineContract
    {
        $this->ensureConnected();

        return new PhpRedisPipelineAdapter($this->redis->pipeline());
    }

    public function warm(): void
    {
        $this->redis = new Redis();
        $this->redis->connect(
            $this->redisDsn->host,
            $this->redisDsn->port,
            $this->redisDsn->timeout,
        );
    }
}
