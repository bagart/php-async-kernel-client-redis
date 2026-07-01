<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis\Client;

use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisPipelineContract;
use BAGArt\ASKClientRedis\Redis\RedisDsn;
use BAGArt\AsyncKernel\Contracts\Daemons\ASKWarmableContract;
use Predis\Client;

final class PredisAdapter implements RedisClientContract, ASKWarmableContract
{
    public function __construct(
        private readonly RedisDsn $redisDsn,
        private ?Client $redis = null,
    ) {
    }

    private function ensureConnected(): void
    {
        if ($this->redis === null) {
            $this->warm();
        }
    }

    public function get(string $key): string|false
    {
        $this->ensureConnected();

        $result = $this->redis->get($key);

        return $result === null ? false : $result;
    }

    public function set(string $key, mixed $value, mixed ...$options): RedisClientContract|bool
    {
        $this->ensureConnected();

        $resolvedOptions = [];
        if (!empty($options)) {
            foreach ($options as $opt) {
                if (is_array($opt)) {
                    $resolvedOptions = array_merge($resolvedOptions, $opt);
                } else {
                    $resolvedOptions[] = $opt;
                }
            }
        }

        $result = empty($resolvedOptions)
            ? $this->redis->set($key, $value)
            : $this->redis->set($key, $value, ...$resolvedOptions);

        return $result ? true : false;
    }

    public function setex(string $key, int $seconds, string $value): RedisClientContract|bool
    {
        $this->ensureConnected();

        $this->redis->setex($key, $seconds, $value);

        return $this;
    }

    public function del(array|string $key, string ...$other_keys): int|false
    {
        $this->ensureConnected();

        $keys = is_array($key) ? $key : array_merge([$key], $other_keys);

        return (int)$this->redis->del($keys);
    }

    public function exists(string $key, string ...$other_keys): int|false
    {
        $this->ensureConnected();

        $keys = array_merge([$key], $other_keys);

        return (int)$this->redis->exists($keys);
    }

    public function incrBy(string $key, int $value): int|false
    {
        $this->ensureConnected();

        return (int)$this->redis->incrby($key, $value);
    }

    public function decrBy(string $key, int $value): int|false
    {
        $this->ensureConnected();

        return (int)$this->redis->decrby($key, $value);
    }

    public function ttl(string $key): int|false
    {
        $this->ensureConnected();

        $ttl = $this->redis->ttl($key);

        return $ttl < 0 ? false : $ttl;
    }

    public function expire(string $key, int $seconds): RedisClientContract|bool
    {
        $this->ensureConnected();

        return (bool)$this->redis->expire($key, $seconds);
    }

    public function mget(array $keys): array|false
    {
        $this->ensureConnected();

        $result = $this->redis->mget($keys);

        return is_array($result) ? $result : false;
    }

    public function mset(array $keyValues): RedisClientContract|bool
    {
        $this->ensureConnected();

        $this->redis->mset($keyValues);

        return $this;
    }

    public function flushDB(): bool
    {
        $this->ensureConnected();

        $this->redis->flushdb();

        return true;
    }

    public function lPop(string $key): string|false
    {
        $this->ensureConnected();

        $result = $this->redis->lpop($key);

        return $result === null ? false : $result;
    }

    public function rPush(string $key, mixed ...$values): int|false
    {
        $this->ensureConnected();

        return (int)$this->redis->rpush($key, ...$values);
    }

    public function lLen(string $key): int|false
    {
        $this->ensureConnected();

        return (int)$this->redis->llen($key);
    }

    public function lPush(string $key, mixed ...$values): int|false
    {
        $this->ensureConnected();

        return (int)$this->redis->lpush($key, ...$values);
    }

    public function lIndex(string $key, int $index): string|false
    {
        $this->ensureConnected();

        $result = $this->redis->lindex($key, $index);

        return $result === null ? false : $result;
    }

    public function hGet(string $key, string $field): string|false
    {
        $this->ensureConnected();

        $result = $this->redis->hget($key, $field);

        return $result === null ? false : $result;
    }

    public function hSet(string $key, string $field, mixed $value): int|false
    {
        $this->ensureConnected();

        return (int)$this->redis->hset($key, $field, $value);
    }

    public function hDel(string $key, string $field, string ...$other_fields): int|false
    {
        $this->ensureConnected();

        return (int)$this->redis->hdel($key, array_merge([$field], $other_fields));
    }

    public function hMSet(string $key, array $keyValues): RedisClientContract|bool
    {
        $this->ensureConnected();

        $this->redis->hmset($key, $keyValues);

        return $this;
    }

    public function hGetAll(string $key): array|false
    {
        $this->ensureConnected();

        $result = $this->redis->hgetall($key);

        return empty($result) ? false : $result;
    }

    public function hSetNx(string $key, string $field, mixed $value): int|false
    {
        $this->ensureConnected();

        return (int)$this->redis->hsetnx($key, $field, $value);
    }

    public function hIncrBy(string $key, string $field, int $value): int|false
    {
        $this->ensureConnected();

        return (int)$this->redis->hincrby($key, $field, $value);
    }

    public function hLen(string $key): int|false
    {
        $this->ensureConnected();

        return (int)$this->redis->hlen($key);
    }

    public function hScan(string $key, ?int &$iterator, ?string $pattern = null, int $count = 10): array|false
    {
        $this->ensureConnected();

        $options = ['COUNT' => $count];
        if ($pattern !== null) {
            $options['MATCH'] = $pattern;
        }

        $result = $this->redis->hscan($key, (int)$iterator, $options);

        if (is_array($result) && isset($result[0])) {
            $iterator = (int)$result[0];
            return $result[1] ?? [];
        }

        return false;
    }

    public function sAdd(string $key, mixed ...$values): int|false
    {
        $this->ensureConnected();

        return (int)$this->redis->sadd($key, ...$values);
    }

    public function sRem(string $key, mixed ...$values): int|false
    {
        $this->ensureConnected();

        return (int)$this->redis->srem($key, ...$values);
    }

    public function sMembers(string $key): array|false
    {
        $this->ensureConnected();

        $result = $this->redis->smembers($key);

        return is_array($result) ? $result : false;
    }

    public function sRandMember(string $key, int $count = 1): string|array|false
    {
        $this->ensureConnected();

        $result = $this->redis->srandmember($key, $count);

        return $result === null ? false : $result;
    }

    // ----- Sorted set operations -----

    public function zAdd(string $key, array $options, float $score, string $member, mixed ...$more): int|false
    {
        $this->ensureConnected();

        // Сборка аргументов для zadd в стиле Predis
        $args = [$key];
        foreach ($options as $opt) {
            $args[] = $opt;
        }
        $args[] = $score;
        $args[] = $member;

        if (!empty($more)) {
            $args = array_merge($args, $more);
        }

        return (int)call_user_func_array([$this->redis, 'zadd'], $args);
    }

    public function zRem(string $key, mixed ...$member): int|false
    {
        $this->ensureConnected();

        return (int)$this->redis->zrem($key, ...$member);
    }

    public function zCard(string $key): int|false
    {
        $this->ensureConnected();

        return (int)$this->redis->zcard($key);
    }

    public function zRangeByScore(string $key, string $start, string $end, array $options = []): array|false
    {
        $this->ensureConnected();

        $result = $this->redis->zrangebyscore($key, $start, $end, $options);

        return is_array($result) ? $result : false;
    }

    public function zRange(string $key, int $start, int $end, ?array $options = null): array|false
    {
        $this->ensureConnected();

        $result = $this->redis->zrange($key, $start, $end, $options ?? []);

        return is_array($result) ? $result : false;
    }

    public function zRevRange(string $key, int $start, int $end, ?array $options = null): array|false
    {
        $this->ensureConnected();

        $result = $this->redis->zrevrange($key, $start, $end, $options ?? []);

        return is_array($result) ? $result : false;
    }

    public function zScore(string $key, string $member): float|false
    {
        $this->ensureConnected();

        $result = $this->redis->zscore($key, $member);

        return $result === null ? false : (float)$result;
    }

    public function zScan(string $key, ?int &$iterator, ?string $pattern = null, int $count = 10): array|false
    {
        $this->ensureConnected();

        $options = ['COUNT' => $count];
        if ($pattern !== null) {
            $options['MATCH'] = $pattern;
        }

        $result = $this->redis->zscan($key, (int)$iterator, $options);

        if (is_array($result) && isset($result[0])) {
            $iterator = (int)$result[0];
            return $result[1] ?? [];
        }

        return false;
    }

    public function zPopMax(string $key, int $count = 1): array|false
    {
        $this->ensureConnected();

        $result = $this->redis->zpopmax($key, $count);

        return is_array($result) ? $result : false;
    }

    // ----- Stream operations -----

    public function xAdd(string $key, string $id, array $fields, int $maxlen = 0, bool $approx = false): string|false
    {
        $this->ensureConnected();

        // Формирование структуры команды XADD для Predis
        $options = [];
        if ($maxlen > 0) {
            $options['MAXLEN'] = $approx ? '~' : '=';
            $options['LIMIT'] = $maxlen;
        }

        $result = $this->redis->xadd($key, $fields, $id, $options);

        return $result === null ? false : (string)$result;
    }

    public function xRead(array $streams, int $count = -1, int $block = 0): array|false
    {
        $this->ensureConnected();

        $options = [];
        if ($count > -1) {
            $options['COUNT'] = $count;
        }
        if ($block > 0) {
            $options['BLOCK'] = $block;
        }

        $result = $this->redis->xread($streams, $options);

        return is_array($result) ? $result : false;
    }

    public function xDel(string $key, string ...$ids): int|false
    {
        $this->ensureConnected();

        return (int)$this->redis->xdel($key, $ids);
    }

    public function trim(string $partitionKey, int $maxLen): void
    {
        $this->ensureConnected();

        $this->redis->xtrim($partitionKey, ['MAXLEN', '~', $maxLen]);
    }

    public function xTrim(string $key, array $options): int|false
    {
        $this->ensureConnected();

        return (int)$this->redis->xtrim($key, $options);
    }

    public function xLen(string $key): int|false
    {
        $this->ensureConnected();

        return (int)$this->redis->xlen($key);
    }

    public function eval(string $script, array $args = [], int $numKeys = 0): mixed
    {
        $this->ensureConnected();

        return $this->redis->eval($script, $numKeys, ...$args);
    }

    public function scan(?int &$iterator, ?string $pattern = null, int $count = 0): array|false
    {
        $this->ensureConnected();

        $options = [];
        if ($pattern !== null) {
            $options['MATCH'] = $pattern;
        }
        if ($count > 0) {
            $options['COUNT'] = $count;
        }

        $result = $this->redis->scan((int)$iterator, $options);

        if (is_array($result) && isset($result[0])) {
            $iterator = (int)$result[0];
            return $result[1] ?? [];
        }

        return false;
    }

    public function pipeline(): RedisPipelineContract
    {
        $this->ensureConnected();

        return new PredisPipelineAdapter($this->redis->pipeline());
    }

    public function warm(): void
    {
        $parameters = [
            'scheme' => 'tcp',
            'host' => $this->redisDsn->host,
            'port' => $this->redisDsn->port,
        ];

        if ($this->redisDsn->password !== null) {
            $parameters['password'] = $this->redisDsn->password;
        }

        if ($this->redisDsn->database !== 0) {
            $parameters['database'] = $this->redisDsn->database;
        }

        $this->redis = new Client($parameters, [
            'timeout' => $this->redisDsn->timeout,
            'read_write_timeout' => $this->redisDsn->timeout,
        ]);

        $this->redis->connect();
    }
}
