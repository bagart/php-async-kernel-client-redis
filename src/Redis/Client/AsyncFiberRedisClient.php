<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis\Client;

use BAGArt\ASKClientRedis\Connection\FiberRedisConnection;
use BAGArt\ASKClientRedis\Exception\ASKRedisException;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisPipelineContract;
use BAGArt\ASKClientRedis\Transport\ASKRedisProtocolDecoder;
use BAGArt\ASKClientRedis\Transport\ASKRedisProtocolEncoder;
use BAGArt\AsyncKernel\Contracts\Daemons\ASKWarmableContract;

final class AsyncFiberRedisClient implements RedisClientContract, ASKWarmableContract
{
    private bool $warmed = false;

    public function __construct(
        private readonly FiberRedisConnection $connection,
        private readonly ASKRedisProtocolEncoder $encoder = new ASKRedisProtocolEncoder(),
        private readonly ASKRedisProtocolDecoder $decoder = new ASKRedisProtocolDecoder(),
        private readonly ?string $password = null,
        private readonly ?int $database = null,
    ) {
    }

    public function warm(): void
    {
        if ($this->warmed) {
            return;
        }

        $this->connection->connect();

        if ($this->password !== null) {
            $this->executeCommand('AUTH', [$this->password]);
        }

        if ($this->database !== null) {
            $this->executeCommand('SELECT', [(string) $this->database]);
        }

        $this->warmed = true;
    }

    private function executeCommand(string $command, array $args = []): mixed
    {
        try {
            if (!$this->connection->isConnected()) {
                $this->warmed = false;
                $this->warm();
            }

            $payload = $this->encoder->encodeCommand($command, $args);
            $this->connection->write($payload);

            while (!$this->decoder->hasCompleteResponse()) {
                $chunk = $this->connection->read();
                $this->decoder->feed($chunk);
            }

            $decoded = $this->decoder->decode();

            if ($decoded instanceof \Exception) {
                throw new ASKRedisException($decoded->getMessage(), $decoded->getCode());
            }

            return $decoded;
        } catch (\Throwable $e) {
            $this->warmed = false;
            $this->connection->disconnect();

            throw $e;
        }
    }

    // ================================================================
    // String
    // ================================================================

    public function get(string $key): string|false
    {
        $result = $this->executeCommand('GET', [$key]);

        return $result === null ? false : $result;
    }

    public function set(string $key, mixed $value, mixed ...$options): RedisClientContract|bool
    {
        $args = [$key, (string) $value, ...$options];

        return $this->executeCommand('SET', $args) === 'OK';
    }

    public function setex(string $key, int $seconds, string $value): RedisClientContract|bool
    {
        return $this->executeCommand('SETEX', [$key, (string) $seconds, $value]) === 'OK';
    }

    public function del(array|string $key, string ...$other_keys): int|false
    {
        $keys = is_array($key)
            ? [...$key, ...$other_keys]
            : [$key, ...$other_keys];

        return $this->executeCommand('DEL', $keys);
    }

    public function exists(string $key, string ...$other_keys): int|false
    {
        return $this->executeCommand('EXISTS', [$key, ...$other_keys]);
    }

    public function incrBy(string $key, int $value): int|false
    {
        return $this->executeCommand('INCRBY', [$key, (string) $value]);
    }

    public function decrBy(string $key, int $value): int|false
    {
        return $this->executeCommand('DECRBY', [$key, (string) $value]);
    }

    public function ttl(string $key): int|false
    {
        $result = $this->executeCommand('TTL', [$key]);

        return $result === null ? false : $result;
    }

    public function expire(string $key, int $seconds): RedisClientContract|bool
    {
        return (bool) $this->executeCommand('EXPIRE', [$key, (string) $seconds]);
    }

    public function mget(array $keys): array|false
    {
        $result = $this->executeCommand('MGET', $keys);

        return $result === null ? false : $result;
    }

    public function mset(array $keyValues): RedisClientContract|bool
    {
        $args = [];

        foreach ($keyValues as $k => $v) {
            $args[] = $k;
            $args[] = (string) $v;
        }

        return $this->executeCommand('MSET', $args) === 'OK';
    }

    public function flushDB(): bool
    {
        return $this->executeCommand('FLUSHDB') === 'OK';
    }

    // ================================================================
    // List
    // ================================================================

    public function lPop(string $key): string|false
    {
        $result = $this->executeCommand('LPOP', [$key]);

        return $result === null ? false : $result;
    }

    public function rPush(string $key, mixed ...$values): int|false
    {
        return $this->executeCommand('RPUSH', [$key, ...array_map('strval', $values)]);
    }

    public function lLen(string $key): int|false
    {
        $result = $this->executeCommand('LLEN', [$key]);

        return $result === null ? false : $result;
    }

    public function lPush(string $key, mixed ...$values): int|false
    {
        return $this->executeCommand('LPUSH', [$key, ...array_map('strval', $values)]);
    }

    public function lIndex(string $key, int $index): string|false
    {
        $result = $this->executeCommand('LINDEX', [$key, (string) $index]);

        return $result === null ? false : $result;
    }

    // ================================================================
    // Hash
    // ================================================================

    public function hGet(string $key, string $field): string|false
    {
        $result = $this->executeCommand('HGET', [$key, $field]);

        return $result === null ? false : $result;
    }

    public function hSet(string $key, string $field, mixed $value): int|false
    {
        return $this->executeCommand('HSET', [$key, $field, (string) $value]);
    }

    public function hDel(string $key, string $field, string ...$other_fields): int|false
    {
        return $this->executeCommand('HDEL', [$key, $field, ...$other_fields]);
    }

    public function hMSet(string $key, array $keyValues): RedisClientContract|bool
    {
        $args = [$key];

        foreach ($keyValues as $k => $v) {
            $args[] = $k;
            $args[] = (string) $v;
        }

        return $this->executeCommand('HMSET', $args) === 'OK';
    }

    public function hGetAll(string $key): array|false
    {
        $result = $this->executeCommand('HGETALL', [$key]);

        if ($result === null) {
            return false;
        }

        $map = [];

        for ($i = 0; $i + 1 < count($result); $i += 2) {
            $map[$result[$i]] = $result[$i + 1];
        }

        return $map;
    }

    public function hSetNx(string $key, string $field, mixed $value): int|false
    {
        return $this->executeCommand('HSETNX', [$key, $field, (string) $value]);
    }

    public function hIncrBy(string $key, string $field, int $value): int|false
    {
        return $this->executeCommand('HINCRBY', [$key, $field, (string) $value]);
    }

    public function hLen(string $key): int|false
    {
        return $this->executeCommand('HLEN', [$key]);
    }

    public function hScan(string $key, ?int &$iterator, ?string $pattern = null, int $count = 10): array|false
    {
        $args = [$key, (string) ($iterator ?? 0)];

        if ($pattern !== null) {
            $args[] = 'MATCH';
            $args[] = $pattern;
        }

        $args[] = 'COUNT';
        $args[] = (string) $count;

        $result = $this->executeCommand('HSCAN', $args);

        if ($result === null) {
            return false;
        }

        $iterator = (int) $result[0];

        $pairs = $result[1] ?? [];
        $map = [];

        for ($i = 0; $i + 1 < count($pairs); $i += 2) {
            $map[$pairs[$i]] = $pairs[$i + 1];
        }

        return $map;
    }

    // ================================================================
    // Set
    // ================================================================

    public function sAdd(string $key, mixed ...$values): int|false
    {
        return $this->executeCommand('SADD', [$key, ...array_map('strval', $values)]);
    }

    public function sRem(string $key, mixed ...$values): int|false
    {
        return $this->executeCommand('SREM', [$key, ...array_map('strval', $values)]);
    }

    public function sMembers(string $key): array|false
    {
        $result = $this->executeCommand('SMEMBERS', [$key]);

        return $result === null ? false : $result;
    }

    public function sRandMember(string $key, int $count = 1): string|array|false
    {
        $result = $this->executeCommand('SRANDMEMBER', [$key, (string) $count]);

        return $result === null ? false : $result;
    }

    // ================================================================
    // Sorted Set
    // ================================================================

    public function zAdd(string $key, array $options, float $score, string $member, mixed ...$more): int|false
    {
        return $this->executeCommand('ZADD', [$key, ...$options, (string) $score, $member, ...array_map('strval', $more)]);
    }

    public function zRem(string $key, mixed ...$member): int|false
    {
        return $this->executeCommand('ZREM', [$key, ...array_map('strval', $member)]);
    }

    public function zCard(string $key): int|false
    {
        return $this->executeCommand('ZCARD', [$key]);
    }

    public function zRangeByScore(string $key, string $start, string $end, array $options = []): array|false
    {
        $args = [$key, $start, $end];

        if (isset($options['limit'])) {
            $args[] = 'LIMIT';
            $args[] = (string) $options['limit'][0];
            $args[] = (string) $options['limit'][1];
        }

        $result = $this->executeCommand('ZRANGEBYSCORE', $args);

        return $result === null ? false : $result;
    }

    public function zRange(string $key, int $start, int $end, ?array $options = null): array|false
    {
        $args = [$key, (string) $start, (string) $end];

        if ($options !== null) {
            foreach ($options as $opt) {
                $args[] = $opt;
            }
        }

        $result = $this->executeCommand('ZRANGE', $args);

        return $result === null ? false : $result;
    }

    public function zRevRange(string $key, int $start, int $end, ?array $options = null): array|false
    {
        $args = [$key, (string) $start, (string) $end];

        if ($options !== null) {
            foreach ($options as $opt) {
                $args[] = $opt;
            }
        }

        $result = $this->executeCommand('ZREVRANGE', $args);

        return $result === null ? false : $result;
    }

    public function zScore(string $key, string $member): float|false
    {
        $result = $this->executeCommand('ZSCORE', [$key, $member]);

        return $result === null ? false : (float) $result;
    }

    public function zScan(string $key, ?int &$iterator, ?string $pattern = null, int $count = 10): array|false
    {
        $args = [$key, (string) ($iterator ?? 0)];

        if ($pattern !== null) {
            $args[] = 'MATCH';
            $args[] = $pattern;
        }

        $args[] = 'COUNT';
        $args[] = (string) $count;

        $result = $this->executeCommand('ZSCAN', $args);

        if ($result === null) {
            return false;
        }

        $iterator = (int) $result[0];

        $pairs = $result[1] ?? [];
        $map = [];

        for ($i = 0; $i + 1 < count($pairs); $i += 2) {
            $map[$pairs[$i]] = (float) $pairs[$i + 1];
        }

        return $map;
    }

    public function zPopMax(string $key, int $count = 1): array|false
    {
        $result = $this->executeCommand('ZPOPMAX', [$key, (string) $count]);

        return $result === null ? false : $result;
    }

    // ================================================================
    // Stream
    // ================================================================

    public function xAdd(string $key, string $id, array $fields, int $maxlen = 0, bool $approx = false): string|false
    {
        $args = [$key, $id];

        foreach ($fields as $k => $v) {
            $args[] = $k;
            $args[] = (string) $v;
        }

        if ($maxlen > 0) {
            $args[] = 'MAXLEN';

            if ($approx) {
                $args[] = '~';
            }

            $args[] = (string) $maxlen;
        }

        $result = $this->executeCommand('XADD', $args);

        return $result === null ? false : $result;
    }

    public function xRead(array $streams, int $count = -1, int $block = 0): array|false
    {
        $args = [];

        if ($count > 0) {
            $args[] = 'COUNT';
            $args[] = (string) $count;
        }

        if ($block > 0) {
            $args[] = 'BLOCK';
            $args[] = (string) $block;
        }

        $args[] = 'STREAMS';

        foreach ($streams as $stream => $id) {
            $args[] = $stream;
        }

        foreach ($streams as $stream => $id) {
            $args[] = $id;
        }

        $result = $this->executeCommand('XREAD', $args);

        return $result === null ? false : $result;
    }

    public function xDel(string $key, string ...$ids): int|false
    {
        return $this->executeCommand('XDEL', [$key, ...$ids]);
    }

    public function trim(string $partitionKey, int $maxLen): void
    {
        $this->executeCommand('XTRIM', [$partitionKey, 'MAXLEN', '~', (string) $maxLen]);
    }

    public function xTrim(string $key, array $options): int|false
    {
        return $this->executeCommand('XTRIM', [$key, ...$options]);
    }

    public function xLen(string $key): int|false
    {
        return $this->executeCommand('XLEN', [$key]);
    }

    // ================================================================
    // Scripting
    // ================================================================

    public function eval(string $script, array $args = [], int $numKeys = 0): mixed
    {
        return $this->executeCommand('EVAL', [$script, (string) $numKeys, ...$args]);
    }

    // ================================================================
    // Scan
    // ================================================================

    public function scan(?int &$iterator, ?string $pattern = null, int $count = 0): array|false
    {
        $args = [(string) ($iterator ?? 0)];

        if ($pattern !== null) {
            $args[] = 'MATCH';
            $args[] = $pattern;
        }

        if ($count > 0) {
            $args[] = 'COUNT';
            $args[] = (string) $count;
        }

        $result = $this->executeCommand('SCAN', $args);

        if ($result === null) {
            return false;
        }

        $iterator = (int) $result[0];

        return $result[1] ?? [];
    }

    // ================================================================
    // Pipeline
    // ================================================================

    public function pipeline(): RedisPipelineContract
    {
        throw new \RuntimeException('Pipeline not implemented in async client. Each executeCommand completes one request before the next.');
    }
}
