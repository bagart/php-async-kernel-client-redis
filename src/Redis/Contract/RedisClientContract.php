<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis\Contract;

/**
 * High-level Redis client contract covering all commands used by consumers.
 */
interface RedisClientContract
{
    // ----- String operations -----

    public function get(string $key): string|false;

    public function set(string $key, mixed $value, mixed ...$options): RedisClientContract|bool;

    public function setex(string $key, int $seconds, string $value): RedisClientContract|bool;

    public function del(array|string $key, string ...$other_keys): int|false;

    public function exists(string $key, string ...$other_keys): int|false;

    public function incrBy(string $key, int $value): int|false;

    public function decrBy(string $key, int $value): int|false;

    public function ttl(string $key): int|false;

    public function expire(string $key, int $seconds): RedisClientContract|bool;

    public function mget(array $keys): array|false;

    public function mset(array $keyValues): RedisClientContract|bool;

    public function flushDB(): bool;

    // ----- List operations -----

    public function lPop(string $key): string|false;

    public function rPush(string $key, mixed ...$values): int|false;

    public function lLen(string $key): int|false;

    public function lPush(string $key, mixed ...$values): int|false;

    public function lIndex(string $key, int $index): string|false;

    // ----- Hash operations -----

    public function hGet(string $key, string $field): string|false;

    public function hSet(string $key, string $field, mixed $value): int|false;

    public function hDel(string $key, string $field, string ...$other_fields): int|false;

    public function hMSet(string $key, array $keyValues): RedisClientContract|bool;

    public function hGetAll(string $key): array|false;

    public function hSetNx(string $key, string $field, mixed $value): int|false;

    public function hIncrBy(string $key, string $field, int $value): int|false;

    public function hLen(string $key): int|false;

    public function hScan(string $key, ?int &$iterator, ?string $pattern = null, int $count = 10): array|false;

    // ----- Set operations -----

    public function sAdd(string $key, mixed ...$values): int|false;

    public function sRem(string $key, mixed ...$values): int|false;

    public function sMembers(string $key): array|false;

    public function sRandMember(string $key, int $count = 1): string|array|false;

    // ----- Sorted set operations -----

    public function zAdd(string $key, array $options, float $score, string $member, mixed ...$more): int|false;

    public function zRem(string $key, mixed ...$member): int|false;

    public function zCard(string $key): int|false;

    public function zRangeByScore(string $key, string $start, string $end, array $options = []): array|false;

    public function zRange(string $key, int $start, int $end, ?array $options = null): array|false;

    public function zRevRange(string $key, int $start, int $end, ?array $options = null): array|false;

    public function zScore(string $key, string $member): float|false;

    public function zScan(string $key, ?int &$iterator, ?string $pattern = null, int $count = 10): array|false;

    public function zPopMax(string $key, int $count = 1): array|false;

    // ----- Stream operations -----

    public function xAdd(string $key, string $id, array $fields, int $maxlen = 0, bool $approx = false): string|false;

    public function xRead(array $streams, int $count = -1, int $block = 0): array|false;

    public function xDel(string $key, string ...$ids): int|false;

    public function trim(string $partitionKey, int $maxLen): void;

    public function xTrim(string $key, array $options): int|false;

    public function xLen(string $key): int|false;

    // ----- Scripting -----

    public function eval(string $script, array $args = [], int $numKeys = 0): mixed;

    // ----- Keyspace / Scan -----

    public function scan(?int &$iterator, ?string $pattern = null, int $count = 0): array|false;

    // ----- Pipeline -----

    public function pipeline(): RedisPipelineContract;
}
