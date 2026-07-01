<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Contracts;

/**
 * Narrow transport interface for {@see \BAGArt\ASKClientRedis\Lockers\RedisLocker}.
 *
 * Introduced in Phase 0 (outbound pipeline) to decouple RedisLocker from the concrete
 * phpredis class {@see \Redis} and make it testable via a hand-rolled fake
 * (no live Redis and no Mockery — per project test convention).
 *
 * Contains exactly the 2 phpredis methods that RedisLocker actually calls:
 *   - {@see set()} with NX/EX options (atomic set-if-not-exists + TTL)
 *   - {@see eval()} for Lua compare-and-delete script.
 *
 * The real {@see \Redis} structurally satisfies this interface (its methods
 * have compatible signatures). To pass \Redis into RedisLocker directly, wrap
 * it in {@see \BAGArt\ASKClientRedis\Lockers\PhpRedisLockerTransport} — a thin adapter
 * with no logic (needed because PHP does not support structural parameter typing:
 * a typed argument requires explicit implements).
 *
 * NOT intended for other Redis classes (RedisCache, QueueRedisAdapter, ...) — they
 * have their own method overlap (see Phase 0 research), a common transport would be
 * ~40 methods (god-interface).
 */
interface RedisLockerTransport
{
    /**
     * SET with NX/EX options (atomic set-if-not-exists + TTL).
     *
     * Corresponds to {@see \Redis::set()} with options = ['nx', 'ex' => $ttl].
     *
     * @param string                                                                $key     Key.
     * @param string                                                                $value   Value (owner token).
     * @param array<int|string, mixed>|array{0:string,ex?:int,px?:int,nx?:bool,xx?:bool} $options phpredis options: ['nx', 'ex' => 30].
     *
     * @return bool true — set; false — key already exists (NX failed).
     */
    public function set(string $key, string $value, array $options = []): bool;

    /**
     * Execute Lua script (EVAL).
     *
     * Corresponds to {@see \Redis::eval()}.
     *
     * @param string   $script   Lua script body.
     * @param list<mixed> $args  Arguments (KEYS + ARGV in a single array, as in phpredis).
     * @param int      $numKeys  Number of keys at the start of $args.
     *
     * @return mixed Script result (for compare-and-delete: 1 = deleted, 0/nil = not).
     */
    public function eval(string $script, array $args = [], int $numKeys = 0): mixed;
}
