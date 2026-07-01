<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Lockers;

use BAGArt\ASKClientRedis\Contracts\RedisLockerTransport;
use BAGArt\AsyncKernel\Contracts\ASKLockerContract;

/**
 * Redis locker implementation (distributed locking, multi-process safe).
 *
 * Depends on the narrow {@see RedisLockerTransport} (rather than on concrete {@see \Redis}),
 * making the class testable via a hand-rolled fake without live Redis.
 *
 * Existing {@see acquire()}/{@see release()} use a per-instance random token
 * as owner (as before Phase 0). New {@see acquireWithTtl()}/{@see releaseWithOwner()}
 * allow explicit TTL and owner — for ordering lock (todo.md §3.5), where owner = task.id.
 *
 * Atomicity:
 *   - acquire: SET NX EX (single round-trip, atomic).
 *   - release: Lua compare-and-delete (GET == owner → DEL, atomic).
 */
final class RedisLocker implements ASKLockerContract
{
    private const string KEY_PREFIX = 'ask:lock:';

    private const int LOCK_TTL_SECONDS = 30;

    /**
     * Lua: atomic deletion only when owner matches.
     * Returns 1 (deleted) or 0 (not owner / key doesn't exist).
     */
    private const string LUA_RELEASE = <<<'LUA'
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
LUA;

    private readonly string $token;

    public function __construct(
        private readonly RedisLockerTransport $redis,
    ) {
        $this->token = bin2hex(random_bytes(16));
    }

    public function acquire(string $key): bool
    {
        return $this->acquireWithTtl($key, self::LOCK_TTL_SECONDS, $this->token);
    }

    public function release(string $key): void
    {
        $this->releaseWithOwner($key, $this->token);
    }

    public function acquireWithTtl(string $key, int $ttl, ?string $owner = null): bool
    {
        $redisKey = self::KEY_PREFIX.$key;
        $ownerToken = $owner ?? $this->token;

        $result = $this->redis->set(
            $redisKey,
            $ownerToken,
            ['nx', 'ex' => $ttl > 0 ? $ttl : self::LOCK_TTL_SECONDS],
        );

        return $result === true;
    }

    public function releaseWithOwner(string $key, ?string $owner = null): void
    {
        $redisKey = self::KEY_PREFIX.$key;
        $ownerToken = $owner ?? $this->token;

        // owner = null → unconditional release (back-compat semantics).
        // Implemented via the same Lua, but owner = this instance's random token —
        // this will NOT release another's lock (only if we acquired it without owner).
        // For a truly unconditional release (DEL without check), the caller should
        // use a different mechanism — but such a need is not established in project convention.
        $this->redis->eval(
            self::LUA_RELEASE,
            [$redisKey, $ownerToken],
            1,
        );
    }
}
