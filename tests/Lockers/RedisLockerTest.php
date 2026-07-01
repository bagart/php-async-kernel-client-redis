<?php

declare(strict_types=1);

use BAGArt\ASKClientRedis\Contracts\RedisLockerTransport;
use BAGArt\ASKClientRedis\Lockers\PhpRedisLockerTransport;
use BAGArt\ASKClientRedis\Lockers\RedisLocker;

/**
 * Hand-rolled fake {@see RedisLockerTransport} for RedisLocker tests.
 * Does not use Mockery (project convention — hand-rolled fakes).
 * Records set()/eval() calls and returns pre-recorded results.
 */
class FakeRedisLockerTransport implements RedisLockerTransport
{
    /** @var array<int, array{key: string, value: string, options: array}> */
    public array $setCalls = [];

    /** @var array<int, array{script: string, args: array, numKeys: int}> */
    public array $evalCalls = [];

    /** @var array<string, bool> keys → set() result (true = set, false = NX conflict) */
    public array $setResults = [];

    /** @var list<mixed> eval result queue (FIFO). */
    public array $evalResults = [];

    /** @var array<string, string> Key storage for simulating Lua compare-and-delete. */
    public array $store = [];

    public function set(string $key, string $value, array $options = []): bool
    {
        $this->setCalls[] = ['key' => $key, 'value' => $value, 'options' => $options];

        // Simulate SET NX: if key already exists and 'nx' option is present — false.
        $isNx = in_array('nx', $options, true) || in_array('NX', $options, true);

        if ($isNx && array_key_exists($key, $this->store)) {
            return false;
        }

        $this->store[$key] = $value;

        return true;
    }

    public function eval(string $script, array $args = [], int $numKeys = 0): mixed
    {
        $this->evalCalls[] = ['script' => $script, 'args' => $args, 'numKeys' => $numKeys];

        // If there are pre-recorded results — return them in order.
        if ($this->evalResults !== []) {
            return array_shift($this->evalResults);
        }

        // Otherwise simulate Lua compare-and-delete: GET key == owner → DEL.
        $key = $args[0] ?? '';
        $owner = $args[1] ?? '';

        if (($this->store[$key] ?? null) === $owner) {
            unset($this->store[$key]);

            return 1;
        }

        return 0;
    }
}

describe('RedisLocker', function () {
    it('acquires via SET NX EX with the per-instance token as value', function () {
        $fake = new FakeRedisLockerTransport();
        $locker = new RedisLocker($fake);

        expect($locker->acquire('chat:1'))->toBeTrue()
            ->and($fake->setCalls)->toHaveCount(1)
            ->and($fake->setCalls[0]['key'])->toBe('ask:lock:chat:1')
            ->and($fake->setCalls[0]['options'])->toContain('nx')
            ->and($fake->setCalls[0]['value'])->not->toBeEmpty();
    });

    it('rejects acquire when SET NX returns false (key already exists)', function () {
        $fake = new FakeRedisLockerTransport();
        $locker = new RedisLocker($fake);

        $locker->acquire('chat:1');

        // Second acquire of the same key — fake returns false (NX conflict, key already in store).
        expect($locker->acquire('chat:1'))->toBeFalse();
    });

    it('release calls Lua compare-and-delete script', function () {
        $fake = new FakeRedisLockerTransport();
        $locker = new RedisLocker($fake);

        $locker->acquire('chat:1');
        $locker->release('chat:1');

        expect($fake->evalCalls)->toHaveCount(1)
            ->and($fake->evalCalls[0]['script'])->toContain('GET')
            ->and($fake->evalCalls[0]['script'])->toContain('DEL')
            ->and($fake->evalCalls[0]['args'][0])->toBe('ask:lock:chat:1');
    });
});

describe('RedisLocker::acquireWithTtl', function () {
    it('uses the provided TTL in the EX option', function () {
        $fake = new FakeRedisLockerTransport();
        $locker = new RedisLocker($fake);

        $locker->acquireWithTtl('chat:1', 45, 'owner-A');

        expect($fake->setCalls[0]['options'])->toBe(['nx', 'ex' => 45])
            ->and($fake->setCalls[0]['value'])->toBe('owner-A');
    });

    it('falls back to default TTL when ttl <= 0', function () {
        $fake = new FakeRedisLockerTransport();
        $locker = new RedisLocker($fake);

        $locker->acquireWithTtl('chat:1', 0, 'owner-A');

        // TTL=0 → fallback to default 30s (LOCK_TTL_SECONDS).
        expect($fake->setCalls[0]['options'])->toBe(['nx', 'ex' => 30]);
    });

    it('uses the per-instance token when owner is null', function () {
        $fake = new FakeRedisLockerTransport();
        $locker = new RedisLocker($fake);

        $locker->acquireWithTtl('chat:1', 60);

        // owner=null → value = random token (non-empty).
        expect($fake->setCalls[0]['value'])->not->toBeEmpty();
    });

    it('rejects a second acquire with a different owner (NX conflict)', function () {
        $fake = new FakeRedisLockerTransport();
        $locker = new RedisLocker($fake);

        $locker->acquireWithTtl('chat:1', 60, 'owner-A');

        expect($locker->acquireWithTtl('chat:1', 60, 'owner-B'))->toBeFalse();
    });
});

describe('RedisLocker::releaseWithOwner', function () {
    it('releases when owner matches (Lua returns 1)', function () {
        $fake = new FakeRedisLockerTransport();
        $locker = new RedisLocker($fake);

        $locker->acquireWithTtl('chat:1', 60, 'owner-A');
        $locker->releaseWithOwner('chat:1', 'owner-A');

        // Fake deleted the key from store → next acquire succeeds.
        expect($locker->acquireWithTtl('chat:1', 60, 'owner-B'))->toBeTrue();
    });

    it('does NOT release when owner does not match (Lua returns 0)', function () {
        $fake = new FakeRedisLockerTransport();
        $locker = new RedisLocker($fake);

        $locker->acquireWithTtl('chat:1', 60, 'owner-A');
        $locker->releaseWithOwner('chat:1', 'owner-B');

        // Key not deleted → new acquire is blocked.
        expect($locker->acquireWithTtl('chat:1', 60, 'owner-C'))->toBeFalse();
    });

    it('passes the owner token (or fallback to per-instance token) as Lua ARGV[1]', function () {
        $fake = new FakeRedisLockerTransport();
        $locker = new RedisLocker($fake);

        $locker->acquireWithTtl('chat:1', 60, 'owner-A');
        $locker->releaseWithOwner('chat:1', 'owner-A');

        expect($fake->evalCalls[0]['args'][1])->toBe('owner-A');
    });
});

describe('PhpRedisLockerTransport', function () {
    it('implements RedisLockerTransport', function () {
        // Verify wrapper satisfies the interface (no live Redis —
        // uses Reflection on method existence).
        expect(PhpRedisLockerTransport::class)
            ->toImplement(RedisLockerTransport::class);
    });
});
