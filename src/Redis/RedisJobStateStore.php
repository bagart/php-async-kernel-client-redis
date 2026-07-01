<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis;

use BAGArt\ASKClient\Contracts\Queue\JobStateStoreContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;
use BAGArt\AsyncKernel\Job\JobState;
use BAGArt\AsyncKernel\Job\JobStateMachine;

final class RedisJobStateStore implements JobStateStoreContract
{
    private const string SUFFIX_JOB = 'job:';

    private const string SUFFIX_RETRY_DISPATCH = 'job:retry:dispatch:';

    private const string SUFFIX_ZOMBIE = 'job:running';

    private const string SUFFIX_FENCING = 'job:fencing:';

    private const string SUFFIX_WORKER_ALIVE = 'worker:alive:';

    private const string ATOMIC_CONSUME = <<<'LUA'
local stateKey = KEYS[1]
local fencingKey = KEYS[2]
local zombieKey = KEYS[3]
local lockKey = KEYS[4]

local jobId = KEYS[5]
local workerId = ARGV[1]
local token = ARGV[2]
local now = ARGV[3]
local payload = ARGV[4]

-- read current state
local state = redis.call('HGET', stateKey, 'state')

-- terminal states reject
if state == 'completed' or state == 'failed' or state == 'dead_letter' then
    return nil
end

-- acquire lock (prevent double claim)
if redis.call('SETNX', lockKey, workerId) == 0 then
    local currentOwner = redis.call('GET', lockKey)
    if currentOwner ~= workerId then
        return nil
    end
end

redis.call('EXPIRE', lockKey, 3600)

-- mark running + workerId + token
local fields = {
    'state', 'running',
    'workerId', workerId,
    'fencingToken', token,
    'startedAt', now,
}

if payload and payload ~= '' then
    table.insert(fields, 'payload')
    table.insert(fields, payload)
end

redis.call('HSET', stateKey, unpack(fields))

-- zombie index
redis.call('ZADD', zombieKey, now, jobId)

-- store fencing token separately for fast verify
redis.call('HSET', fencingKey, 'workerId', workerId, 'token', token)

return token
LUA;

    private const string LUA_START = <<<'LUA'
local lockKey = KEYS[1]
local stateKey = KEYS[2]
local fencingKey = KEYS[3]
local zombieKey = KEYS[4]
local jobId = KEYS[5]

local workerId = ARGV[1]
local now = ARGV[2]
local token = ARGV[3]

if redis.call('SETNX', lockKey, workerId) == 0 then
    return 0
end

redis.call('EXPIRE', lockKey, 3600)

redis.call('HSET', stateKey,
    'state', 'running',
    'workerId', workerId,
    'fencingToken', token,
    'startedAt', now,
    'attempt', ARGV[4]
)

redis.call('ZADD', zombieKey, now, jobId)
redis.call('HSET', fencingKey, 'workerId', workerId, 'token', token)

return 1
LUA;

    private const string LUA_COMPLETE = <<<'LUA'
local stateKey = KEYS[1]
local zombieKey = KEYS[2]
local jobId = KEYS[3]
local retryKey = KEYS[4]
local fencingKey = KEYS[5]

local state = redis.call('HGET', stateKey, 'state')

if state == 'completed' then
    return 0
end

if state ~= 'running' then
    return 0
end

redis.call('HSET', stateKey,
    'state', 'completed',
    'completedAt', ARGV[1]
)

redis.call('ZREM', zombieKey, jobId)
redis.call('DEL', retryKey)
redis.call('DEL', fencingKey)

return 1
LUA;

    public function __construct(
        private readonly RedisClientContract $redis,
        private readonly string $prefix = 'ASK:',
    ) {
    }

    private function jobKey(string $jobId): string
    {
        return $this->prefix.self::SUFFIX_JOB.$jobId;
    }

    private function lockKey(string $jobId): string
    {
        return $this->prefix.self::SUFFIX_JOB.'lock:'.$jobId;
    }

    private function retryDispatchKey(string $jobId): string
    {
        return $this->prefix.self::SUFFIX_RETRY_DISPATCH.$jobId;
    }

    private function zombieKey(): string
    {
        return $this->prefix.self::SUFFIX_ZOMBIE;
    }

    private function fencingKey(string $jobId): string
    {
        return $this->prefix.self::SUFFIX_FENCING.$jobId;
    }

    private function workerAliveKey(string $workerId): string
    {
        return $this->prefix.self::SUFFIX_WORKER_ALIVE.$workerId;
    }

    public function atomicConsume(string $jobId, string $workerId, ?string $payload = null): ?string
    {
        $token = bin2hex(random_bytes(16));

        $result = $this->redis->eval(
            self::ATOMIC_CONSUME,
            [
                $this->jobKey($jobId),
                $this->fencingKey($jobId),
                $this->zombieKey(),
                $this->lockKey($jobId),
                $jobId,
                $workerId,
                $token,
                time(),
                $payload ?? '',
            ],
            5
        );

        return $result ? (string)$result : null;
    }

    public function tryMarkRetryDispatch(string $jobId): bool
    {
        return (bool)$this->redis->set(
            $this->retryDispatchKey($jobId),
            '1',
            ['NX', 'EX' => 3600]
        );
    }

    public function tryStart(string $jobId, string $workerId, int $ttlSeconds = 3600): bool
    {
        $token = bin2hex(random_bytes(16));

        return (bool)$this->redis->eval(
            self::LUA_START,
            [
                $this->lockKey($jobId),
                $this->jobKey($jobId),
                $this->fencingKey($jobId),
                $this->zombieKey(),
                $jobId,
                $workerId,
                time(),
                $token,
                0,
            ],
            5
        );
    }

    public function claim(string $jobId, string $workerId, ?string $payload = null): bool
    {
        return $this->claimWithFencing($jobId, $workerId, $payload) !== null;
    }

    public function claimWithFencing(string $jobId, string $workerId, ?string $payload = null): ?string
    {
        return $this->atomicConsume($jobId, $workerId, $payload);
    }

    public function verifyFencing(string $jobId, string $workerId, string $fencingToken): bool
    {
        $key = $this->fencingKey($jobId);

        $storedWorker = $this->redis->hGet($key, 'workerId');
        $storedToken = $this->redis->hGet($key, 'token');

        return $storedWorker === $workerId && $storedToken === $fencingToken;
    }

    public function getPayload(string $jobId): ?string
    {
        $raw = $this->redis->hGet($this->jobKey($jobId), 'payload');

        return $raw !== false ? $raw : null;
    }

    public function markCompleted(string $jobId): bool
    {
        return (bool)$this->redis->eval(
            self::LUA_COMPLETE,
            [
                $this->jobKey($jobId),
                $this->zombieKey(),
                $jobId,
                $this->retryDispatchKey($jobId),
                $this->fencingKey($jobId),
                time(),
            ],
            5
        );
    }

    public function markFailed(string $jobId, string $error): void
    {
        JobStateMachine::transition(
            JobState::from($this->getState($jobId) ?? 'new'),
            JobState::FAILED,
        );

        $this->redis->hMSet($this->jobKey($jobId), [
            'state' => 'failed',
            'error' => $error,
            'completedAt' => time(),
        ]);

        $this->redis->zRem($this->zombieKey(), $jobId);
        $this->redis->del($this->fencingKey($jobId));
    }

    public function markDeadLetter(string $jobId, string $error): void
    {
        $this->redis->hMSet($this->jobKey($jobId), [
            'state' => 'dead_letter',
            'error' => $error,
            'completedAt' => time(),
        ]);

        $this->redis->zRem($this->zombieKey(), $jobId);
        $this->redis->del($this->fencingKey($jobId));
        $this->redis->del($this->retryDispatchKey($jobId));
    }

    public function markRetry(string $jobId, int $retryAt): void
    {
        $key = $this->jobKey($jobId);

        $currentState = $this->getState($jobId);

        if ($currentState !== null) {
            JobStateMachine::transition(
                JobState::from($currentState),
                JobState::RETRY,
            );
        }

        $attempt = (int)$this->redis->hGet($key, 'attempt');

        $this->redis->hMSet($key, [
            'state' => 'retry',
            'retryAt' => $retryAt,
            'attempt' => $attempt + 1,
        ]);

        $this->redis->zRem($this->zombieKey(), $jobId);
        $this->redis->del($this->fencingKey($jobId));
    }

    public function isCompleted(string $jobId): bool
    {
        return $this->getState($jobId) === 'completed';
    }

    public function getState(string $jobId): ?string
    {
        $raw = $this->redis->hGet($this->jobKey($jobId), 'state');

        return $raw !== false ? $raw : null;
    }

    public function getMeta(string $jobId): ?array
    {
        $raw = $this->redis->hGetAll($this->jobKey($jobId));

        if ($raw === false || $raw === []) {
            return null;
        }

        return [
            'state' => $raw['state'] ?? null,
            'workerId' => $raw['workerId'] ?? null,
            'fencingToken' => $raw['fencingToken'] ?? null,
            'attempt' => isset($raw['attempt']) ? (int)$raw['attempt'] : 0,
            'startedAt' => isset($raw['startedAt']) ? (int)$raw['startedAt'] : null,
            'completedAt' => isset($raw['completedAt']) ? (int)$raw['completedAt'] : null,
            'retryAt' => isset($raw['retryAt']) ? (int)$raw['retryAt'] : null,
            'error' => $raw['error'] ?? null,
        ];
    }

    public function findZombies(int $timeoutSeconds): array
    {
        $deadline = time() - $timeoutSeconds;

        $result = $this->redis->zRangeByScore(
            $this->zombieKey(),
            '-inf',
            (string)$deadline
        );

        return $result ?: [];
    }

    public function heartbeat(string $workerId, int $ttlSeconds): void
    {
        $this->redis->set(
            $this->workerAliveKey($workerId),
            (string)time(),
            ['EX' => $ttlSeconds]
        );
    }

    public function forget(string $jobId): void
    {
        $this->redis->del($this->jobKey($jobId));
        $this->redis->del($this->lockKey($jobId));
        $this->redis->zRem($this->zombieKey(), $jobId);
        $this->redis->del($this->retryDispatchKey($jobId));
        $this->redis->del($this->fencingKey($jobId));
    }
}
