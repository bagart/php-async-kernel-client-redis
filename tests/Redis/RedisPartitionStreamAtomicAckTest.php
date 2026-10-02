<?php

declare(strict_types=1);

use BAGArt\AsyncKernel\Job\AsyncJob;
use BAGArt\AskQueue\Contracts\JobSerializerContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;
use BAGArt\ASKClientRedis\Redis\Contract\RedisPipelineContract;
use BAGArt\ASKClientRedis\Redis\RedisPartitionStream;

class FakeRedisForPartitionStream implements RedisClientContract
{
    /** @var array<string, string> */
    public array $strings = [];

    /** @var array<string, array<string, string>> */
    public array $hashes = [];

    /** @var array<string, list<array{id: string, fields: array<string, string>}>> */
    public array $streams = [];

    /** @var array<string, int> */
    public array $ttls = [];

    /** @var int */
    public int $evalCount = 0;

    /** @var array<int, array{script: string, args: array, numKeys: int}> */
    public array $evalCalls = [];

    public function get(string $key): string|false
    {
        return $this->strings[$key] ?? false;
    }

    public function set(string $key, mixed $value, mixed ...$options): RedisClientContract|bool
    {
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
            unset($this->hashes[$k], $this->streams[$k]);
        }
        return $count;
    }

    public function exists(string $key, string ...$other_keys): int|false
    {
        return 0;
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
        $this->ttls[$key] = $seconds;
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
        return $this->hashes[$key] ?? false;
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
        foreach ($values as $v) {
            $this->strings[$key.':'.(string)$v] = '1';
        }
        return count($values);
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
        return 1;
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
        $entryId = $id === '*' ? '1-0' : $id;
        $this->streams[$key][] = ['id' => $entryId, 'fields' => $fields];
        return $entryId;
    }

    public function xRead(array $streams, int $count = -1, int $block = 0): array|false
    {
        return [];
    }

    public function xDel(string $key, string ...$ids): int|false
    {
        if (!isset($this->streams[$key])) {
            return 0;
        }
        $count = 0;
        foreach ($ids as $id) {
            foreach ($this->streams[$key] as $i => $entry) {
                if ($entry['id'] === $id) {
                    unset($this->streams[$key][$i]);
                    $count++;
                }
            }
        }
        return $count;
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
        return count($this->streams[$key] ?? []);
    }
    public function scan(?int &$iterator, ?string $pattern = null, int $count = 0): array|false
    {
        return [];
    }
    public function pipeline(): RedisPipelineContract
    {
        throw new \LogicException('Not implemented');
    }

    public function eval(string $script, array $args = [], int $numKeys = 0): mixed
    {
        $this->evalCount++;
        $this->evalCalls[] = ['script' => $script, 'args' => $args, 'numKeys' => $numKeys];

        $streamKey = $args[0] ?? '';
        $ownershipKey = $args[1] ?? '';
        $entryId = $args[2] ?? '';
        $workerId = $args[3] ?? '';

        if (str_contains($script, 'XDEL')) {
            if ($workerId !== '') {
                $storedWorker = $this->hashes[$ownershipKey]['workerId'] ?? '';
                if ($storedWorker !== $workerId) {
                    return 0;
                }
            }

            if (isset($this->streams[$streamKey])) {
                foreach ($this->streams[$streamKey] as $i => $entry) {
                    if ($entry['id'] === $entryId) {
                        unset($this->streams[$streamKey][$i]);
                        break;
                    }
                }
            }

            // LUA_ACK expires the ownership key (EXPIRE 1) instead of DELeting it
            $this->ttls[$ownershipKey] = 1;

            return 1;
        }

        return 0;
    }
}

class FakeSerializer implements JobSerializerContract
{
    public function serialize(AsyncJob $job): string
    {
        return json_encode($job);
    }

    public function deserialize(string $payload): AsyncJob
    {
        return json_decode($payload);
    }

    public function serializeToMeta(AsyncJob $job): string
    {
        return json_encode($job);
    }

    public function deserializeFromMeta(string $jobId, array $meta): ?AsyncJob
    {
        return json_decode($meta['payload'] ?? '{}');
    }

    public function serializeToRecoveryPayload(AsyncJob $job, string $fencingToken = ''): string
    {
        return json_encode($job);
    }
}

describe('RedisPartitionStream atomic ack', function () {
    beforeEach(function () {
        $this->fake = new FakeRedisForPartitionStream();
        $this->stream = new RedisPartitionStream($this->fake, new FakeSerializer(), prefix: 'TEST:');
    });

    it('ack uses a single Lua eval call', function () {
        $this->stream->ack('partition-A', '1-0');

        expect($this->fake->evalCount)->toBe(1);
    });

    it('ack without workerId deletes stream entry and expires ownership key atomically', function () {
        $streamKey = 'TEST:{partition-A}partition:partition-A';
        $ownershipKey = 'TEST:{partition-A}stream:ownership:partition-A:1-0';

        $this->fake->streams[$streamKey][] = ['id' => '1-0', 'fields' => ['jobId' => 'job-1']];
        $this->fake->hashes[$ownershipKey] = ['workerId' => 'w-1', 'fencingToken' => 'tok'];

        $this->stream->ack('partition-A', '1-0');

        expect($this->fake->evalCount)->toBe(1);
        expect(array_column($this->fake->streams[$streamKey] ?? [], 'id'))->not->toContain('1-0');
        expect($this->fake->hashes[$ownershipKey])->toHaveKey('workerId')
            ->and($this->fake->ttls[$ownershipKey])->toBe(1);
    });

    it('ack with matching workerId deletes stream entry and expires ownership key', function () {
        $streamKey = 'TEST:{partition-B}partition:partition-B';
        $ownershipKey = 'TEST:{partition-B}stream:ownership:partition-B:2-0';

        $this->fake->streams[$streamKey][] = ['id' => '2-0', 'fields' => ['jobId' => 'job-2']];
        $this->fake->hashes[$ownershipKey] = ['workerId' => 'w-2', 'fencingToken' => ''];

        $this->stream->ack('partition-B', '2-0', 'w-2');

        expect($this->fake->evalCount)->toBe(1);
        expect(array_column($this->fake->streams[$streamKey] ?? [], 'id'))->not->toContain('2-0');
        expect($this->fake->hashes[$ownershipKey])->toHaveKey('workerId')
            ->and($this->fake->ttls[$ownershipKey])->toBe(1);
    });

    it('ack with wrong workerId does not delete anything', function () {
        $streamKey = 'TEST:{partition-C}partition:partition-C';
        $ownershipKey = 'TEST:{partition-C}stream:ownership:partition-C:3-0';

        $this->fake->streams[$streamKey][] = ['id' => '3-0', 'fields' => ['jobId' => 'job-3']];
        $this->fake->hashes[$ownershipKey] = ['workerId' => 'w-correct', 'fencingToken' => ''];

        $this->stream->ack('partition-C', '3-0', 'w-wrong');

        expect($this->fake->evalCount)->toBe(1);
        expect(array_column($this->fake->streams[$streamKey], 'id'))->toContain('3-0');
        expect($this->fake->hashes[$ownershipKey])->toHaveKey('workerId')
            ->and($this->fake->ttls)->not->toHaveKey($ownershipKey);
    });

    it('ack with null workerId (no ownership check) deletes entry atomically', function () {
        $streamKey = 'TEST:{partition-D}partition:partition-D';

        $this->fake->streams[$streamKey][] = ['id' => '4-0', 'fields' => ['jobId' => 'job-4']];

        $this->stream->ack('partition-D', '4-0', null);

        expect($this->fake->evalCount)->toBe(1);
        expect($this->fake->streams[$streamKey])->toBeEmpty();
    });

    it('LUA_ACK script deletes the stream entry and expires the ownership key', function () {
        $this->stream->ack('partition-E', '5-0');

        expect($this->fake->evalCalls[0]['script'])->toContain('XDEL')
            ->and($this->fake->evalCalls[0]['script'])->toContain('EXPIRE')
            ->and($this->fake->evalCalls[0]['numKeys'])->toBe(2)
            ->and($this->fake->evalCalls[0]['args'])->toHaveCount(4);
    });
});
