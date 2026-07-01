<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Cache;

use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;
use BAGArt\AsyncKernel\Cache\ASKCacheSimpleReuseMethodsTrait;
use BAGArt\AsyncKernel\Contracts\ASKClockContract;
use BAGArt\AsyncKernel\Contracts\Cache\ASKCacheContract;
use DateInterval;

final class RedisCache implements ASKCacheContract
{
    use ASKCacheSimpleReuseMethodsTrait;

    public const string TYPE = 'redis';

    public function __construct(
        private readonly RedisClientContract $redis,
        private readonly ASKClockContract $clock,
    ) {
    }

    protected function clock(): ASKClockContract
    {
        return $this->clock;
    }

    public function increment($key, $value = 1): int|bool
    {
        return $this->redis->incrBy($key, (int) $value);
    }

    public function decrement($key, $value = 1): int|bool
    {
        return $this->redis->decrBy($key, (int) $value);
    }

    public function get(string $key, mixed $default = null): mixed
    {
        $value = $this->redis->get($key);

        if ($value === false) {
            return $default;
        }

        return unserialize($value, ['allowed_classes' => []]);
    }

    public function set(string $key, mixed $value, DateInterval|int|null $ttl = null): bool
    {
        $serialized = serialize($value);
        $seconds = $this->resolveTtlSeconds($ttl);

        return $seconds > 0
            ? $this->redis->setex($key, $seconds, $serialized)
            : $this->redis->set($key, $serialized);
    }

    public function getMultiple(iterable $keys, mixed $default = null): iterable
    {
        $keysArray = is_array($keys) ? $keys : iterator_to_array($keys);
        $values = $this->redis->mget($keysArray);

        $result = [];
        foreach ($keysArray as $index => $key) {
            $val = $values[$index];
            $result[$key] = ($val === false)
                ? $default
                : unserialize($val, ['allowed_classes' => []]);
        }
        return $result;
    }

    public function setMultiple(iterable $values, DateInterval|int|null $ttl = null): bool
    {
        $data = [];
        foreach ($values as $key => $value) {
            $data[$key] = serialize($value);
        }

        $success = $this->redis->mset($data);

        if ($success && $ttl !== null) {
            $seconds = $this->resolveTtlSeconds($ttl);
            $pipe = $this->redis->pipeline();
            foreach (array_keys($data) as $key) {
                $pipe->expire($key, $seconds);
            }
            $pipe->exec();
        }

        return (bool)$success;
    }

    public function delete(string $key): bool
    {
        return (bool)$this->redis->del($key);
    }

    public function deleteMultiple(iterable $keys): bool
    {
        $keysArray = is_array($keys) ? $keys : iterator_to_array($keys);
        return (bool)$this->redis->del($keysArray);
    }

    public function has(string $key): bool
    {
        return (bool)$this->redis->exists($key);
    }

    public function clear(): bool
    {
        return $this->redis->flushDB();
    }

    private function resolveTtlSeconds(DateInterval|int|null $ttl): int
    {
        if ($ttl === null) {
            return 0;
        }

        return ($ttl instanceof DateInterval)
            ? $this->clock->getSecondsFromInterval($ttl)
            : $ttl;
    }
}
