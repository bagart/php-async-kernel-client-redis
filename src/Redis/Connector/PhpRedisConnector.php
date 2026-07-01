<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis\Connector;

use BAGArt\ASKClientRedis\Exception\ASKRedisConnectionException;
use BAGArt\ASKClientRedis\Redis\Contract\RedisConnectorContract;
use BAGArt\ASKClientRedis\Redis\RedisDsn;
use Redis;

/**
 * Connects to Redis using the phpredis extension.
 */
final class PhpRedisConnector implements RedisConnectorContract
{
    public function connect(RedisDsn $dsn): Redis
    {
        $redis = new Redis();
        $connected = $redis->connect(
            host: $dsn->host,
            port: $dsn->port,
            timeout: $dsn->timeout,
        );

        if (!$connected) {
            throw new ASKRedisConnectionException(
                sprintf('Redis connection failed: %s:%d', $dsn->host, $dsn->port),
            );
        }

        if ($dsn->password !== null) {
            $redis->auth($dsn->password);
        }

        if ($dsn->database !== 0) {
            $redis->select($dsn->database);
        }

        return $redis;
    }
}
