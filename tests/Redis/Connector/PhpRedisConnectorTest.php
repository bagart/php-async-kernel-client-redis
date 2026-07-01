<?php

declare(strict_types=1);

use BAGArt\ASKClientRedis\Redis\Contract\RedisConnectorContract;
use BAGArt\ASKClientRedis\Redis\Connector\PhpRedisConnector;
use BAGArt\ASKClientRedis\Redis\RedisDsn;

describe('PhpRedisConnector', function () {
    it('implements RedisConnectorInterface', function () {
        expect(PhpRedisConnector::class)
            ->toImplement(RedisConnectorContract::class);
    });

    it('connects to Redis and returns a Redis instance', function () {
        $connector = new PhpRedisConnector();
        $dsn = new RedisDsn('127.0.0.1', 6379, 2.0);

        $redis = $connector->connect($dsn);

        expect($redis)->toBeInstanceOf(\Redis::class);

        $redis->close();
    })->skipIf(!extension_loaded('redis'), 'ext-redis is not loaded');
});
