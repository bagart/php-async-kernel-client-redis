<?php

declare(strict_types=1);

use BAGArt\ASKClientRedis\Exception\ASKRedisConnectionException;
use BAGArt\ASKClientRedis\Redis\Client\PhpRedisAdapter;
use BAGArt\ASKClientRedis\Redis\RedisDsn;

describe('PhpRedisAdapter::warm reconnect', function () {
    it('retries on connect failure and throws after max attempts', function () {
        $dsn = new RedisDsn(host: '127.0.0.1', port: 19999, timeout: 0.1);

        $adapter = new PhpRedisAdapter($dsn);

        $threw = false;
        try {
            $adapter->warm();
        } catch (ASKRedisConnectionException $e) {
            $threw = true;
            expect($e->getMessage())->toContain('PhpRedis');
        }

        expect($threw)->toBeTrue();
    });

    it('resets redis to null after failed connect', function () {
        $dsn = new RedisDsn(host: '127.0.0.1', port: 19999, timeout: 0.1);

        $adapter = new PhpRedisAdapter($dsn);

        try {
            $adapter->warm();
        } catch (ASKRedisConnectionException) {
        }

        $reflection = new ReflectionClass($adapter);
        $prop = $reflection->getProperty('redis');
        $prop->setAccessible(true);

        expect($prop->getValue($adapter))->toBeNull();
    });
});
