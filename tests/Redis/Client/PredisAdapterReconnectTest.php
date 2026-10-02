<?php

declare(strict_types=1);

use BAGArt\ASKClientRedis\Exception\ASKRedisConnectionException;
use BAGArt\ASKClientRedis\Redis\Client\PredisAdapter;
use BAGArt\ASKClientRedis\Redis\RedisDsn;

describe('PredisAdapter::warm reconnect', function () {
    beforeEach(function () {
        if (!class_exists(\Predis\Client::class)) {
            $this->markTestSkipped('Predis not installed');
        }
    });

    it('retries on connect failure and throws after max attempts', function () {
        $dsn = new RedisDsn(host: '127.0.0.1', port: 19999, timeout: 0.1);

        $adapter = new PredisAdapter($dsn);

        $threw = false;
        try {
            $adapter->warm();
        } catch (ASKRedisConnectionException $e) {
            $threw = true;
            expect($e->getMessage())->toContain('Predis');
        }

        expect($threw)->toBeTrue();
    });

    it('resets redis to null after failed connect', function () {
        $dsn = new RedisDsn(host: '127.0.0.1', port: 19999, timeout: 0.1);

        $adapter = new PredisAdapter($dsn);

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
