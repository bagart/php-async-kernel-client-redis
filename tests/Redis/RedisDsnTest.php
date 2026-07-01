<?php

declare(strict_types=1);

use BAGArt\ASKClientRedis\Exception\ASKRedisConnectionException;
use BAGArt\ASKClientRedis\Redis\RedisDsn;

describe('RedisDsn::parse', function () {
    it('parses tcp://host:port', function () {
        $dsn = RedisDsn::parse('tcp://127.0.0.1:6379');

        expect($dsn->host)->toBe('127.0.0.1')
            ->and($dsn->port)->toBe(6379)
            ->and($dsn->timeout)->toBe(2.0)
            ->and($dsn->password)->toBeNull()
            ->and($dsn->database)->toBe(0);
    });

    it('parses redis://host:port with query params', function () {
        $dsn = RedisDsn::parse('redis://redis.local:6380?timeout=5.0&password=secret&db=3');

        expect($dsn->host)->toBe('redis.local')
            ->and($dsn->port)->toBe(6380)
            ->and($dsn->timeout)->toBe(5.0)
            ->and($dsn->password)->toBe('secret')
            ->and($dsn->database)->toBe(3);
    });

    it('uses default port when omitted', function () {
        $dsn = RedisDsn::parse('tcp://localhost');

        expect($dsn->host)->toBe('localhost')
            ->and($dsn->port)->toBe(6379);
    });

    it('throws on invalid DSN', function () {
        RedisDsn::parse('not-a-dsn');
    })->throws(ASKRedisConnectionException::class);

    it('throws on empty string', function () {
        RedisDsn::parse('');
    })->throws(ASKRedisConnectionException::class);
});

describe('RedisDsn::toString', function () {
    it('serializes with host and port', function () {
        $dsn = new RedisDsn('127.0.0.1', 6379);

        expect($dsn->toString())->toBe('tcp://127.0.0.1:6379');
    });

    it('includes non-default query params', function () {
        $dsn = new RedisDsn('redis.local', 6380, 5.0, 'secret', 3);

        $str = $dsn->toString();

        expect($str)->toContain('tcp://redis.local:6380')
            ->and($str)->toContain('timeout=5')
            ->and($str)->toContain('password=secret')
            ->and($str)->toContain('db=3');
    });

    it('round-trips through parse', function () {
        $dsn = new RedisDsn('redis.local', 6380, 5.0, 'secret', 3);
        $parsed = RedisDsn::parse($dsn->toString());

        expect($parsed->host)->toBe($dsn->host)
            ->and($parsed->port)->toBe($dsn->port)
            ->and($parsed->timeout)->toBe($dsn->timeout)
            ->and($parsed->password)->toBe($dsn->password)
            ->and($parsed->database)->toBe($dsn->database);
    });

    it('omits default values from query string', function () {
        $dsn = new RedisDsn('127.0.0.1', 6379, 2.0, null, 0);

        expect($dsn->toString())->toBe('tcp://127.0.0.1:6379');
    });
});
