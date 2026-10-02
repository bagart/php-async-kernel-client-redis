<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis;

use BAGArt\ASKClientRedis\Exception\ASKRedisConnectionException;

/**
 * Redis DSN value object — uniform way to parse and serialize connection parameters.
 *
 * Supported formats:
 *   tcp://127.0.0.1:6379
 *   tcp://127.0.0.1:6379?timeout=2.0&password=secret&db=3
 *   redis://host:port?...
 */
final readonly class RedisDsn
{
    public const float DEFAULT_TIMEOUT = 2.0;

    public const int DEFAULT_PORT = 6379;

    public function __construct(
        public string $host = '127.0.0.1',
        public int $port = self::DEFAULT_PORT,
        public float $timeout = self::DEFAULT_TIMEOUT,
        public ?string $password = null,
        public int $database = 0,
    ) {
    }

    public static function parse(string $dsn): self
    {
        $parsed = parse_url($dsn);

        if ($parsed === false || !isset($parsed['host'])) {
            throw new ASKRedisConnectionException(
                sprintf('Invalid Redis DSN: %s', $dsn),
            );
        }

        $query = [];
        if (isset($parsed['query'])) {
            parse_str($parsed['query'], $query);
        }

        return new self(
            host: $parsed['host'],
            port: (int)($parsed['port'] ?? self::DEFAULT_PORT),
            timeout: (float)($query['timeout'] ?? self::DEFAULT_TIMEOUT),
            password: $query['password'] ?? null,
            database: (int)($query['db'] ?? 0),
        );
    }

    public function toString(): string
    {
        $qs = http_build_query(array_filter([
            'timeout' => $this->timeout === self::DEFAULT_TIMEOUT ? null : $this->timeout,
            'password' => $this->password,
            'db' => $this->database,
        ], fn ($v) => $v !== null && $v !== '' && $v !== 0 && $v !== 0.0));

        return 'tcp://'.$this->host.':'.$this->port
            .($qs !== '' ? '?'.$qs : '');
    }
}
