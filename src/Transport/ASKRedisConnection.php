<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Transport;

use BAGArt\ASKClientRedis\Exception\ASKRedisConnectionException;
use BAGArt\ASKClientRedis\Redis\RedisDsn;

final class ASKRedisConnection
{
    private $socket = null;

    private bool $connected = false;

    private string $host = '127.0.0.1';

    private int $port = 6379;

    public function __construct(
        private readonly string $dsn,
        private readonly int $timeout = 5,
        private readonly bool $persistent = true,
    ) {
        $this->parseDsn($dsn);
    }

    public function connect(): void
    {
        if ($this->connected) {
            return;
        }

        $this->openSocket();
    }

    public function disconnect(): void
    {
        $this->closeSocket();
    }

    public function isConnected(): bool
    {
        return $this->connected && is_resource($this->socket);
    }

    public function write(string $payload): void
    {
        if (!$this->isConnected()) {
            $this->connect();
        }

        $written = @fwrite($this->socket, $payload);

        if ($written === false || $written === 0) {
            throw new ASKRedisConnectionException(
                sprintf('Failed to write to Redis at %s:%d', $this->host, $this->port)
            );
        }
    }

    public function read(): string
    {
        if (!$this->isConnected()) {
            throw new ASKRedisConnectionException('Not connected to Redis');
        }

        $data = @fread($this->socket, 65536);

        if ($data === false) {
            throw new ASKRedisConnectionException(
                sprintf('Failed to read from Redis at %s:%d', $this->host, $this->port)
            );
        }

        return $data;
    }

    public function readLine(): string
    {
        if (!$this->isConnected()) {
            throw new ASKRedisConnectionException('Not connected to Redis');
        }

        $line = @fgets($this->socket);

        if ($line === false) {
            throw new ASKRedisConnectionException(
                sprintf('Failed to read line from Redis at %s:%d', $this->host, $this->port)
            );
        }

        return rtrim($line, "\r\n");
    }

    public function readBytes(int $length): string
    {
        if (!$this->isConnected()) {
            throw new ASKRedisConnectionException('Not connected to Redis');
        }

        $data = '';
        $remaining = $length;

        while ($remaining > 0) {
            $chunk = @fread($this->socket, $remaining);

            if ($chunk === false || $chunk === '') {
                throw new ASKRedisConnectionException(
                    sprintf('Failed to read %d bytes from Redis at %s:%d', $length, $this->host, $this->port)
                );
            }

            $data .= $chunk;
            $remaining -= strlen($chunk);
        }

        return $data;
    }

    private function parseDsn(string $dsn): void
    {
        $parsed = RedisDsn::parse($dsn);

        $this->host = $parsed->host;
        $this->port = $parsed->port;
    }

    private function openSocket(): void
    {
        $address = sprintf('tcp://%s:%d', $this->host, $this->port);
        $errno = 0;
        $errstr = '';

        $this->socket = @fsockopen(
            $address,
            $this->port,
            $errno,
            $errstr,
            (float) $this->timeout
        );

        if (!is_resource($this->socket)) {
            throw new ASKRedisConnectionException(
                sprintf('Cannot connect to Redis at %s:%d — %s (%d)', $this->host, $this->port, $errstr, $errno)
            );
        }

        stream_set_timeout($this->socket, $this->timeout);
        $this->connected = true;
    }

    private function closeSocket(): void
    {
        if (is_resource($this->socket)) {
            @fclose($this->socket);
        }

        $this->socket = null;
        $this->connected = false;
    }
}
