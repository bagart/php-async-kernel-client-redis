<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Connection;

use BAGArt\ASKClientRedis\Exception\ASKRedisConnectionException;
use BAGArt\AsyncKernel\Contracts\ASKSocketSchedulerContract;
use Fiber;

final class FiberRedisConnection
{
    private const int READ_BUF_SIZE = 65536;

    /**
     * @var resource|null
     */
    private $socket = null;

    private bool $connected = false;

    public function __construct(
        private readonly ASKSocketSchedulerContract $scheduler,
        private readonly string $host,
        private readonly int $port,
        private readonly float $timeout = 5.0,
    ) {
    }

    public function connect(): void
    {
        if ($this->connected) {
            return;
        }

        $errno = 0;
        $errstr = '';

        $socket = @stream_socket_client(
            sprintf('tcp://%s:%d', $this->host, $this->port),
            $errno,
            $errstr,
            $this->timeout,
            STREAM_CLIENT_CONNECT,
        );

        if (!is_resource($socket)) {
            throw new ASKRedisConnectionException(
                sprintf('Connection failed: %s (%d)', $errstr, $errno),
            );
        }

        stream_set_blocking($socket, false);

        $this->socket = $socket;
        $this->connected = true;
    }

    public function disconnect(): void
    {
        if (is_resource($this->socket)) {
            $this->scheduler->unwatchRead($this->socket);
            $this->scheduler->unwatchWrite($this->socket);
            @fclose($this->socket);
        }

        $this->socket = null;
        $this->connected = false;
    }

    public function isConnected(): bool
    {
        return $this->connected && is_resource($this->socket);
    }

    public function write(string $data): void
    {
        $total = strlen($data);
        $written = 0;

        while ($written < $total) {
            if (!is_resource($this->socket)) {
                throw new ASKRedisConnectionException('Socket is closed');
            }

            $chunk = substr($data, $written);
            $result = @fwrite($this->socket, $chunk);

            if ($result === false) {
                $this->disconnect();

                throw new ASKRedisConnectionException('Socket write error');
            }

            if ($result > 0) {
                $written += $result;

                continue;
            }

            // OS send buffer full — wait for write readiness
            $this->scheduler->watchWrite($this->socket);
            Fiber::suspend();
            $this->scheduler->unwatchWrite($this->socket);
        }
    }

    public function read(): string
    {
        while (true) {
            if (!is_resource($this->socket)) {
                throw new ASKRedisConnectionException('Socket is closed');
            }

            if (feof($this->socket)) {
                $this->disconnect();

                throw new ASKRedisConnectionException('Connection closed by peer');
            }

            $data = @fread($this->socket, self::READ_BUF_SIZE);

            if ($data === false) {
                $this->disconnect();

                throw new ASKRedisConnectionException('Socket read error');
            }

            if ($data !== '') {
                $this->scheduler->unwatchRead($this->socket);

                return $data;
            }

            $this->scheduler->watchRead($this->socket);
            Fiber::suspend();
            $this->scheduler->unwatchRead($this->socket);
        }
    }

    public function __destruct()
    {
        $this->disconnect();
    }
}
