<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Transport;

use BAGArt\AsyncKernel\Contracts\ASKPromiseContract;
use BAGArt\AsyncKernel\Promise\ASKDeferred;
use BAGArt\AsyncKernel\Wrappers\ASKLogWrapper;
use BAGArt\ASKClientRedis\Contracts\ASKRedisOperationContract;
use BAGArt\ASKClientRedis\Contracts\ASKRedisTransportContract;
use BAGArt\ASKClientRedis\Exception\ASKRedisConnectionException;
use BAGArt\ASKClientRedis\Exception\ASKRedisException;

final class ASKRedisTransport implements ASKRedisTransportContract
{
    private readonly ASKRedisProtocolEncoder $encoder;

    private readonly ASKRedisProtocolDecoder $decoder;

    public function __construct(
        private readonly ASKRedisConnection $connection,
        ?ASKRedisProtocolEncoder $encoder = null,
        ?ASKRedisProtocolDecoder $decoder = null,
        private readonly ?ASKLogWrapper $logger = null,
    ) {
        $this->encoder = $encoder ?? new ASKRedisProtocolEncoder();
        $this->decoder = $decoder ?? new ASKRedisProtocolDecoder();
    }

    public function execute(ASKRedisOperationContract $operation): ASKPromiseContract
    {
        $deferred = new ASKDeferred();

        try {
            $this->connection->connect();

            $payload = $this->encoder->encodeCommand(
                $operation->command(),
                $operation->arguments()
            );

            $this->sendRaw($payload);

            $response = $this->readResponse();

            if ($response instanceof ASKRedisException) {
                $deferred->reject($response);
            } else {
                $deferred->resolve($response);
            }
        } catch (\Throwable $e) {
            $deferred->reject($e);
        }

        return $deferred->promise();
    }

    /**
     * @param  ASKRedisOperationContract[]  $operations
     */
    public function executeBatch(array $operations): ASKPromiseContract
    {
        $deferred = new ASKDeferred();

        try {
            $this->connection->connect();

            $commands = array_map(
                static fn (ASKRedisOperationContract $op) => [$op->command(), $op->arguments()],
                $operations
            );

            $payload = $this->encoder->encodePipeline($commands);
            $this->sendRaw($payload);

            $results = [];
            for ($i = 0; $i < count($operations); $i++) {
                $results[] = $this->readResponse();
            }

            $deferred->resolve($results);
        } catch (\Throwable $e) {
            $deferred->reject($e);
        }

        return $deferred->promise();
    }

    public function subscribe(string $channel): ASKPromiseContract
    {
        $deferred = new ASKDeferred();

        try {
            $this->connection->connect();

            $payload = $this->encoder->encodeCommand('SUBSCRIBE', [$channel]);
            $this->sendRaw($payload);

            $confirmation = $this->readResponse();

            if ($confirmation instanceof ASKRedisException) {
                $deferred->reject($confirmation);
            } else {
                $deferred->resolve(true);
            }
        } catch (\Throwable $e) {
            $deferred->reject($e);
        }

        return $deferred->promise();
    }

    public function publish(string $channel, mixed $payload): ASKPromiseContract
    {
        $deferred = new ASKDeferred();

        try {
            $this->connection->connect();

            $value = is_string($payload) ? $payload : json_encode($payload, JSON_THROW_ON_ERROR);
            $redisPayload = $this->encoder->encodeCommand('PUBLISH', [$channel, $value]);
            $this->sendRaw($redisPayload);

            $response = $this->readResponse();

            if ($response instanceof ASKRedisException) {
                $deferred->reject($response);
            } else {
                $deferred->resolve($response);
            }
        } catch (\Throwable $e) {
            $deferred->reject($e);
        }

        return $deferred->promise();
    }

    public function readPubSubMessage(): ?array
    {
        if (!$this->connection->isConnected()) {
            return null;
        }

        try {
            $response = $this->readResponse();

            if (!is_array($response) || count($response) < 3) {
                return null;
            }

            $type = $response[0];

            if (!is_string($type) || $type !== 'message') {
                return null;
            }

            return [
                'channel' => (string) $response[1],
                'payload' => (string) $response[2],
            ];
        } catch (\Throwable) {
            return null;
        }
    }

    private function sendRaw(string $payload): void
    {
        $this->connection->write($payload);
        $this->logger?->debug('[ASKRedis] sent', ['bytes' => strlen($payload)]);
    }

    private function readResponse(): mixed
    {
        $maxAttempts = 100;

        for ($attempt = 0; $attempt < $maxAttempts; $attempt++) {
            if ($this->decoder->hasCompleteResponse()) {
                return $this->decoder->decode();
            }

            $chunk = $this->connection->read();

            if ($chunk === '') {
                throw new ASKRedisConnectionException('Empty response from Redis');
            }

            $this->decoder->feed($chunk);
        }

        throw new ASKRedisException('Max read attempts exceeded');
    }
}
