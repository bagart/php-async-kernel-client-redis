<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\PubSub;

use BAGArt\ASKClient\Client\ASKFuture;
use BAGArt\ASKClient\Contracts\Client\ASKClientContract;
use BAGArt\ASKClient\Contracts\Pipeline\ASKFutureContract;
use BAGArt\ASKClient\Contracts\Pipeline\FutureProducerContract;
use BAGArt\ASKClientRedis\Contracts\ASKRedisMessageHandlerContract;
use BAGArt\ASKClientRedis\Contracts\ASKRedisSubscriberContract;
use BAGArt\ASKClientRedis\Contracts\PubSubReadableContract;
use BAGArt\ASKClientRedis\Exception\ASKRedisException;
use BAGArt\ASKClientRedis\Operations\ASKRedisSubscribeOperation;

final class ASKRedisSubscriber implements ASKRedisSubscriberContract, FutureProducerContract
{
    public const int BASE_BACKOFF_MS = 50;

    public const int MAX_BACKOFF_MS = 8000;

    public const int MAX_CONSECUTIVE_FAILURES = 30;

    private bool $running = false;

    private ?ASKRedisMessageHandlerContract $handler = null;

    private int $consecutiveFailures = 0;

    public function __construct(
        private readonly ASKClientContract $client,
        private readonly PubSubReadableContract $adapter,
        private readonly string $channel,
        private readonly int $baseBackoffMs = self::BASE_BACKOFF_MS,
        private readonly int $maxBackoffMs = self::MAX_BACKOFF_MS,
        private readonly int $maxConsecutiveFailures = self::MAX_CONSECUTIVE_FAILURES,
    ) {
    }

    public function onMessage(ASKRedisMessageHandlerContract $handler): self
    {
        $this->handler = $handler;

        return $this;
    }

    public function start(): ASKFutureContract
    {
        if ($this->handler === null) {
            throw new ASKRedisException('No message handler registered. Call onMessage() first.');
        }

        $this->subscribe();

        $this->running = true;

        return ASKFuture::pending($this);
    }

    public function stop(): void
    {
        $this->running = false;
    }

    public function produce(): mixed
    {
        $this->listenLoop();

        return null;
    }

    private function listenLoop(): void
    {
        $handler = $this->handler;
        if ($handler === null) {
            throw new ASKRedisException('No message handler registered. Call onMessage() first.');
        }

        while ($this->running) {
            try {
                $message = $this->adapter->readPubSubMessage();

                if ($message !== null) {
                    $handler->handle($message['channel'], $message['payload']);
                    $this->consecutiveFailures = 0;

                    continue;
                }

                $this->consecutiveFailures++;
                $this->backoff();
                $this->subscribe();
            } catch (\Throwable) {
                $this->consecutiveFailures++;

                if ($this->consecutiveFailures >= $this->maxConsecutiveFailures) {
                    $this->running = false;

                    throw new ASKRedisException(
                        sprintf('Subscriber disconnected after %d consecutive failures.', $this->maxConsecutiveFailures),
                    );
                }

                $this->backoff();
                $this->subscribe();
            }
        }
    }

    private function subscribe(): void
    {
        $this->client->execute(new ASKRedisSubscribeOperation($this->channel));
    }

    private function backoff(): void
    {
        $delayMs = min(
            $this->baseBackoffMs * (2 ** $this->consecutiveFailures),
            $this->maxBackoffMs,
        );

        usleep($delayMs * 1000);
    }
}
