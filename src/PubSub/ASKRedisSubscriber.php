<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\PubSub;

use BAGArt\ASKClient\ASKFuture;
use BAGArt\ASKClient\Contracts\ASKFutureContract;
use BAGArt\ASKClient\Contracts\Client\ASKClientContract;
use BAGArt\ASKClientRedis\Contracts\ASKRedisSubscriberContract;
use BAGArt\ASKClientRedis\Exception\ASKRedisException;
use BAGArt\ASKClientRedis\Operations\ASKRedisSubscribeOperation;
use BAGArt\ASKClientRedis\Transport\ASKRedisTransportAdapter;

final class ASKRedisSubscriber implements ASKRedisSubscriberContract
{
    private bool $running = false;

    /** @var callable|null */
    private $handler = null;

    public function __construct(
        private readonly ASKClientContract $client,
        private readonly ASKRedisTransportAdapter $adapter,
        private readonly string $channel,
    ) {
    }

    public function onMessage(callable $handler): self
    {
        $this->handler = $handler;

        return $this;
    }

    public function start(): ASKFutureContract
    {
        if ($this->handler === null) {
            throw new ASKRedisException('No message handler registered. Call onMessage() first.');
        }

        $this->client->execute(new ASKRedisSubscribeOperation($this->channel));

        $this->running = true;

        return ASKFuture::pending(function (): void {
            $this->listenLoop($this->handler);
        });
    }

    public function stop(): void
    {
        $this->running = false;
    }

    private function listenLoop(callable $handler): void
    {
        while ($this->running) {
            $message = $this->adapter->readPubSubMessage();

            if ($message !== null) {
                ($handler)($message['channel'], $message['payload']);
            }
        }
    }
}
