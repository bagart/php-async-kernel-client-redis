<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\PubSub;

use BAGArt\ASKClient\Client\ASKFuture;
use BAGArt\ASKClient\Contracts\Client\ASKClientContract;
use BAGArt\ASKClient\Contracts\Pipeline\ASKFutureContract;
use BAGArt\ASKClient\Contracts\Pipeline\FutureProducerContract;
use BAGArt\ASKClientRedis\Contracts\ASKRedisMessageHandlerContract;
use BAGArt\ASKClientRedis\Contracts\ASKRedisSubscriberContract;
use BAGArt\ASKClientRedis\Exception\ASKRedisException;
use BAGArt\ASKClientRedis\Operations\ASKRedisSubscribeOperation;
use BAGArt\ASKClientRedis\Transport\ASKRedisTransportAdapter;

final class ASKRedisSubscriber implements ASKRedisSubscriberContract, FutureProducerContract
{
    private bool $running = false;

    private ?ASKRedisMessageHandlerContract $handler = null;

    public function __construct(
        private readonly ASKClientContract $client,
        private readonly ASKRedisTransportAdapter $adapter,
        private readonly string $channel,
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

        $this->client->execute(new ASKRedisSubscribeOperation($this->channel));

        $this->running = true;

        // The subscriber itself is the lazy producer: await() drives listenLoop().
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
            $message = $this->adapter->readPubSubMessage();

            if ($message !== null) {
                $handler->handle($message['channel'], $message['payload']);
            }
        }
    }
}
