<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Transport;

use BAGArt\ASKClient\ASKFuture;
use BAGArt\ASKClient\Contracts\ASKContextContract;
use BAGArt\ASKClient\Contracts\ASKFutureContract;
use BAGArt\ASKClient\Contracts\Transporting\ASKTransportContract;
use BAGArt\ASKClientRedis\Contracts\ASKRedisOperationContract;
use BAGArt\ASKClientRedis\Contracts\ASKRedisTransportContract;
use BAGArt\ASKClientRedis\Operations\ASKRedisPublishOperation;
use BAGArt\ASKClientRedis\Operations\ASKRedisSubscribeOperation;
use BAGArt\ASKClientRedis\Pipeline\ASKRedisPipeline;
use BAGArt\AsyncKernel\Contracts\ASKPromiseContract;

/**
 * Infrastructure adapter that bridges ASKTransportContract → ASKRedisTransportContract.
 *
 * Registered inside ASKClient's transport registry as "redis://".
 * Handles dispatching different operation types to the underlying Redis transport.
 */
final class ASKRedisTransportAdapter implements ASKTransportContract
{
    public function __construct(
        private readonly ASKRedisTransportContract $redisTransport,
    ) {
    }

    public function execute(
        object $operation,
        ASKContextContract $context,
    ): ASKFutureContract {
        return match (true) {
            $operation instanceof ASKRedisPipeline =>
                $this->fromPromise($this->redisTransport->executeBatch($operation->operations())),
            $operation instanceof ASKRedisPublishOperation =>
                $this->fromPromise($this->redisTransport->publish($operation->channel(), $operation->payload())),
            $operation instanceof ASKRedisSubscribeOperation =>
                $this->fromPromise($this->redisTransport->subscribe($operation->channel())),
            $operation instanceof ASKRedisOperationContract =>
                $this->fromPromise($this->redisTransport->execute($operation)),
            default => throw new \InvalidArgumentException(
                sprintf('Unsupported Redis operation: %s', get_debug_type($operation)),
            ),
        };
    }

    public function readPubSubMessage(): ?array
    {
        return $this->redisTransport->readPubSubMessage();
    }

    private function fromPromise(ASKPromiseContract $promise): ASKFutureContract
    {
        return ASKFuture::pending(function () use ($promise) {
            return $promise->await(true);
        });
    }
}
