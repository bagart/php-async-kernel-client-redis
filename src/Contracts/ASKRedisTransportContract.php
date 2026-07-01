<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Contracts;

use BAGArt\AsyncKernel\Contracts\ASKPromiseContract;

interface ASKRedisTransportContract
{
    public function execute(ASKRedisOperationContract $operation): ASKPromiseContract;

    /**
     * @param  ASKRedisOperationContract[]  $operations
     */
    public function executeBatch(array $operations): ASKPromiseContract;

    public function subscribe(string $channel): ASKPromiseContract;

    public function publish(string $channel, mixed $payload): ASKPromiseContract;

    /**
     * Read the next pub/sub message from the transport.
     *
     * @return array{channel: string, payload: string}|null
     */
    public function readPubSubMessage(): ?array;
}
