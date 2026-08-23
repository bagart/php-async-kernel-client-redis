<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Contracts;

/**
 * Typed handler for incoming Redis Pub/Sub messages. Implementations are
 * named classes so subscription behavior is visible in traces and to the
 * type system instead of a raw callable.
 */
interface ASKRedisMessageHandlerContract
{
    public function handle(string $channel, string $payload): void;
}
