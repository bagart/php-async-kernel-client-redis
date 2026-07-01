<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Operations;

/**
 * DTO for Redis PUBLISH command.
 */
final readonly class ASKRedisPublishOperation
{
    public function __construct(
        private string $channel,
        private mixed $payload,
    ) {
    }

    public function channel(): string
    {
        return $this->channel;
    }

    public function payload(): mixed
    {
        return $this->payload;
    }
}
