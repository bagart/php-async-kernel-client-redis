<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Operations;

/**
 * DTO for Redis SUBSCRIBE command.
 */
final readonly class ASKRedisSubscribeOperation
{
    public function __construct(
        private string $channel,
    ) {
    }

    public function channel(): string
    {
        return $this->channel;
    }
}
