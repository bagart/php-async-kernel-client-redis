<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Operations;

use BAGArt\ASKClientRedis\Contracts\ASKRedisOperationContract;

final readonly class ASKRedisExpireOperation implements ASKRedisOperationContract
{
    public function __construct(
        private string $key,
        private int $seconds,
    ) {
    }

    public function command(): string
    {
        return 'EXPIRE';
    }

    public function arguments(): array
    {
        return [$this->key, (string) $this->seconds];
    }

}
