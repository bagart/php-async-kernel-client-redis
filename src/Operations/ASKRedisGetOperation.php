<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Operations;

use BAGArt\ASKClientRedis\Contracts\ASKRedisOperationContract;

final readonly class ASKRedisGetOperation implements ASKRedisOperationContract
{
    public function __construct(
        private string $key,
    ) {
    }

    public function command(): string
    {
        return 'GET';
    }

    public function arguments(): array
    {
        return [$this->key];
    }

}
