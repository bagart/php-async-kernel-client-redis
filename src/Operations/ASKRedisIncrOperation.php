<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Operations;

use BAGArt\ASKClientRedis\Contracts\ASKRedisOperationContract;

final readonly class ASKRedisIncrOperation implements ASKRedisOperationContract
{
    public function __construct(
        private string $key,
        private int $by = 1,
    ) {
    }

    public function command(): string
    {
        return $this->by === 1 ? 'INCR' : 'INCRBY';
    }

    public function arguments(): array
    {
        if ($this->by === 1) {
            return [$this->key];
        }

        return [$this->key, (string) $this->by];
    }

}
