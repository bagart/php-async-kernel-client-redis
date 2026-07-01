<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Operations;

use BAGArt\ASKClientRedis\Contracts\ASKRedisOperationContract;

final readonly class ASKRedisDelOperation implements ASKRedisOperationContract
{
    /**
     * @param  string[]  $keys
     */
    public function __construct(
        private array $keys,
    ) {
    }

    public function command(): string
    {
        return 'DEL';
    }

    public function arguments(): array
    {
        return $this->keys;
    }

}
