<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Contracts;

interface ASKRedisOperationContract
{
    public function command(): string;

    public function arguments(): array;
}
