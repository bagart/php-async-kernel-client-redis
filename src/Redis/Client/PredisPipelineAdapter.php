<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis\Client;

use BAGArt\ASKClientRedis\Redis\Contract\RedisPipelineContract;
use Predis\Pipeline\Pipeline;

final class PredisPipelineAdapter implements RedisPipelineContract
{
    public function __construct(
        private readonly Pipeline $pipeline,
    ) {
    }

    public function __call(string $name, array $arguments): mixed
    {
        return $this->pipeline->{$name}(...$arguments);
    }

    public function exec(): array|false
    {
        return $this->pipeline->execute();
    }
}
