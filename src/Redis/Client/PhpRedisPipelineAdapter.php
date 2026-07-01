<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis\Client;

use BAGArt\ASKClientRedis\Redis\Contract\RedisPipelineContract;

/**
 * Thin wrapper over a phpredis pipeline handle.
 *
 * Phpredis `pipeline()` returns a `\Redis` instance operating in pipeline mode.
 * This class delegates arbitrary method calls and `exec()` to it.
 */
final class PhpRedisPipelineAdapter implements RedisPipelineContract
{
    public function __construct(
        private readonly mixed $pipeline,
    ) {
    }

    public function __call(string $name, array $arguments): mixed
    {
        return $this->pipeline->{$name}(...$arguments);
    }

    public function exec(): array|false
    {
        return $this->pipeline->exec();
    }
}
