<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis\Contract;

/**
 * Pipeline interface for batching Redis commands.
 */
interface RedisPipelineContract
{
    /**
     * Forward any Redis command to the underlying pipeline.
     *
     * @param  array<int, mixed>  $arguments
     */
    public function __call(string $name, array $arguments): mixed;

    /**
     * Execute the pipeline and return all results.
     *
     * @return array<int, mixed>|false
     */
    public function exec(): array|false;
}
