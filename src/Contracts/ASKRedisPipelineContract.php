<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Contracts;

use BAGArt\ASKClient\Contracts\Pipeline\ASKFutureContract;

interface ASKRedisPipelineContract
{
    public function add(ASKRedisOperationContract $op): self;

    public function execute(): ASKFutureContract;

    public function clear(): self;

    public function count(): int;
}
