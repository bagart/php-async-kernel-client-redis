<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Contracts;

use BAGArt\ASKClient\Contracts\ASKFutureContract;

interface ASKRedisSubscriberContract
{
    public function onMessage(callable $handler): self;

    public function start(): ASKFutureContract;

    public function stop(): void;
}
