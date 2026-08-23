<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Contracts;

use BAGArt\ASKClient\Contracts\Pipeline\ASKFutureContract;

interface ASKRedisSubscriberContract
{
    public function onMessage(ASKRedisMessageHandlerContract $handler): self;

    public function start(): ASKFutureContract;

    public function stop(): void;
}
