<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis\Contract;

use BAGArt\ASKClientRedis\Redis\RedisDsn;
use Redis;

/**
 * Creates a connected Redis instance from a DSN.
 */
interface RedisConnectorContract
{
    public function connect(RedisDsn $dsn): Redis;
}
