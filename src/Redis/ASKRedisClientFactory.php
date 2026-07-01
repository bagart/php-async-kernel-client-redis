<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Redis;

use BAGArt\ASKClientRedis\Exception\ASKRedisConnectionException;
use BAGArt\ASKClientRedis\Redis\Client\PhpRedisAdapter;
use BAGArt\ASKClientRedis\Redis\Client\PredisAdapter;
use BAGArt\ASKClientRedis\Redis\Contract\RedisClientContract;

final class ASKRedisClientFactory
{
    public function create(RedisDsn $redisDsn): RedisClientContract
    {
        if (class_exists(\Predis\Client::class)) {
            return new PredisAdapter($redisDsn);
        }

        if (extension_loaded('redis')) {
            return new PhpRedisAdapter($redisDsn);
        }

        throw new ASKRedisConnectionException(
            'Neither Predis nor PhpRedis extension is available. Install predis/predis or the redis PHP extension.',
        );
    }
}
