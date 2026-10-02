<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Contracts;

interface PubSubReadableContract
{
    /**
     * Read the next pub/sub message from the transport.
     *
     * @return array{channel: string, payload: string}|null
     */
    public function readPubSubMessage(): ?array;
}
