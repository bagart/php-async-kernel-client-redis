<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Pipeline;

use BAGArt\ASKClient\Contracts\ASKFutureContract;
use BAGArt\ASKClient\Contracts\Client\ASKClientContract;
use BAGArt\ASKClientRedis\Contracts\ASKRedisOperationContract;
use BAGArt\ASKClientRedis\Contracts\ASKRedisPipelineContract;

final class ASKRedisPipeline implements ASKRedisPipelineContract
{
    /** @var ASKRedisOperationContract[] */
    private array $operations = [];

    public function __construct(
        private readonly ASKClientContract $client,
    ) {
    }

    public function add(ASKRedisOperationContract $op): self
    {
        $this->operations[] = $op;

        return $this;
    }

    public function execute(): ASKFutureContract
    {
        $future = $this->client->execute($this);

        $this->operations = [];

        return $future;
    }

    /**
     * @return ASKRedisOperationContract[]
     */
    public function operations(): array
    {
        return $this->operations;
    }

    public function clear(): self
    {
        $this->operations = [];

        return $this;
    }

    public function count(): int
    {
        return count($this->operations);
    }
}
