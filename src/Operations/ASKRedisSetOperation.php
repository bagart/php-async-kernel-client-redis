<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Operations;

use BAGArt\ASKClientRedis\Contracts\ASKRedisOperationContract;

final readonly class ASKRedisSetOperation implements ASKRedisOperationContract
{
    /**
     * @param  string  $key
     * @param  mixed  $value
     * @param  int|null  $ttl  TTL in seconds, null for no expiry
     */
    public function __construct(
        private string $key,
        private mixed $value,
        private ?int $ttl = null,
    ) {
    }

    public function command(): string
    {
        return 'SET';
    }

    public function arguments(): array
    {
        $args = [$this->key, $this->serializeValue($this->value)];

        if ($this->ttl !== null) {
            $args[] = 'EX';
            $args[] = $this->ttl;
        }

        return $args;
    }

    private function serializeValue(mixed $value): string
    {
        if (is_string($value)) {
            return $value;
        }

        if (is_int($value) || is_float($value)) {
            return (string) $value;
        }

        if (is_null($value)) {
            return '';
        }

        return match (true) {
            is_bool($value) => $value ? '1' : '0',
            is_array($value) => json_encode($value, JSON_THROW_ON_ERROR),
            default => (string) $value,
        };
    }
}
