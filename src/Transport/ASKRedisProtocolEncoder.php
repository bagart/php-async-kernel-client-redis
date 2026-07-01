<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Transport;

final class ASKRedisProtocolEncoder
{
    public function encodeCommand(string $command, array $arguments): string
    {
        $parts = array_merge([$command], array_map('strval', $arguments));
        $count = count($parts);

        $result = '*' . $count . "\r\n";

        foreach ($parts as $part) {
            $result .= '$' . strlen($part) . "\r\n" . $part . "\r\n";
        }

        return $result;
    }

    /**
     * @param  array<array{string, array}>  $commands  Array of [command, args] pairs
     */
    public function encodePipeline(array $commands): string
    {
        $result = '';

        foreach ($commands as [$command, $arguments]) {
            $result .= $this->encodeCommand($command, $arguments);
        }

        return $result;
    }

}
