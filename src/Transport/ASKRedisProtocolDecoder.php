<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Transport;

use BAGArt\ASKClientRedis\Exception\ASKRedisException;

final class ASKRedisProtocolDecoder
{
    private string $buffer = '';

    public function feed(string $data): void
    {
        $this->buffer .= $data;
    }

    public function hasCompleteResponse(): bool
    {
        if ($this->buffer === '') {
            return false;
        }

        return self::scan($this->buffer) !== null;
    }

    public function decode(): mixed
    {
        $result = self::scan($this->buffer);

        if ($result === null) {
            throw new ASKRedisException('No complete RESP response available in buffer');
        }

        $this->buffer = $result['remaining'];

        return $result['value'];
    }

    /**
     * Scans the given buffer without touching decoder state, so probing via
     * hasCompleteResponse() never consumes part of a response.
     *
     * @return array{value: mixed, remaining: string}|null
     */
    private static function scan(string $buffer): ?array
    {
        if ($buffer === '') {
            return null;
        }

        $type = $buffer[0];
        $pos = 1;

        return match ($type) {
            '+' => self::decodeSimpleString($buffer, $pos),
            '-' => self::decodeError($buffer, $pos),
            ':' => self::decodeInteger($buffer, $pos),
            '$' => self::decodeBulkString($buffer, $pos),
            '*' => self::decodeArray($buffer, $pos),
            default => throw new ASKRedisException(sprintf('Unknown RESP type: %s', $type)),
        };
    }

    /**
     * @return array{value: mixed, remaining: string}|null
     */
    private static function decodeSimpleString(string $buffer, int $pos): ?array
    {
        $end = strpos($buffer, "\r\n", $pos);

        if ($end === false) {
            return null;
        }

        $value = substr($buffer, $pos, $end - $pos);

        return [
            'value' => $value,
            'remaining' => substr($buffer, $end + 2),
        ];
    }

    /**
     * @return array{value: mixed, remaining: string}|null
     */
    private static function decodeError(string $buffer, int $pos): ?array
    {
        $end = strpos($buffer, "\r\n", $pos);

        if ($end === false) {
            return null;
        }

        $message = substr($buffer, $pos, $end - $pos);

        return [
            'value' => new ASKRedisException($message),
            'remaining' => substr($buffer, $end + 2),
        ];
    }

    /**
     * @return array{value: mixed, remaining: string}|null
     */
    private static function decodeInteger(string $buffer, int $pos): ?array
    {
        $end = strpos($buffer, "\r\n", $pos);

        if ($end === false) {
            return null;
        }

        $numStr = substr($buffer, $pos, $end - $pos);
        $value = (int)$numStr;

        return [
            'value' => $value,
            'remaining' => substr($buffer, $end + 2),
        ];
    }

    /**
     * @return array{value: mixed, remaining: string}|null
     */
    private static function decodeBulkString(string $buffer, int $pos): ?array
    {
        $end = strpos($buffer, "\r\n", $pos);

        if ($end === false) {
            return null;
        }

        $length = (int)substr($buffer, $pos, $end - $pos);

        if ($length === -1) {
            return [
                'value' => null,
                'remaining' => substr($buffer, $end + 2),
            ];
        }

        $dataStart = $end + 2;
        $totalNeeded = $dataStart + $length + 2;

        if (strlen($buffer) < $totalNeeded) {
            return null;
        }

        $value = substr($buffer, $dataStart, $length);

        return [
            'value' => $value,
            'remaining' => substr($buffer, $totalNeeded),
        ];
    }

    /**
     * @return array{value: mixed, remaining: string}|null
     */
    private static function decodeArray(string $buffer, int $pos): ?array
    {
        $end = strpos($buffer, "\r\n", $pos);

        if ($end === false) {
            return null;
        }

        $count = (int)substr($buffer, $pos, $end - $pos);

        if ($count === -1) {
            return [
                'value' => null,
                'remaining' => substr($buffer, $end + 2),
            ];
        }

        $remaining = substr($buffer, $end + 2);
        $items = [];

        for ($i = 0; $i < $count; $i++) {
            $inner = self::scan($remaining);

            if ($inner === null) {
                return null;
            }

            $items[] = $inner['value'];
            $remaining = $inner['remaining'];
        }

        return [
            'value' => $items,
            'remaining' => $remaining,
        ];
    }
}
