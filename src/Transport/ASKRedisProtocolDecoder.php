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

        return $this->scanForCompleteResponse() !== null;
    }

    public function decode(): mixed
    {
        $result = $this->scanForCompleteResponse();

        if ($result === null) {
            throw new ASKRedisException('No complete RESP response available in buffer');
        }

        $this->buffer = $result['remaining'];

        return $result['value'];
    }

    private function scanForCompleteResponse(): ?array
    {
        if ($this->buffer === '') {
            return null;
        }

        $pos = 0;
        $len = strlen($this->buffer);

        if ($pos >= $len) {
            return null;
        }

        $type = $this->buffer[$pos];
        $pos++;

        return match ($type) {
            '+' => $this->decodeSimpleString($pos),
            '-' => $this->decodeError($pos),
            ':' => $this->decodeInteger($pos),
            '$' => $this->decodeBulkString($pos),
            '*' => $this->decodeArray($pos),
            default => throw new ASKRedisException(sprintf('Unknown RESP type: %s', $type)),
        };
    }

    private function decodeSimpleString(int $pos): ?array
    {
        $end = strpos($this->buffer, "\r\n", $pos);

        if ($end === false) {
            return null;
        }

        $value = substr($this->buffer, $pos, $end - $pos);

        return [
            'value' => $value,
            'remaining' => substr($this->buffer, $end + 2),
        ];
    }

    private function decodeError(int $pos): ?array
    {
        $end = strpos($this->buffer, "\r\n", $pos);

        if ($end === false) {
            return null;
        }

        $message = substr($this->buffer, $pos, $end - $pos);

        return [
            'value' => new ASKRedisException($message),
            'remaining' => substr($this->buffer, $end + 2),
        ];
    }

    private function decodeInteger(int $pos): ?array
    {
        $end = strpos($this->buffer, "\r\n", $pos);

        if ($end === false) {
            return null;
        }

        $numStr = substr($this->buffer, $pos, $end - $pos);
        $value = (int) $numStr;

        return [
            'value' => $value,
            'remaining' => substr($this->buffer, $end + 2),
        ];
    }

    private function decodeBulkString(int $pos): ?array
    {
        $end = strpos($this->buffer, "\r\n", $pos);

        if ($end === false) {
            return null;
        }

        $length = (int) substr($this->buffer, $pos, $end - $pos);

        if ($length === -1) {
            return [
                'value' => null,
                'remaining' => substr($this->buffer, $end + 2),
            ];
        }

        $dataStart = $end + 2;
        $totalNeeded = $dataStart + $length + 2;

        if (strlen($this->buffer) < $totalNeeded) {
            return null;
        }

        $value = substr($this->buffer, $dataStart, $length);

        return [
            'value' => $value,
            'remaining' => substr($this->buffer, $totalNeeded),
        ];
    }

    private function decodeArray(int $pos): ?array
    {
        $end = strpos($this->buffer, "\r\n", $pos);

        if ($end === false) {
            return null;
        }

        $count = (int) substr($this->buffer, $pos, $end - $pos);

        if ($count === -1) {
            return [
                'value' => null,
                'remaining' => substr($this->buffer, $end + 2),
            ];
        }

        $remaining = substr($this->buffer, $end + 2);
        $items = [];

        for ($i = 0; $i < $count; $i++) {
            $this->buffer = $remaining;
            $inner = $this->scanForCompleteResponse();

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
