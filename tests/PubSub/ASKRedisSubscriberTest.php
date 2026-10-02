<?php

declare(strict_types=1);

namespace BAGArt\ASKClientRedis\Tests\PubSub;

use BAGArt\ASKClient\Contracts\Client\ASKClientContract;
use BAGArt\ASKClientRedis\Contracts\ASKRedisMessageHandlerContract;
use BAGArt\ASKClientRedis\Contracts\PubSubReadableContract;
use BAGArt\ASKClientRedis\Exception\ASKRedisException;
use BAGArt\ASKClientRedis\PubSub\ASKRedisSubscriber;
use PHPUnit\Framework\TestCase;

final class ASKRedisSubscriberTest extends TestCase
{
    private const FAST_BASE_BACKOFF_MS = 1;

    private const FAST_MAX_BACKOFF_MS = 5;

    private const FAST_MAX_FAILURES = 3;

    public function test_throws_after_max_consecutive_failures(): void
    {
        $client = $this->createMock(ASKClientContract::class);
        $adapter = $this->createMock(PubSubReadableContract::class);
        $handler = $this->createMock(ASKRedisMessageHandlerContract::class);

        $adapter->method('readPubSubMessage')
            ->willReturnCallback(static fn () => throw new \RuntimeException('connection lost'));

        $subscriber = new ASKRedisSubscriber(
            $client,
            $adapter,
            'test-channel',
            self::FAST_BASE_BACKOFF_MS,
            self::FAST_MAX_BACKOFF_MS,
            self::FAST_MAX_FAILURES,
        );
        $subscriber->onMessage($handler);

        $this->expectException(ASKRedisException::class);
        $this->expectExceptionMessage('Subscriber disconnected after');

        $subscriber->start()->await();
    }

    public function test_resets_failure_count_on_success(): void
    {
        $client = $this->createMock(ASKClientContract::class);
        $adapter = $this->createMock(PubSubReadableContract::class);
        $handler = $this->createMock(ASKRedisMessageHandlerContract::class);

        $callCount = 0;
        $adapter->method('readPubSubMessage')
            ->willReturnCallback(function () use (&$callCount): ?array {
                $callCount++;

                if ($callCount === 1) {
                    throw new \RuntimeException('transient error');
                }

                return ['channel' => 'test-channel', 'payload' => 'hello'];
            });

        $subscriber = new ASKRedisSubscriber(
            $client,
            $adapter,
            'test-channel',
            self::FAST_BASE_BACKOFF_MS,
            self::FAST_MAX_BACKOFF_MS,
            self::FAST_MAX_FAILURES,
        );

        $handler->method('handle')
            ->with('test-channel', 'hello')
            ->willReturnCallback(function () use ($subscriber): void {
                $subscriber->stop();
            });

        $subscriber->onMessage($handler);

        $subscriber->start()->await();

        self::assertSame(2, $callCount);
    }

    public function test_stop_terminates_loop(): void
    {
        $client = $this->createMock(ASKClientContract::class);
        $adapter = $this->createMock(PubSubReadableContract::class);
        $handler = $this->createMock(ASKRedisMessageHandlerContract::class);

        $callCount = 0;
        $adapter->method('readPubSubMessage')
            ->willReturnCallback(function () use (&$callCount): ?array {
                $callCount++;

                return ['channel' => 'test-channel', 'payload' => 'msg-' . $callCount];
            });

        $subscriber = new ASKRedisSubscriber(
            $client,
            $adapter,
            'test-channel',
            self::FAST_BASE_BACKOFF_MS,
            self::FAST_MAX_BACKOFF_MS,
            self::FAST_MAX_FAILURES,
        );
        $subscriber->onMessage($handler);

        $handler->method('handle')
            ->willReturnCallback(function () use ($subscriber, &$callCount): void {
                if ($callCount >= 3) {
                    $subscriber->stop();
                }
            });

        $subscriber->start()->await();

        self::assertGreaterThanOrEqual(3, $callCount);
    }

    public function test_throws_when_no_handler_registered(): void
    {
        $client = $this->createMock(ASKClientContract::class);
        $adapter = $this->createMock(PubSubReadableContract::class);

        $subscriber = new ASKRedisSubscriber($client, $adapter, 'test-channel');

        $this->expectException(ASKRedisException::class);
        $this->expectExceptionMessage('No message handler');

        $subscriber->start()->await();
    }
}
