<?php

declare(strict_types=1);

use BAGArt\ASKClient\Contracts\Client\ASKClientContract;
use BAGArt\ASKClient\Contracts\Pipeline\ASKContextContract;
use BAGArt\ASKClient\Contracts\Pipeline\ASKFutureContract;
use BAGArt\ASKClientRedis\Contracts\ASKRedisMessageHandlerContract;
use BAGArt\ASKClientRedis\Contracts\PubSubReadableContract;
use BAGArt\ASKClientRedis\PubSub\ASKRedisSubscriber;

/**
 * Hand-rolled fakes for subscriber re-subscribe tests (no Mockery).
 */
class FakePubSubAdapter implements PubSubReadableContract
{
    /** @var list<array{channel: string, payload: string}|null> */
    private array $responses;

    private int $index = 0;

    /**
     * @param  list<array{channel: string, payload: string}|null>  $responses
     */
    public function __construct(array $responses)
    {
        $this->responses = $responses;
    }

    public function readPubSubMessage(): ?array
    {
        if ($this->index >= count($this->responses)) {
            return null;
        }

        return $this->responses[$this->index++];
    }
}

class FakeRecordingClient implements ASKClientContract
{
    /** @var list<object> */
    public array $operations = [];

    public function execute(object $operation, ?ASKContextContract $context = null): ASKFutureContract
    {
        $this->operations[] = $operation;

        return new FakeResolvingFuture();
    }
}

class FakeResolvingFuture implements ASKFutureContract
{
    public function isCompleted(): bool
    {
        return true;
    }

    public function isSuccessful(): bool
    {
        return true;
    }

    public function getError(): ?\Throwable
    {
        return null;
    }

    public function await(): mixed
    {
        return null;
    }

    public function then(callable $callback): self
    {
        return $this;
    }

    public function catch(callable $callback): self
    {
        return $this;
    }

    public function recover(callable $callback): self
    {
        return $this;
    }

    public function finally(callable $callback): self
    {
        return $this;
    }

    public function dispose(): void
    {
    }

    public function onDispose(\Closure $callback): void
    {
    }
}

class StoppingHandler implements ASKRedisMessageHandlerContract
{
    private \Closure $onMessage;

    public function __construct(callable $onMessage)
    {
        $this->onMessage = \Closure::fromCallable($onMessage);
    }

    public function handle(string $channel, string $payload): void
    {
        ($this->onMessage)($channel, $payload);
    }
}

describe('ASKRedisSubscriber re-subscribe on null returns', function () {
    it('re-subscribes when adapter returns null (simulating transport disconnect)', function () {
        $adapter = new FakePubSubAdapter([
            null,
            null,
            ['channel' => 'test', 'payload' => 'hello'],
        ]);
        $client = new FakeRecordingClient();

        $subscriber = new ASKRedisSubscriber(
            $client,
            $adapter,
            'test',
            1,
            5,
            30,
        );

        $subscriber->onMessage(new StoppingHandler(function () use ($subscriber) {
            $subscriber->stop();
        }));

        $subscriber->start()->await();

        $subscribeOps = array_filter(
            $client->operations,
            static fn (object $op) => str_contains(get_class($op), 'SubscribeOperation'),
        );

        expect($subscribeOps)->not->toBeEmpty();
    });

    it('re-subscribes after each null before receiving a real message', function () {
        $adapter = new FakePubSubAdapter([
            null,
            null,
            null,
            ['channel' => 'ch', 'payload' => 'data'],
        ]);
        $client = new FakeRecordingClient();

        $subscriber = new ASKRedisSubscriber(
            $client,
            $adapter,
            'ch',
            1,
            5,
            30,
        );

        $subscriber->onMessage(new StoppingHandler(function () use ($subscriber) {
            $subscriber->stop();
        }));

        $subscriber->start()->await();

        $subscribeOps = array_filter(
            $client->operations,
            static fn (object $op) => str_contains(get_class($op), 'SubscribeOperation'),
        );

        // 1 initial subscribe + 3 re-subscribes after null returns
        expect($subscribeOps)->toHaveCount(4);
    });

    it('resets failure count after successful message and stops cleanly', function () {
        $callIndex = 0;
        $adapter = new FakePubSubAdapter([
            null,
            ['channel' => 'x', 'payload' => 'first'],
            null,
            ['channel' => 'x', 'payload' => 'second'],
        ]);
        $client = new FakeRecordingClient();

        $subscriber = new ASKRedisSubscriber(
            $client,
            $adapter,
            'x',
            1,
            5,
            30,
        );

        $received = [];
        $subscriber->onMessage(new StoppingHandler(function (string $channel, string $payload) use (&$received, $subscriber) {
            $received[] = $payload;
            if (count($received) >= 2) {
                $subscriber->stop();
            }
        }));

        $subscriber->start()->await();

        expect($received)->toBe(['first', 'second']);
    });
});
