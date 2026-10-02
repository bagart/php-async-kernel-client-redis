<?php

declare(strict_types=1);

use BAGArt\ASKClientRedis\Transport\ASKRedisConnection;
use BAGArt\ASKClientRedis\Transport\ASKRedisTransport;
use BAGArt\AsyncKernel\Wrappers\ASKLogWrapper;
use Psr\Log\AbstractLogger;

class FakeWarningLogger extends AbstractLogger
{
    /** @var list<array{level: string, message: string, context: array}> */
    public array $records = [];

    public function log($level, string|\Stringable $message, array $context = []): void
    {
        $this->records[] = [
            'level' => $level,
            'message' => (string) $message,
            'context' => $context,
        ];
    }
}

describe('ASKRedisTransport::readPubSubMessage error logging', function () {
    it('returns null when not connected (no logging needed)', function () {
        $connection = new ASKRedisConnection('tcp://127.0.0.1:19999', timeout: 1);

        $logger = new FakeWarningLogger();
        $transport = new ASKRedisTransport($connection, logger: new ASKLogWrapper($logger));

        $result = $transport->readPubSubMessage();

        expect($result)->toBeNull()
            ->and($logger->records)->toBeEmpty();
    });

    it('logs a warning when readPubSubMessage catches a read exception', function () {
        $server = @stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if ($server === false) {
            $this->markTestSkipped("stream_socket_server failed: $errstr");
        }

        $addr = stream_socket_get_name($server, false);
        $client = @stream_socket_client("tcp://$addr", $errno2, $errstr2, 2);
        if ($client === false) {
            fclose($server);
            $this->markTestSkipped("stream_socket_client failed: $errstr2");
        }

        $peer = @stream_socket_accept($server, 2);
        if ($peer === false) {
            fclose($server);
            fclose($client);
            $this->markTestSkipped('stream_socket_accept failed');
        }

        $connection = new ASKRedisConnection('tcp://127.0.0.1:19999');

        $ref = new ReflectionClass($connection);

        $socketProp = $ref->getProperty('socket');
        $socketProp->setValue($connection, $client);

        $connectedProp = $ref->getProperty('connected');
        $connectedProp->setValue($connection, true);

        fclose($peer);

        $logger = new FakeWarningLogger();
        $transport = new ASKRedisTransport($connection, logger: new ASKLogWrapper($logger));

        $result = $transport->readPubSubMessage();

        expect($result)->toBeNull()
            ->and($logger->records)->not->toBeEmpty()
            ->and($logger->records[0]['level'])->toBe('warning')
            ->and($logger->records[0]['message'])->toContain('readPubSubMessage failed');

        fclose($server);
    });

    it('returns null after logging without propagating the exception', function () {
        $server = @stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if ($server === false) {
            $this->markTestSkipped("stream_socket_server failed: $errstr");
        }

        $addr = stream_socket_get_name($server, false);
        $client = @stream_socket_client("tcp://$addr", $errno2, $errstr2, 2);
        if ($client === false) {
            fclose($server);
            $this->markTestSkipped("stream_socket_client failed: $errstr2");
        }

        $peer = @stream_socket_accept($server, 2);
        if ($peer === false) {
            fclose($server);
            fclose($client);
            $this->markTestSkipped('stream_socket_accept failed');
        }

        $connection = new ASKRedisConnection('tcp://127.0.0.1:19999');

        $ref = new ReflectionClass($connection);

        $socketProp = $ref->getProperty('socket');
        $socketProp->setValue($connection, $client);

        $connectedProp = $ref->getProperty('connected');
        $connectedProp->setValue($connection, true);

        fclose($peer);

        $logger = new FakeWarningLogger();
        $transport = new ASKRedisTransport($connection, logger: new ASKLogWrapper($logger));

        $result = $transport->readPubSubMessage();

        expect($result)->toBeNull();

        fclose($server);
    });

    it('does not log when adapter receives a valid message', function () {
        $server = @stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
        if ($server === false) {
            $this->markTestSkipped("stream_socket_server failed: $errstr");
        }

        $addr = stream_socket_get_name($server, false);
        $client = @stream_socket_client("tcp://$addr", $errno2, $errstr2, 2);
        if ($client === false) {
            fclose($server);
            $this->markTestSkipped("stream_socket_client failed: $errstr2");
        }

        $peer = @stream_socket_accept($server, 2);
        if ($peer === false) {
            fclose($server);
            fclose($client);
            $this->markTestSkipped('stream_socket_accept failed');
        }

        $connection = new ASKRedisConnection('tcp://127.0.0.1:19999');

        $ref = new ReflectionClass($connection);

        $socketProp = $ref->getProperty('socket');
        $socketProp->setValue($connection, $client);

        $connectedProp = $ref->getProperty('connected');
        $connectedProp->setValue($connection, true);

        $resp = "*3\r\n$7\r\nmessage\r\n$7\r\nchannel\r\n$7\r\npayload\r\n";
        fwrite($peer, $resp);

        $logger = new FakeWarningLogger();
        $transport = new ASKRedisTransport($connection, logger: new ASKLogWrapper($logger));

        $result = $transport->readPubSubMessage();

        expect($result)->toBe(['channel' => 'channel', 'payload' => 'payload'])
            ->and($logger->records)->toBeEmpty();

        fclose($peer);
        fclose($server);
    });
});
