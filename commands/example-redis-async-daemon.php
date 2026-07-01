<?php

declare(strict_types=1);

use BAGArt\ASKClientRedis\Connection\FiberRedisConnection;
use BAGArt\ASKClientRedis\Redis\RedisDsn;
use BAGArt\ASKClientRedis\Transport\ASKRedisProtocolDecoder;
use BAGArt\ASKClientRedis\Transport\ASKRedisProtocolEncoder;
use BAGArt\AsyncKernel\ASK;
use BAGArt\AsyncKernel\AsyncKernel;
use BAGArt\AsyncKernel\CliActions;
use BAGArt\AsyncKernel\Daemons\ASKFnDaemon;
use BAGArt\AsyncKernel\Daemons\ASKFnDaemonContext;
use BAGArt\AsyncKernel\Drivers\ASKFiberScheduler;
use BAGArt\AsyncKernel\Wrappers\ASKLogWrapper;

require_once __DIR__.'/../../../../vendor/autoload.php';

$definedOptions = [
    'dsn::',
    'help',
];

$options = CliActions::parseOptions(
    getopt('', $definedOptions),
    $definedOptions,
);

CliActions::initRuntime($options);

if (isset($options['help'])) {
    echo "Usage:
  php commands/example-redis-async-daemon.php
  php commands/example-redis-async-daemon.php --dsn=tcp://127.0.0.1:6379

Options:
  --dsn=DSN                  Redis DSN (default: tcp://127.0.0.1:6379)
  --help
";
    exit(0);
}

$dsn = (string)($options['dsn'] ?? 'tcp://127.0.0.1:6379');
$maxConcurrent = 50;

$parsedDsn = RedisDsn::parse($dsn);
$host = $parsedDsn->host;
$port = $parsedDsn->port;

$logger = new ASKLogWrapper(minLevel: 'error');
$scheduler = new ASKFiberScheduler();
$kernel = new AsyncKernel(logger: $logger);

$histogram = [
    '.' => 0,
    ',' => 0,
    ':' => 0,
    ';' => 0,
    '|' => 0,
];

$spawnOp = function (ASKFiberScheduler $scheduler, ASKFnDaemonContext $context, string $host, int $port) use (
    &
    $histogram
): void {
    $loopCount = 6000000 + random_int(0, 12000000);
    $luaScript = "local n=0;for i=1,{$loopCount} do n=n+i end;return n";

    $scheduler->enqueue(
        function () use ($scheduler, $host, $port, $luaScript, $context, &$histogram): void {
            $encoder = new ASKRedisProtocolEncoder();
            $decoder = new ASKRedisProtocolDecoder();

            $connection = new FiberRedisConnection(
                scheduler: $scheduler,
                host: $host,
                port: $port,
                timeout: 10,
            );

            $active = 0;
            $failed = false;

            try {
                $connection->connect();
                $payload = $encoder->encodeCommand('EVAL', [$luaScript, '0']);
                $connection->write($payload);

                while (!$decoder->hasCompleteResponse()) {
                    $decoder->feed($connection->read());
                }
                $decoder->decode();

                $active = $context->payload['active'];
            } catch (\Throwable $e) {
                $active = $context->payload['active'];
                $failed = true;
            } finally {
                $context->payload['active'] = $active - 1;
                $connection->disconnect();
            }

            if ($failed) {
                echo '!';
            } else {
                $char = match (true) {
                    $active <= 5 => '.',
                    $active <= 10 => ',',
                    $active <= 20 => ':',
                    $active <= 30 => ';',
                    default => '|',
                };

                $histogram[$char]++;

                echo $char;
            }
        }
    );

    $context->payload['totalProduced']++;
    $context->payload['active']++;
};

$waveSizes = [5, 10, 20, 35, 50, 35, 20, 10, 5];

$daemon = new ASKFnDaemon(
    daemonContext: new ASKFnDaemonContext(
        daemonName: 'redis-async-demo',
        scheduler: $scheduler,
        logger: $logger,
        payload: [
            'active' => 0,
            'totalProduced' => 0,
            'totalLimit' => array_sum($waveSizes),
            'maxConcurrent' => $maxConcurrent,
            'waveIndex' => 0,
            'waveSizes' => $waveSizes,
        ],
    ),
    fnStartup: function (ASKFnDaemonContext $context): void {
        echo "Concurrent connections → char:\n";
        echo "   0-5:  .\n";
        echo "   5-10: ,\n";
        echo "  10-20: :\n";
        echo "  20-30: ;\n";
        echo " 30-50: |\n";
        echo "\nEach ↑ line = new batch spawned. Chars = completions.\n\n";
    },
    fnProduce: function (ASKFnDaemonContext $context) use ($scheduler, $host, $port, $spawnOp): void {
        $waveSizes = $context->payload['waveSizes'];
        $waveSize = $waveSizes[$context->payload['waveIndex']];

        for ($i = 0; $i < $waveSize; $i++) {
            $spawnOp($scheduler, $context, $host, $port);

            ASK::sleep(25)->await();
        }

        echo "\n  ↑ wave {$context->payload['waveIndex']} spawned ({$waveSize} conns)\n";

        $context->payload['waveIndex']++;
    },
    fnCanProduce: function (ASKFnDaemonContext $context): bool {
        $waveSizes = $context->payload['waveSizes'];

        return $context->payload['waveIndex'] < count($waveSizes)
            && $context->payload['active'] === 0;
    },
    fnTick: function (ASKFnDaemonContext $context) use ($kernel): void {
        if (
            $context->payload['totalProduced'] >= $context->payload['totalLimit']
            && $context->payload['active'] === 0
        ) {
            $kernel->stop('all operations complete');
        }
    },
    fnShutdown: function (ASKFnDaemonContext $context, mixed $shutdownContext = null): bool {
        return true;
    },
);

$startTime = microtime(true);

$kernel
    ->addDaemon(daemon: $daemon, producerInterval: 0)
    ->run();

$elapsed = microtime(true) - $startTime;

$total = array_sum($histogram);
$labels = [
    '.' => '  0-5',
    ',' => ' 5-10',
    ':' => '10-20',
    ';' => '20-30',
    '|' => '30-50',
];

echo "\n--- Distribution ---\n";
foreach ($histogram as $char => $count) {
    $pct = $total > 0 ? $count / $total * 100 : 0;
    $barLen = max(1, (int)($pct));
    echo "  {$char} {$labels[$char]}: ".str_repeat('█', $barLen)." {$count} (".round($pct)."%)\n";
}

echo "\n--- Summary ---\n";
echo "  {$total} EVAL calls in ".number_format($elapsed, 2).'s';
echo ' ('.number_format($total / max($elapsed, 0.001), 0)." ops/s)\n";
