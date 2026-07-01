<?php

declare(strict_types=1);

use BAGArt\AsyncKernel\CliActions;
use BAGArt\ASKClient\ASKClient;
use BAGArt\ASKClientRedis\ASKRedisClient;
use BAGArt\ASKClientRedis\Operations\ASKRedisDelOperation;
use BAGArt\ASKClientRedis\Operations\ASKRedisGetOperation;
use BAGArt\ASKClientRedis\Operations\ASKRedisIncrOperation;
use BAGArt\ASKClientRedis\Operations\ASKRedisSetOperation;
use BAGArt\ASKClientRedis\Transport\ASKRedisConnection;
use BAGArt\ASKClientRedis\Transport\ASKRedisTransport;
use BAGArt\ASKClientRedis\Transport\ASKRedisTransportAdapter;

require_once __DIR__.'/../../../../vendor/autoload.php';

$definedOptions = [
    'dsn::',
    'workers::',
    'help',
];

$options = CliActions::parseOptions(
    getopt('', $definedOptions),
    $definedOptions
);

if (isset($options['help'])) {
    echo "Usage:
php commands/example-redis-worker-pool.php                    # Default: 5 workers
php commands/example-redis-worker-pool.php --workers=10

Options:
  --dsn=DSN                        Redis DSN (default: tcp://127.0.0.1:6379)
  --workers=N                       Number of parallel workers (default: 5)
  --help
";
    exit(0);
}

$dsn = (string)($options['dsn'] ?? 'tcp://127.0.0.1:6379');
$numWorkers = (int)($options['workers'] ?? 5);

$connection = new ASKRedisConnection(dsn: $dsn, timeout: 5);
$redisTransport = new ASKRedisTransport(connection: $connection);
$adapter = new ASKRedisTransportAdapter($redisTransport);

$askClient = new ASKClient($adapter);

$client = new ASKRedisClient($askClient, $adapter);

echo "=== ASKRedis Worker Pool Example ===\n";
echo "Workers: {$numWorkers}\n\n";

$queueKey = 'worker:jobs:pending';
$resultPrefix = 'worker:jobs:result';
$activeKey = 'worker:jobs:active';

$client->del($queueKey, $activeKey)->await();

$totalJobs = 20;
for ($i = 0; $i < $totalJobs; $i++) {
    $job = json_encode([
        'id' => $i,
        'type' => $i % 3 === 0 ? 'heavy' : 'light',
        'payload' => ['data' => str_repeat('x', rand(100, 1000))],
    ]);
    $client->pipeline()
        ->add(new ASKRedisSetOperation("{$queueKey}:{$i}", $job))
        ->add(new ASKRedisIncrOperation($queueKey))
        ->execute()
        ->await();
}

echo "--- Enqueued {$totalJobs} jobs ---\n\n";

echo "--- Fanning out to {$numWorkers} workers ---\n";
$start = microtime(true);

$workerFutures = [];
for ($w = 0; $w < $numWorkers; $w++) {
    $workerFutures[$w] = $client->get($queueKey);
}

$jobCounts = array_fill(0, $numWorkers, 0);
foreach ($workerFutures as $workerId => $future) {
    $count = $future->await();
    $jobCounts[$workerId] = (int)$count;
    echo "  Worker {$workerId}: sees {$count} jobs in queue\n";
}

$elapsed = microtime(true) - $start;
echo "\nQueue scan time: " . number_format($elapsed, 3) . "s\n\n";

echo "--- Processing jobs with batched writes ---\n";
$start = microtime(true);

$batchSize = 5;
$processed = 0;

for ($batch = 0; $batch < ceil($totalJobs / $batchSize); $batch++) {
    $pipeline = $client->pipeline();

    $startIdx = $batch * $batchSize;
    $endIdx = min($startIdx + $batchSize, $totalJobs);

    for ($i = $startIdx; $i < $endIdx; $i++) {
        $pipeline->add(new ASKRedisGetOperation("{$queueKey}:{$i}"));
    }

    $results = $pipeline->execute()->await();

    $writePipeline = $client->pipeline();
    foreach ($results as $i => $jobData) {
        if ($jobData === null) {
            continue;
        }

        $job = json_decode((string)$jobData, true);
        $result = [
            'job_id' => $job['id'],
            'worker' => $i % $numWorkers,
            'processed_at' => microtime(true),
            'hash' => md5($job['payload']['data']),
        ];

        $writePipeline->add(new ASKRedisSetOperation(
            "{$resultPrefix}:{$job['id']}",
            json_encode($result),
            ttl: 120,
        ));

        $processed++;
    }

    $writePipeline->execute()->await();
    echo "  Batch {$batch}: processed " . count($results) . " jobs\n";
}

$elapsed = microtime(true) - $start;
echo "\nProcessing time: " . number_format($elapsed, 3) . "s ({$processed} jobs)\n\n";

echo "--- Collecting results with parallel GET ---\n";
$start = microtime(true);

$futures = [];
for ($i = 0; $i < $totalJobs; $i++) {
    $futures[$i] = $client->get("{$resultPrefix}:{$i}");
}

$results = [];
foreach ($futures as $i => $future) {
    $value = $future->await();
    if ($value !== null) {
        $results[$i] = json_decode((string)$value, true);
    }
}

$elapsed = microtime(true) - $start;
echo "Collected " . count($results) . " results in " . number_format($elapsed, 3) . "s\n\n";

echo "--- Worker distribution ---\n";
$distribution = array_fill(0, $numWorkers, 0);
foreach ($results as $r) {
    $distribution[$r['worker']]++;
}

foreach ($distribution as $workerId => $count) {
    $bar = str_repeat('█', $count);
    echo str_pad("  Worker {$workerId}:", 15) . str_pad("{$count}", 5) . " {$bar}\n";
}

echo "\n--- Cleanup ---\n";
$cleanupPipeline = $client->pipeline();
for ($i = 0; $i < $totalJobs; $i++) {
    $cleanupPipeline->add(new ASKRedisDelOperation([
        "{$queueKey}:{$i}",
        "{$resultPrefix}:{$i}",
    ]));
}
$cleanupPipeline->add(new ASKRedisDelOperation([$queueKey, $activeKey]));
$cleanupPipeline->execute()->await();
echo "  Cleaned up {$totalJobs} job keys + queue metadata\n";

$totalTime = microtime(true) - $start;
echo "\nTotal time: " . number_format($totalTime, 3) . "s\n";
echo "Throughput: " . number_format($processed / max($totalTime, 0.001), 1) . " jobs/sec\n";

echo "\nDone.\n";
