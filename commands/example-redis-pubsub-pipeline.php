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
php commands/example-redis-pubsub-pipeline.php

Options:
  --dsn=DSN                        Redis DSN (default: tcp://127.0.0.1:6379)
  --workers=N                       Pipeline parallelism (default: 3)
  --help
";
    exit(0);
}

$dsn = (string)($options['dsn'] ?? 'tcp://127.0.0.1:6379');
$parallelism = (int)($options['workers'] ?? 3);

$connection = new ASKRedisConnection(dsn: $dsn, timeout: 5);
$redisTransport = new ASKRedisTransport(connection: $connection);
$adapter = new ASKRedisTransportAdapter($redisTransport);

$askClient = new ASKClient($adapter);

$client = new ASKRedisClient($askClient, $adapter);

echo "=== ASKRedis PubSub + Pipeline Complex Example ===\n\n";

echo "--- 1. Pipeline fan-out: write N keys in one round-trip ---\n";
$start = microtime(true);

$keyCount = 50;
$pipeline = $client->pipeline();
for ($i = 0; $i < $keyCount; $i++) {
    $pipeline->add(new ASKRedisSetOperation(
        "fanout:{$i}",
        json_encode([
            'index' => $i,
            'timestamp' => microtime(true),
            'hash' => md5((string)$i),
            'tags' => ['tag:' . ($i % 5), 'batch:' . floor($i / 10)],
        ]),
        ttl: 120,
    ));
}
$pipeline->execute()->await();

$elapsed = microtime(true) - $start;
echo "  Wrote {$keyCount} keys in " . number_format($elapsed, 4) . "s\n\n";

echo "--- 2. Pipeline fan-in: read all back with chunked parallelism ---\n";
$start = microtime(true);

$chunkSize = 10;
$chunks = array_chunk(range(0, $keyCount - 1), $chunkSize);
$allResults = [];

foreach ($chunks as $chunkIndex => $chunk) {
    $pipeline = $client->pipeline();
    foreach ($chunk as $i) {
        $pipeline->add(new ASKRedisGetOperation("fanout:{$i}"));
    }

    $results = $pipeline->execute()->await();

    foreach ($results as $i => $value) {
        if ($value !== null) {
            $allResults[] = json_decode((string)$value, true);
        }
    }
}

$elapsed = microtime(true) - $start;
echo "  Read {$keyCount} keys in " . count($chunks) . " chunks, " . number_format($elapsed, 4) . "s\n";
echo "  Sample: index={$allResults[0]['index']}, hash={$allResults[0]['hash']}\n\n";

echo "--- 3. Conditional pipeline: read → compare → write ---\n";
$start = microtime(true);

$thresholdKey = 'fanout:threshold';
$client->set($thresholdKey, '5', ttl: 60)->await();

$readPipeline = $client->pipeline();
$readPipeline->add(new ASKRedisGetOperation($thresholdKey));
for ($i = 0; $i < 10; $i++) {
    $readPipeline->add(new ASKRedisGetOperation("fanout:{$i}"));
}

$readResults = $readPipeline->execute()->await();
$threshold = (int)$readResults[0];

$writePipeline = $client->pipeline();
$count = 0;
for ($i = 0; $i < 10; $i++) {
    $data = json_decode((string)$readResults[$i + 1], true);
    if ($data['index'] < $threshold) {
        $writePipeline->add(new ASKRedisSetOperation(
            "fanout:below_threshold:{$i}",
            json_encode($data),
            ttl: 60,
        ));
        $count++;
    }
}
$writePipeline->execute()->await();

$elapsed = microtime(true) - $start;
echo "  Threshold: {$threshold}, matched: {$count}/10, time: " . number_format($elapsed, 4) . "s\n\n";

echo "--- 4. Counter aggregation: parallel INCR across multiple keys ---\n";
$start = microtime(true);

$metrics = ['views', 'clicks', 'signups', 'purchases', 'errors'];
$futures = [];

foreach ($metrics as $metric) {
    $pipeline = $client->pipeline();
    for ($i = 0; $i < 10; $i++) {
        $pipeline->add(new ASKRedisIncrOperation("metric:{$metric}"));
    }
    $futures[$metric] = $pipeline->execute();
}

$finalValues = [];
foreach ($futures as $metric => $future) {
    $results = $future->await();
    $finalValues[$metric] = end($results);
}

arsort($finalValues);
foreach ($finalValues as $metric => $value) {
    $bar = str_repeat('█', min((int)$value, 40));
    echo str_pad("  {$metric}:", 15) . str_pad((string)$value, 5) . " {$bar}\n";
}

$elapsed = microtime(true) - $start;
echo "\n  Aggregation time: " . number_format($elapsed, 4) . "s\n\n";

echo "--- 5. Multi-step pipeline: SET → INCR → GET → DEL ---\n";
$start = microtime(true);

$stepKey = 'multi:step';
$steps = 5;

for ($step = 0; $step < $steps; $step++) {
    $pipeline = $client->pipeline();

    $pipeline->add(new ASKRedisSetOperation($stepKey, "step:{$step}"));
    $pipeline->add(new ASKRedisIncrOperation('multi:counter'));
    $pipeline->add(new ASKRedisGetOperation($stepKey));
    $pipeline->add(new ASKRedisGetOperation('multi:counter'));

    $results = $pipeline->execute()->await();

    echo "  Step {$step}: SET=" . $results[0] . ", INCR=" . $results[1]
        . ", GET=" . $results[2] . ", COUNTER=" . $results[3] . "\n";
}

$client->del($stepKey, 'multi:counter')->await();

$elapsed = microtime(true) - $start;
echo "  Multi-step time: " . number_format($elapsed, 4) . "s\n\n";

echo "--- 6. Stress test: N parallel pipelines of M operations each ---\n";
$start = microtime(true);

$pipelineCount = $parallelism;
$opsPerPipeline = 20;
$futures = [];

for ($p = 0; $p < $pipelineCount; $p++) {
    $pipeline = $client->pipeline();
    for ($o = 0; $o < $opsPerPipeline; $o++) {
        $pipeline->add(new ASKRedisSetOperation(
            "stress:{$p}:{$o}",
            bin2hex(random_bytes(32)),
            ttl: 30,
        ));
    }
    $futures[$p] = $pipeline->execute();
}

foreach ($futures as $p => $future) {
    $future->await();
}

$totalOps = $pipelineCount * $opsPerPipeline;
$elapsed = microtime(true) - $start;
echo "  {$pipelineCount} pipelines x {$opsPerPipeline} ops = {$totalOps} total ops\n";
echo "  Time: " . number_format($elapsed, 4) . "s\n";
echo "  Throughput: " . number_format($totalOps / max($elapsed, 0.001), 0) . " ops/sec\n\n";

echo "--- Cleanup ---\n";
$cleanup = $client->pipeline();
for ($p = 0; $p < $pipelineCount; $p++) {
    for ($o = 0; $o < $opsPerPipeline; $o++) {
        $cleanup->add(new ASKRedisDelOperation(["stress:{$p}:{$o}"]));
    }
}
for ($i = 0; $i < $keyCount; $i++) {
    $cleanup->add(new ASKRedisDelOperation(["fanout:{$i}"]));
}
$cleanup->add(new ASKRedisDelOperation([
    $thresholdKey,
    'fanout:threshold',
]));
$cleanup->execute()->await();
echo "  Cleaned up all keys\n";

echo "\nDone.\n";
