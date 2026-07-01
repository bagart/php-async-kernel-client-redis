<?php

declare(strict_types=1);

use BAGArt\ASKClient\ASKClient;
use BAGArt\AsyncKernel\CliActions;
use BAGArt\ASKClientRedis\ASKRedisClient;
use BAGArt\ASKClientRedis\Operations\ASKRedisDelOperation;
use BAGArt\ASKClientRedis\Operations\ASKRedisExpireOperation;
use BAGArt\ASKClientRedis\Operations\ASKRedisGetOperation;
use BAGArt\ASKClientRedis\Operations\ASKRedisIncrOperation;
use BAGArt\ASKClientRedis\Operations\ASKRedisSetOperation;
use BAGArt\ASKClientRedis\Transport\ASKRedisConnection;
use BAGArt\ASKClientRedis\Transport\ASKRedisTransport;
use BAGArt\ASKClientRedis\Transport\ASKRedisTransportAdapter;

require_once __DIR__.'/../../../../vendor/autoload.php';

$definedOptions = [
    'dsn::',
    'help',
];

$options = CliActions::parseOptions(
    getopt('', $definedOptions),
    $definedOptions
);

if (isset($options['help'])) {
    echo "Usage:
php commands/example-redis-batch.php                       # Default: localhost:6379
php commands/example-redis-batch.php --dsn=tcp://127.0.0.1:6379

Options:
  --dsn=DSN                        Redis DSN (default: tcp://127.0.0.1:6379)
  --help
";
    exit(0);
}

$dsn = (string)($options['dsn'] ?? 'tcp://127.0.0.1:6379');

$connection = new ASKRedisConnection(dsn: $dsn, timeout: 5);
$redisTransport = new ASKRedisTransport(connection: $connection);
$adapter = new ASKRedisTransportAdapter($redisTransport);

$askClient = new ASKClient($adapter);

$client = new ASKRedisClient($askClient, $adapter);

echo "=== ASKRedis Batch & Parallel Example ===\n\n";

echo "--- 1. Pipeline: batched SET + GET ---\n";
$start = microtime(true);

$pipeline = $client->pipeline();
for ($i = 0; $i < 10; $i++) {
    $pipeline->add(new ASKRedisSetOperation(
        "batch:test:{$i}",
        json_encode(['id' => $i, 'ts' => time(), 'value' => bin2hex(random_bytes(8))]),
        ttl: 60,
    ));
}

$future = $pipeline->execute();
$future->await();

$getPipeline = $client->pipeline();
for ($i = 0; $i < 10; $i++) {
    $getPipeline->add(new ASKRedisGetOperation("batch:test:{$i}"));
}

$results = $getPipeline->execute()->await();
foreach ($results as $i => $value) {
    $data = json_decode((string)$value, true);
    echo str_pad("  [batch:test:{$i}]", 25) . " => {$data['value']}\n";
}

$elapsed = microtime(true) - $start;
echo "Pipeline batch time: " . number_format($elapsed, 3) . "s\n\n";

echo "--- 2. Parallel INCR (race condition demo) ---\n";
$start = microtime(true);

$key = 'batch:counter';
$client->set($key, '0', ttl: 60)->await();

$futures = [];
for ($i = 0; $i < 100; $i++) {
    $futures[] = $client->incr($key);
}

$results = [];
foreach ($futures as $i => $future) {
    $results[] = $future->await();
}

$finalValue = end($results);
echo "  Counter value after 100 parallel INCRs: {$finalValue}\n";
echo "  (Expected: 100, got: {$finalValue})\n";

$elapsed = microtime(true) - $start;
echo "Parallel INCR time: " . number_format($elapsed, 3) . "s\n\n";

echo "--- 3. Pipeline: batch DEL + verify ---\n";
$start = microtime(true);

$delPipeline = $client->pipeline();
for ($i = 0; $i < 10; $i++) {
    $delPipeline->add(new ASKRedisDelOperation(["batch:test:{$i}"]));
}

$delResults = $delPipeline->execute()->await();
$deletedCount = array_sum(array_map(fn ($r) => (int)$r, $delResults));
echo "  Deleted {$deletedCount} keys\n";

$verifyPipeline = $client->pipeline();
for ($i = 0; $i < 10; $i++) {
    $verifyPipeline->add(new ASKRedisGetOperation("batch:test:{$i}"));
}

$verifyResults = $verifyPipeline->execute()->await();
$nullCount = array_sum(array_map(fn ($r) => $r === null ? 1 : 0, $verifyResults));
echo "  Verified {$nullCount} keys are null (deleted)\n";

$elapsed = microtime(true) - $start;
echo "DEL batch time: " . number_format($elapsed, 3) . "s\n\n";

echo "--- 4. Promise chaining: SET → GET → INCR → GET ---\n";
$start = microtime(true);

$client->set('chain:start', 'initial', ttl: 60)->await();
echo "  Step 0 - SET: OK\n";

$value = $client->get('chain:start')->await();
echo "  Step 1 - GET: " . (string)$value . "\n";

$count = $client->incr('chain:counter')->await();
echo "  Step 2 - INCR: {$count}\n";

$value = $client->get('chain:counter')->await();
echo "  Step 3 - GET: " . (string)$value . "\n";

echo "  Chain complete.\n";

$elapsed = microtime(true) - $start;
echo "Chaining time: " . number_format($elapsed, 3) . "s\n\n";

echo "--- 5. Rate limiter pattern (sliding window) ---\n";
$start = microtime(true);

$userId = 'user:123';
$window = 60;
$maxRequests = 5;

for ($i = 0; $i < 8; $i++) {
    $pipeline = $client->pipeline();
    $pipeline->add(new ASKRedisIncrOperation($userId));
    $pipeline->add(new ASKRedisExpireOperation($userId, $window));

    $results = $pipeline->execute()->await();
    $count = (int)$results[0];

    if ($count <= $maxRequests) {
        echo "  Request " . ($i + 1) . ": ALLOWED (count={$count}/{$maxRequests})\n";
    } else {
        echo "  Request " . ($i + 1) . ": DENIED  (count={$count}/{$maxRequests})\n";
    }
}

$elapsed = microtime(true) - $start;
echo "Rate limiter time: " . number_format($elapsed, 3) . "s\n\n";

echo "--- 6. Distributed lock pattern ---\n";
$start = microtime(true);

$lockKey = 'lock:resource:a';
$lockTtl = 10;
$holder = bin2hex(random_bytes(8));

$client->set($lockKey, $holder, ttl: $lockTtl)->await();
echo "  Lock acquired: {$lockKey} (holder={$holder})\n";

$isLocked = $client->get($lockKey)->await();
echo "  Lock check: " . ($isLocked === $holder ? 'HELD' : 'STOLEN') . "\n";

$client->del($lockKey)->await();
echo "  Lock released\n";

$isGone = $client->get($lockKey)->await();
echo "  Lock deleted: " . ($isGone === null ? 'YES' : 'NO') . "\n";

$elapsed = microtime(true) - $start;
echo "Lock time: " . number_format($elapsed, 3) . "s\n\n";

echo "Done.\n";
