--TEST--
Kislay Queue: push/consume/ack happy path, nack requeue with attempts, and dead-lettering past max_attempts
--EXTENSIONS--
kislayphp_queue
--FILE--
<?php
require __DIR__ . '/server_helper.inc';

$host = '127.0.0.1';
$port = reserve_free_port();
$bootstrap = <<<'PHP'
$server = new Kislay\Queue\Server(['host' => '__HOST__', 'port' => __PORT__]);
$server->listen('__HOST__', __PORT__);
$server->declare('jobs', ['max_attempts' => 2, 'retry_backoff_ms' => 0]);
$server->run();
PHP;
$bootstrap = strtr($bootstrap, ['__HOST__' => $host, '__PORT__' => (string) $port]);
$server = start_kislay_queue_server($bootstrap, $host, $port);

try {
    $client = new Kislay\Queue\Client("http://{$host}:{$port}");

    // --- happy path: push then consume-and-ack ---
    $jobId = $client->push('jobs', ['n' => 1]);
    echo "push returned a job id: " . (is_string($jobId) && $jobId !== '' ? 'yes' : 'no') . "\n";

    $worker = new Kislay\Queue\Worker("http://{$host}:{$port}");
    $seen = [];
    $worker->consume('jobs', function ($job) use (&$seen) {
        $seen[] = $job->payload();
        return true; // ack
    }, ['poll_interval_ms' => 50, 'stop_when_empty' => true]);
    echo "acked job payload: " . json_encode($seen[0] ?? null) . "\n";

    $stats = $client->stats('jobs');
    echo "stats after ack - ready: {$stats['ready']}, acked_total: {$stats['acked_total']}\n";

    // --- nack path: reject once (requeue), then ack on the retry ---
    $client->push('jobs', ['n' => 2]);
    $attempts = [];
    $worker->consume('jobs', function ($job) use (&$attempts) {
        $attempts[] = $job->attempts();
        if (count($attempts) === 1) {
            return false; // nack -> requeue
        }
        return true; // ack on the second delivery
    }, ['poll_interval_ms' => 50, 'stop_when_empty' => true]);
    echo "attempts seen for the requeued job: " . implode(',', $attempts) . "\n";

    $stats = $client->stats('jobs');
    echo "stats after nack+ack - nacked_total: {$stats['nacked_total']}, retried_total: {$stats['retried_total']}, acked_total: {$stats['acked_total']}\n";

    // --- dead-letter path: max_attempts=2, so nack-nack moves it to jobs.dlq ---
    $client->push('jobs', ['n' => 3]);
    $dlqAttempts = 0;
    $worker->consume('jobs', function ($job) use (&$dlqAttempts) {
        $dlqAttempts++;
        return false; // always nack - should dead-letter after 2 attempts
    }, ['poll_interval_ms' => 50, 'stop_when_empty' => true]);
    echo "attempts before dead-lettering: {$dlqAttempts}\n";

    $stats = $client->stats('jobs');
    echo "source queue ready after dead-lettering: {$stats['ready']}\n";
    echo "source queue dead_lettered_total: {$stats['dead_lettered_total']}\n";

    $dlqStats = $client->stats('jobs.dlq');
    echo "jobs.dlq ready count: {$dlqStats['ready']}\n";

    // --- pushBatch ---
    $ids = $client->pushBatch('jobs', [['n' => 10], ['n' => 11], ['n' => 12]]);
    echo "pushBatch returned 3 ids: " . (count($ids) === 3 ? 'yes' : 'no') . "\n";
    $statsAfterBatch = $client->stats('jobs');
    echo "ready after batch push: {$statsAfterBatch['ready']}\n";

    // --- purge ---
    $removed = $client->purge('jobs');
    echo "purge removed: {$removed}\n";
    $statsAfterPurge = $client->stats('jobs');
    echo "ready after purge: {$statsAfterPurge['ready']}\n";
} finally {
    stop_kislay_queue_server($server);
}
?>
--EXPECT--
push returned a job id: yes
acked job payload: {"n":1}
stats after ack - ready: 0, acked_total: 1
attempts seen for the requeued job: 1,2
stats after nack+ack - nacked_total: 1, retried_total: 1, acked_total: 2
attempts before dead-lettering: 2
source queue ready after dead-lettering: 0
source queue dead_lettered_total: 1
jobs.dlq ready count: 1
pushBatch returned 3 ids: yes
ready after batch push: 3
purge removed: 3
ready after purge: 0
