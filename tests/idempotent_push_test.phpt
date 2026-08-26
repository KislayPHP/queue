--TEST--
Kislay Queue: push with an explicit idempotency_key is deduplicated server-side; a fresh key is not
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

    // Same idempotency_key sent twice (simulating the automatic
    // reconnect-retry in kislay_http_request() resending the same body after
    // a send-succeeded-but-recv-failed round trip): the server must recognize
    // the repeat and return the SAME job id, not create a second job.
    $firstId = $client->push('jobs', ['n' => 1], ['idempotency_key' => 'fixed-key-1']);
    $secondId = $client->push('jobs', ['n' => 1], ['idempotency_key' => 'fixed-key-1']);
    echo "repeated idempotency_key returns the same job id: " . var_export($firstId === $secondId, true) . "\n";

    $stats = $client->stats('jobs');
    echo "ready after one push repeated twice with the same key: {$stats['ready']}\n";
    echo "pushed_total (jobs actually created): {$stats['pushed_total']}\n";
    // push_requests_total counts genuinely NEW pushes (cache misses), not raw
    // HTTP requests received - the repeat above was a cache hit and correctly
    // does not bump it a second time.
    echo "push_requests_total (distinct pushes, not counting the idempotent replay): {$stats['push_requests_total']}\n";

    // A different idempotency_key (or none, letting the client mint a fresh
    // one per call) must NOT be deduplicated against the above - this is a
    // genuinely new logical push.
    $thirdId = $client->push('jobs', ['n' => 2]);
    echo "a push with a different/no key gets its own job id: " . var_export($thirdId !== $firstId, true) . "\n";

    $stats = $client->stats('jobs');
    echo "ready after the second, distinct push: {$stats['ready']}\n";
    echo "pushed_total after the second, distinct push: {$stats['pushed_total']}\n";
} finally {
    stop_kislay_queue_server($server);
}
?>
--EXPECT--
repeated idempotency_key returns the same job id: true
ready after one push repeated twice with the same key: 1
pushed_total (jobs actually created): 1
push_requests_total (distinct pushes, not counting the idempotent replay): 1
a push with a different/no key gets its own job id: true
ready after the second, distinct push: 2
pushed_total after the second, distinct push: 2
