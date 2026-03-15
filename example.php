<?php

function fail(string $message): void {
    fwrite(STDERR, "FAIL: {$message}\n");
    exit(1);
}

if (!extension_loaded('kislayphp_queue')) {
    fail('kislayphp_queue not loaded');
}

$mode = $argv[1] ?? 'help';

if ($mode === 'server') {
    $server = new Kislay\Queue\Server();
    $server->declare('emails', [
        'visibility_timeout_ms' => 30000,
        'max_attempts' => 5,
        'retry_backoff_ms' => 1000,
        'dead_letter_queue' => 'emails.dlq',
    ]);
    $server->listen('127.0.0.1', 19020);
    echo "Queue server listening on http://127.0.0.1:19020\n";
    $server->run();
    exit(0);
}

if ($mode === 'producer') {
    $client = new Kislay\Queue\Client('http://127.0.0.1:19020');
    $jobId = $client->push('emails', [
        'to' => 'user@example.com',
        'subject' => 'Welcome',
    ], [
        'headers' => ['trace_id' => 'trace-1'],
        'max_attempts' => 5,
    ]);
    echo "Pushed job {$jobId}\n";
    print_r($client->stats('emails'));
    exit(0);
}

if ($mode === 'worker') {
    $worker = new Kislay\Queue\Worker('http://127.0.0.1:19020');
    $worker->consume('emails', function (Kislay\Queue\Job $job) {
        echo 'Processing job ' . $job->id() . PHP_EOL;
        echo json_encode([
            'payload' => $job->payload(),
            'headers' => $job->headers(),
            'attempts' => $job->attempts(),
        ], JSON_PRETTY_PRINT) . PHP_EOL;
        return true;
    }, [
        'worker_id' => 'example-worker',
        'lease_ms' => 30000,
        'stop_when_empty' => true,
    ]);
    exit(0);
}

if ($mode === 'local') {
    $queue = new Kislay\Queue\Queue();
    $queue->enqueue('jobs', ['id' => 1, 'task' => 'email']);
    $queue->enqueue('jobs', ['id' => 2, 'task' => 'sms']);

    if ($queue->size('jobs') !== 2) {
        fail('local queue size mismatch');
    }

    $job = $queue->dequeue('jobs');
    if (!is_array($job) || ($job['id'] ?? null) !== 1) {
        fail('local dequeue returned unexpected payload');
    }

    echo "OK: local queue fallback works\n";
    exit(0);
}

echo <<<TXT
Usage:
  php -d extension=modules/kislayphp_queue.so example.php server
  php -d extension=modules/kislayphp_queue.so example.php producer
  php -d extension=modules/kislayphp_queue.so example.php worker
  php -d extension=modules/kislayphp_queue.so example.php local
TXT;
