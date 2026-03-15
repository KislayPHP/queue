# Kislay Queue Documentation

## Introduction

`kislayphp/queue` provides queue semantics for KislayPHP services using a standalone queue server plus remote clients and workers. The goal in `0.0.4` is to establish the correct distributed model first:

- standalone queue node
- remote producer client
- remote worker client
- ack / nack lifecycle
- retry / backoff
- dead-letter queue

The current implementation is intentionally in-memory. It is suitable for development, integration testing, and early production experimentation where you understand the restart tradeoff.

## Table of Contents

- [Architecture](#architecture)
- [Install](#install)
- [Quickstart](#quickstart)
- [Queue Configuration](#queue-configuration)
- [Producer API](#producer-api)
- [Worker API](#worker-api)
- [Delivery Guarantees](#delivery-guarantees)
- [Failure and DLQ Behavior](#failure-and-dlq-behavior)
- [Legacy Local Queue](#legacy-local-queue)
- [Troubleshooting](#troubleshooting)

## Architecture

```text
+------------------+        push         +----------------------+
| Producer Service | ------------------> | Kislay\Queue\Server |
| Client           |                     | queue state          |
+------------------+                     | ready / delayed      |
                                         | leased / DLQ         |
+------------------+        fetch        +----------+-----------+
| Worker Service   | <------------------------------+
| Worker           |         ack / nack             |
+------------------+                                 |
                                                     v
                                            +------------------+
                                            | queue.dlq        |
                                            +------------------+
```

## Install

```bash
pie install kislayphp/queue:0.0.4
```

Enable in `php.ini`:

```ini
extension=kislayphp_queue.so
```

## Quickstart

### Queue server

```php
<?php
$server = new Kislay\Queue\Server();
$server->declare('emails', [
    'visibility_timeout_ms' => 30000,
    'max_attempts' => 5,
    'retry_backoff_ms' => 1000,
    'dead_letter_queue' => 'emails.dlq',
]);
$server->listen('0.0.0.0', 9020);
$server->run();
```

### Producer

```php
<?php
$client = new Kislay\Queue\Client('http://127.0.0.1:9020');

$jobId = $client->push('emails', [
    'to' => 'user@example.com',
    'subject' => 'Welcome',
], [
    'headers' => ['trace_id' => 'trace-1'],
    'max_attempts' => 5,
]);
```

### Worker

```php
<?php
$worker = new Kislay\Queue\Worker('http://127.0.0.1:9020');

$worker->consume('emails', function (Kislay\Queue\Job $job) {
    $payload = $job->payload();

    sendEmail($payload['to'], $payload['subject']);

    return true;
}, [
    'worker_id' => 'emails-worker-1',
    'lease_ms' => 30000,
    'poll_interval_ms' => 250,
]);
```

## Queue Configuration

Available queue declaration options in `0.0.4`:

```php
$server->declare('emails', [
    'visibility_timeout_ms' => 30000,
    'max_attempts' => 5,
    'retry_backoff_ms' => 1000,
    'dead_letter_queue' => 'emails.dlq',
]);
```

Meaning:

- `visibility_timeout_ms`: how long a worker lease remains valid before the job becomes eligible for redelivery
- `max_attempts`: maximum delivery attempts before moving to the DLQ
- `retry_backoff_ms`: delay applied after a nack or lease timeout before the next retry
- `dead_letter_queue`: queue name used when retries are exhausted

## Producer API

### Push one job

```php
$jobId = $client->push('emails', ['to' => 'user@example.com'], [
    'headers' => ['trace_id' => 'abc123'],
    'delay_ms' => 5000,
    'max_attempts' => 3,
]);
```

### Push a batch

```php
$jobIds = $client->pushBatch('emails', [
    ['payload' => ['to' => 'a@example.com']],
    ['payload' => ['to' => 'b@example.com'], 'options' => ['delay_ms' => 1000]],
]);
```

### Queue stats

```php
$stats = $client->stats('emails');
```

### Purge a queue

```php
$removed = $client->purge('emails');
```

## Worker API

### Success path

If the handler returns `true`, the worker automatically acks the job.

```php
$worker->consume('emails', function (Kislay\Queue\Job $job) {
    processEmail($job->payload());
    return true;
}, ['worker_id' => 'emails-1']);
```

### Failure path

If the handler returns `false`, the worker automatically nacks the job.

```php
$worker->consume('emails', function (Kislay\Queue\Job $job) {
    return false;
});
```

### Manual control

```php
$worker->consume('emails', function (Kislay\Queue\Job $job) {
    $payload = $job->payload();

    if (!canSendNow($payload)) {
        $job->release(5000);
        return true;
    }

    if (!sendEmail($payload)) {
        $job->nack(true, 1000);
        return true;
    }

    return $job->ack();
});
```

## Delivery Guarantees

`0.0.4` uses at-least-once delivery.

That means:

- a job can be delivered more than once
- workers should be idempotent where needed
- a worker crash before ack can result in redelivery after the lease expires

This is the correct guarantee for the current implementation. Do not describe it as exactly-once.

## Failure and DLQ Behavior

When a worker fails a job:

1. the job is nacked
2. attempts are checked against `max_attempts`
3. if attempts remain, the job is requeued with backoff
4. if attempts are exhausted, the job moves to the DLQ

Example:

```php
$client->push('payments', ['amount' => 10], ['max_attempts' => 1]);
```

If a worker returns `false` on the first delivery, the job moves directly to `payments.dlq`.

## Legacy Local Queue

The old in-process queue is still available:

```php
$queue = new Kislay\Queue\Queue();
$queue->enqueue('jobs', ['task' => 'send_email']);
$job = $queue->dequeue('jobs');
```

Use it only as a local fallback. It is not the primary architecture for the distributed queue direction anymore.

## Troubleshooting

### `Client::push()` or `Worker::consume()` cannot reach the queue server

Check:

- the server is running
- the base URL is correct
- the port is open
- `/health` responds

```bash
curl http://127.0.0.1:9020/health
```

### Jobs disappear after a queue server restart

That is expected in `0.0.4`. The queue server is still in-memory only.

### Jobs are retried again after a worker crash

That is expected. The lease expired before an ack was received.

### DLQ stays empty

Check:

- `max_attempts`
- queue declaration for `dead_letter_queue`
- whether the handler is actually returning `false` or throwing
