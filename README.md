# Kislay Queue

[![PHP Version](https://img.shields.io/badge/PHP-8.2%2B-blue.svg)](https://php.net)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![Build Status](https://img.shields.io/github/actions/workflow/status/KislayPHP/queue/ci.yml?branch=main&label=CI)](https://github.com/KislayPHP/queue/actions)
[![PIE](https://img.shields.io/badge/install-pie-blueviolet)](https://github.com/php/pie)

> Native PHP distributed job queue for long-running services. Phase 1 ships a standalone queue server, producer client, worker client, retries, delayed jobs, and DLQ support.

Part of the [KislayPHP ecosystem](https://skelves.com/kislayphp/docs).

## What It Is

`kislayphp/queue` now has two modes:

- `Kislay\Queue\Server` for the standalone queue node
- `Kislay\Queue\Client` for producers and operational reads
- `Kislay\Queue\Worker` for consumers
- `Kislay\Queue\Job` for ack/nack control inside a handler
- `Kislay\Queue\Queue` as the legacy local in-process queue for development fallback

Delivery model in `0.0.4`:

- at-least-once delivery
- one leased job per worker fetch
- retries with backoff
- delayed jobs
- dead-letter queue support
- in-memory server state only

## Install

```bash
pie install kislayphp/queue:0.0.4
```

Enable in `php.ini`:

```ini
extension=kislayphp_queue.so
```

## 5-Minute Quickstart

### 1. Start the queue server

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

### 2. Push a job

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

var_dump($jobId);
```

### 3. Run a worker

```php
<?php
$worker = new Kislay\Queue\Worker('http://127.0.0.1:9020');

$worker->consume('emails', function (Kislay\Queue\Job $job) {
    $payload = $job->payload();
    $headers = $job->headers();

    sendEmail($payload['to'], $payload['subject']);

    return true;
}, [
    'worker_id' => 'emails-worker-1',
    'lease_ms' => 30000,
]);
```

## Architecture

```text
+------------------+        push         +----------------------+
| Producer Service | ------------------> | Kislay\Queue\Server |
| Client           |                     | owns queue state     |
+------------------+                     | leases jobs          |
                                         | retries / DLQ        |
+------------------+        fetch        +----------+-----------+
| Worker Service   | <------------------------------+
| Kislay\Queue\Worker |         ack / nack          |
+------------------+                                 |
                                                     v
                                            +------------------+
                                            | queue.dlq        |
                                            +------------------+
```

## Public API

```php
namespace Kislay\Queue;

class Server {
    public function __construct(array $options = []);
    public function listen(string $host, int $port): bool;
    public function run(): void;
    public function stop(): bool;
    public function declare(string $queue, ?array $options = null): bool;
    public function stats(?string $queue = null): array;
}

class Client {
    public function __construct(string $baseUrl, array $options = []);
    public function push(string $queue, mixed $payload, ?array $options = null): string;
    public function pushBatch(string $queue, array $jobs): array;
    public function stats(string $queue): array;
    public function purge(string $queue): int;
}

class Worker {
    public function __construct(string $baseUrl, array $options = []);
    public function consume(string $queue, callable $handler, ?array $options = null): bool;
    public function stop(): bool;
}

class Job {
    public function id(): string;
    public function queue(): string;
    public function payload(): mixed;
    public function headers(): array;
    public function attempts(): int;
    public function maxAttempts(): int;
    public function availableAt(): int;
    public function ack(): bool;
    public function nack(?bool $requeue = true, ?int $delayMs = null): bool;
    public function release(?int $delayMs = null): bool;
}
```

Legacy local queue API remains available:

```php
$queue = new Kislay\Queue\Queue();
$queue->enqueue('jobs', ['task' => 'send_email']);
$job = $queue->dequeue('jobs');
```

## Queue Semantics

`0.0.4` semantics are intentionally explicit:

- `push()` creates a job in `ready` or `delayed`
- `Worker::consume()` fetches one leased job at a time
- if the handler returns `true`, the worker acks the job
- if the handler returns `false`, the worker nacks the job
- if a handler throws, the worker nacks the job and stops with the exception still raised
- if retries are exhausted, the job moves to the configured DLQ

## Operational Notes

What `0.0.4` is good for:

- background task processing between services
- queue semantics for microservice workloads
- development and early production validation
- explicit retry / DLQ flows

What `0.0.4` does **not** do yet:

- durable persistence across queue server restarts
- worker concurrency inside one `consume()` loop
- distributed replication
- exactly-once delivery

If the queue server process stops, in-memory jobs are lost. That is the correct tradeoff for this phase; do not market it as durable yet.

## End-to-End Example

See [`example.php`](example.php) for a minimal runnable server / producer / worker flow.

## Ecosystem Fit

Use this module for job queue semantics.

Use [eventbus](https://github.com/KislayPHP/eventbus) for fanout and realtime pub/sub.

Typical flow:

```text
HTTP request -> queue push -> worker processes job -> eventbus emits completion event
```

## Docs

- Full docs: [https://skelves.com/kislayphp/docs/queue](https://skelves.com/kislayphp/docs/queue)
- Insight articles: [https://skelves.com/insights](https://skelves.com/insights)

## License

[Apache License 2.0](LICENSE)
