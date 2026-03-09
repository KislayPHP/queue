# KislayQueue

> In-process message queue extension for KislayPHP — reliable enqueue/dequeue with TTL, priority, delay, dead-letter queues, and pub/sub.

[![PHP Version](https://img.shields.io/badge/PHP-8.2+-blue.svg)](https://php.net)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)

## Installation

**Via PIE (recommended):**
```bash
pie install kislayphp/queue:0.0.2
```

Add to `php.ini`:
```ini
extension=kislayphp_queue.so
```

**Build from source:**
```bash
git clone https://github.com/KislayPHP/queue.git
cd queue && phpize && ./configure --enable-kislayphp_queue && make && sudo make install
```

## Requirements

- PHP 8.2+
- kislayphp/eventbus for pub/sub fanout (optional)

## Quick Start

```php
<?php
$queue = new Kislay\Queue\Queue();

// Enqueue jobs
$queue->enqueue('emails', ['to' => 'user@example.com', 'subject' => 'Welcome']);
$queue->enqueue('emails', ['to' => 'admin@example.com', 'subject' => 'Alert'], ttlMs: 60000);

// Process jobs
while ($job = $queue->dequeue('emails')) {
    send_email($job['to'], $job['subject']);
    $queue->ack('emails');
}

echo "Remaining: " . $queue->size('emails') . "\n";
```

## API Reference

### `Queue`

#### `__construct()`
Creates a new Queue instance. Use `setClient()` to delegate to a remote backend.

#### `setClient(Kislay\Queue\ClientInterface $client): bool`
Delegates queue operations to a remote client implementation.

#### `enqueue(string $name, mixed $payload, ?int $ttlMs = null, ?int $priority = null, ?int $delayMs = null): bool`
Adds a message to the named queue.
- `$name` — queue name, e.g. `'emails'`
- `$payload` — any serializable value (array, string, int, …)
- `$ttlMs` — message time-to-live in milliseconds; `null` = no expiry
- `$priority` — higher value = higher priority; `null` = FIFO order
- `$delayMs` — delay before the message becomes visible; `null` = immediate
- Returns `true` on success

#### `dequeue(string $name): mixed`
Removes and returns the next available message from the queue.
- Returns `null` if the queue is empty or no message is currently visible
- Message is in-flight until `ack()` or `nack()` is called

#### `dequeueWithId(string $name): ?array`
Like `dequeue()` but returns `['id' => string, 'payload' => mixed]`, enabling explicit ACK/NACK by ID.

#### `ack(string $name, ?string $id = null): bool`
Acknowledges successful processing. Removes the message permanently.
- `$id` — message ID from `dequeueWithId()`; omit to ACK the last dequeued message

#### `nack(string $name, ?string $id = null, bool $requeue = true): bool`
Negative-acknowledges a message. Requeues it if `$requeue = true`, or routes it to the DLQ.

#### `peek(string $name): mixed`
Returns the next message payload without removing it.

#### `size(string $name): int`
Returns the number of messages currently in the queue (including delayed and in-flight).

#### `subscribe(string $name, callable $handler): bool`
Registers a handler called for every new message. Non-blocking; messages are dispatched as they arrive.
- Signature: `function(mixed $payload, string $id): void`

#### `setDLQ(string $name, string $dlqName): bool`
Configures a dead-letter queue. Messages that are `nack()`-ed with `$requeue = false` or that expire are moved to `$dlqName`.

#### `purgeExpired(string $name): int`
Removes all expired (past-TTL) messages. Returns the number removed.

#### `clear(string $name): int`
Removes all messages from the queue. Returns the count removed.

---

### `ClientInterface`

| Method | Signature | Description |
|--------|-----------|-------------|
| `enqueue` | `enqueue(string $queue, mixed $payload): bool` | Add a message |
| `dequeue` | `dequeue(string $queue): mixed` | Remove and return next message |
| `size` | `size(string $queue): int` | Count messages |

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KISLAY_QUEUE_DEFAULT_TTL_MS` | `0` (no expiry) | Default TTL for messages without explicit TTL |
| `KISLAY_QUEUE_MAX_SIZE` | `0` (unlimited) | Max messages per queue before `enqueue()` returns `false` |
| `KISLAY_RPC_ENABLED` | `0` | Enable RPC transport for remote queue operations |
| `KISLAY_RPC_TIMEOUT_MS` | `200` | RPC call timeout |

## Examples

### Priority Queue

```php
<?php
$queue = new Kislay\Queue\Queue();

$queue->enqueue('tasks', ['job' => 'low-priority'],  priority: 1);
$queue->enqueue('tasks', ['job' => 'high-priority'], priority: 10);
$queue->enqueue('tasks', ['job' => 'urgent'],        priority: 100);

// Dequeues 'urgent' first
$task = $queue->dequeue('tasks');
```

### Dead-Letter Queue

```php
<?php
$queue = new Kislay\Queue\Queue();
$queue->setDLQ('jobs', 'jobs.failed');

$msg = $queue->dequeueWithId('jobs');
if ($msg) {
    try {
        process($msg['payload']);
        $queue->ack('jobs', $msg['id']);
    } catch (Throwable $e) {
        $queue->nack('jobs', $msg['id'], requeue: false); // moves to jobs.failed
    }
}
```

### Delayed Job

```php
$queue->enqueue('notifications', ['user' => 42, 'type' => 'reminder'], delayMs: 3600000); // 1 hour
```

### Subscribe Pattern

```php
$queue->subscribe('events', function (mixed $payload, string $id) use ($queue) {
    process($payload);
    $queue->ack('events', $id);
});
```

## Related Extensions

| Extension | Use Case |
|-----------|----------|
| [kislayphp/eventbus](https://github.com/KislayPHP/eventbus) | Event fanout / pub-sub (use instead of Queue for broadcast) |
| [kislayphp/core](https://github.com/KislayPHP/core) | Runs the worker processes consuming the queue |
| [kislayphp/metrics](https://github.com/KislayPHP/metrics) | Track queue depth and processing rate |

## License

Licensed under the [Apache License 2.0](LICENSE).
