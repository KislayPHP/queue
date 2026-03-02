# KislayPHP Queue

Queue runtime for KislayPHP.

Primary runtime namespace is `Kislay\Queue` with backward-compatible aliases under `KislayPHP\Queue`.

## Concurrency Mode

- Default API mode is synchronous.
- Calls return concrete values (`bool`, payload, `int`) and do not expose Promise/Fiber APIs.
- Optional RPC transport is supported internally when built with stubs, but method semantics remain synchronous.

This module is intentionally sync-first to keep queue processing deterministic in long-lived PHP workers.

## Installation

```bash
pie install kislayphp/queue:0.0.2
```

Enable in `php.ini`:

```ini
extension=kislayphp_queue.so
```

## Public API

`Kislay\Queue\Queue`:

- `__construct()`
- `setClient(Kislay\Queue\ClientInterface $client): bool`
- `enqueue(string $queue, mixed $payload): bool`
- `dequeue(string $queue): mixed`
- `peek(string $queue): mixed`
- `size(string $queue): int`
- `clear(string $queue): int`

`Kislay\Queue\ClientInterface`:

- `enqueue(string $queue, mixed $payload): bool`
- `dequeue(string $queue): mixed`
- `size(string $queue): int`

Legacy aliases:

- `KislayPHP\Queue\Queue`
- `KislayPHP\Queue\ClientInterface`

## Quick Start

```php
<?php

$queue = new Kislay\Queue\Queue();

$queue->enqueue('jobs', ['id' => 1, 'task' => 'send_email']);
$queue->enqueue('jobs', ['id' => 2, 'task' => 'reindex']);

$next = $queue->dequeue('jobs');
var_dump($next);

$remaining = $queue->size('jobs');
var_dump($remaining);
```

## Notes

- Use this extension for in-process queue semantics and adapter integration points.
- If you need event-driven fanout or socket transport, use `kislayphp/eventbus`.
- For full ecosystem communication guidelines, see `SERVICE_COMMUNICATION.md`.

