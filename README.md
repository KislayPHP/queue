# KislayPHP Queue

[![PHP Version](https://img.shields.io/badge/PHP-8.2%2B-blue.svg)](https://php.net)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![Build Status](https://img.shields.io/github/actions/workflow/status/KislayPHP/queue/ci.yml?branch=main&label=CI)](https://github.com/KislayPHP/queue/actions)
[![PIE](https://img.shields.io/badge/install-pie-blueviolet)](https://github.com/php/pie)

> **Deterministic message queue for PHP microservices.** In-process queue with adapter interface for Redis, SQS, or any backend. Sync-first API for reliable task processing.

Part of the [KislayPHP ecosystem](https://skelves.com/kislayphp/docs).

---

## ✨ What It Does

`kislayphp/queue` provides a typed message queue primitive with a clean adapter interface. Use the built-in in-memory queue for development, or swap in Redis, SQS, or RabbitMQ for production without changing application code.

```php
<?php
$queue = new Kislay\Queue\Queue();
$queue->enqueue('jobs', ['task' => 'send_email', 'to' => 'user@example.com']);

$job = $queue->dequeue('jobs');
// process $job...
```

---

## 📦 Installation

```bash
pie install kislayphp/queue
```

Enable in `php.ini`:
```ini
extension=kislayphp_queue.so
```

---

## 🚀 Quick Start

### In-Process Queue

```php
<?php
$queue = new Kislay\Queue\Queue();

// Producer: add jobs
$queue->enqueue('email-jobs', ['to' => 'alice@example.com', 'template' => 'welcome']);
$queue->enqueue('email-jobs', ['to' => 'bob@example.com',   'template' => 'invoice']);

// Consumer: process jobs
while ($job = $queue->dequeue('email-jobs')) {
    send_email($job['to'], $job['template']);
}

echo $queue->size('email-jobs');  // 0
```

### Custom Backend Adapter

Swap to Redis, SQS, etc. without changing your application:

```php
<?php
class RedisQueueClient implements Kislay\Queue\ClientInterface {
    public function __construct(private Redis $redis) {}

    public function enqueue(string $queue, mixed $payload): bool {
        return (bool) $this->redis->rpush($queue, serialize($payload));
    }

    public function dequeue(string $queue): mixed {
        $raw = $this->redis->lpop($queue);
        return $raw ? unserialize($raw) : null;
    }

    public function size(string $queue): int {
        return $this->redis->llen($queue);
    }
}

$queue = new Kislay\Queue\Queue();
$queue->setClient(new RedisQueueClient(new Redis()));

// Same API, backed by Redis
$queue->enqueue('tasks', ['id' => 123]);
```

---

## 📖 Public API

```php
namespace Kislay\Queue;

class Queue {
    public function __construct();
    public function setClient(ClientInterface $client): bool;
    public function enqueue(string $queue, mixed $payload): bool;
    public function dequeue(string $queue): mixed;
    public function peek(string $queue): mixed;
    public function size(string $queue): int;
    public function clear(string $queue): int;   // returns count cleared
}

interface ClientInterface {
    public function enqueue(string $queue, mixed $payload): bool;
    public function dequeue(string $queue): mixed;
    public function size(string $queue): int;
}
```

Legacy aliases: `KislayPHP\Queue\Queue`, `KislayPHP\Queue\ClientInterface`

---

## 💡 When to Use Each Extension

| Need | Use |
|---|---|
| In-process task queue | **queue** (this) |
| Realtime push / fanout to connected clients | [eventbus](https://github.com/KislayPHP/eventbus) |
| Background async computation | [core](https://github.com/KislayPHP/core) `async()` |

---

## 🔗 Ecosystem

[core](https://github.com/KislayPHP/core) · [gateway](https://github.com/KislayPHP/gateway) · [discovery](https://github.com/KislayPHP/discovery) · [metrics](https://github.com/KislayPHP/metrics) · **queue** · [eventbus](https://github.com/KislayPHP/eventbus)

## 📄 License

[Apache License 2.0](LICENSE) · **[Full Docs](https://skelves.com/kislayphp/docs)**
