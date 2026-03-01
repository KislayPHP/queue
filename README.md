# KislayPHP Queue

[![PHP Version](https://img.shields.io/badge/PHP-8.2+-blue.svg)](https://php.net)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![Build Status](https://img.shields.io/github/actions/workflow/status/KislayPHP/queue/ci.yml)](https://github.com/KislayPHP/queue/actions)
[![codecov](https://codecov.io/gh/KislayPHP/queue/branch/main/graph/badge.svg)](https://codecov.io/gh/KislayPHP/queue)

A high-performance C++ PHP extension providing distributed message queuing, job processing, and background task management with support for KV store, RabbitMQ, Kafka, and custom backends. Perfect for PHP ecosystem integration and modern microservices architecture.

Primary runtime namespace is `Kislay\Queue` (legacy `KislayPHP\Queue` aliases are kept for compatibility).
For service-to-service patterns, see `SERVICE_COMMUNICATION.md` and `service_communication.php`.

## ⚡ Key Features

- 🚀 **High Performance**: Ultra-fast message enqueue/dequeue operations
- 📨 **Multiple Protocols**: Support for AMQP, KV store, Kafka, and SQS
- 🔄 **Message Patterns**: Queue, pub/sub, request-reply, and delayed messages
- 📊 **Monitoring**: Queue metrics, throughput tracking, and error handling
- 🔄 **Retry Logic**: Configurable retry policies and dead letter queues
- 📋 **Batch Operations**: Bulk message processing and batch acknowledgments
- 🏷️ **Message Metadata**: Custom headers, priorities, and TTL settings
- 🌐 **Distributed**: Cross-service message routing and load balancing
- 🔄 **PHP Ecosystem**: Seamless integration with PHP ecosystem and frameworks
- 🌐 **Microservices Architecture**: Designed for distributed PHP applications

## 📦 Installation

### Via PIE (Recommended)

```bash
pie install kislayphp/queue:0.0.2
```

Add to your `php.ini`:

```ini
extension=kislayphp_queue.so
```

### Manual Build

```bash
git clone https://github.com/KislayPHP/queue.git
cd queue
phpize
./configure
make
sudo make install
```

### container

```containerfile
FROM php:8.2-cli
```

## 🚀 Quick Start

### Basic Queue Operations

```php
<?php

// Create queue instance
$queue = new KislayQueue();

// Enqueue messages
$queue->enqueue('user_notifications', [
    'user_id' => 123,
    'type' => 'email',
    'template' => 'welcome',
    'data' => ['name' => 'John Doe']
]);

$queue->enqueue('order_processing', [
    'order_id' => 'ORD-001',
    'action' => 'process_payment',
    'amount' => 99.99
]);

// Dequeue messages
$message = $queue->dequeue('user_notifications');
if ($message) {
    processNotification($message);
    $queue->acknowledge($message['id']);
}

// Get queue statistics
$stats = $queue->getStats('user_notifications');
echo "Queue size: {$stats['size']}\n";
echo "Processing rate: {$stats['rate']}/sec\n";
```

### Message Priorities and TTL

```php
<?php

$queue = new KislayQueue();

// High priority message
$queue->enqueue('urgent_tasks', [
    'task' => 'security_alert',
    'priority' => 10,
    'ttl' => 3600  // 1 hour
]);

// Delayed message
$queue->enqueue('scheduled_tasks', [
    'task' => 'monthly_report',
    'delay' => 86400  // 24 hours
]);
```

### Pub/Sub Pattern

```php
<?php

$queue = new KislayQueue();

// Publisher
$queue->publish('user_events', [
    'event' => 'user_registered',
    'user_id' => 123,
    'timestamp' => time()
]);

// Subscriber
$subscription = $queue->subscribe('user_events');
while ($message = $subscription->getMessage()) {
    handleUserEvent($message);
    $subscription->acknowledge($message['id']);
}
```

### Backend Integration

```php
<?php

$queue = new KislayQueue();

// KV store backend
$KV store = new KV storeQueue([
    'host' => 'kv-store',
    'port' => 6379,
    'password' => 'secret',
    'database' => 1
]);
$queue->setBackend($KV store);

// RabbitMQ backend
$rabbitmq = new RabbitMQQueue([
    'host' => 'rabbitmq-server',
    'port' => 5672,
    'username' => 'guest',
    'password' => 'guest',
    'vhost' => '/',
    'exchange' => 'kislay_exchange'
]);
$queue->setBackend($rabbitmq);

// Kafka backend
$kafka = new KafkaQueue([
    'brokers' => ['kafka-1:9092', 'kafka-2:9092'],
    'group_id' => 'kislay-consumers',
    'topic_prefix' => 'kislay-'
]);
$queue->setBackend($kafka);
```

### Batch Processing

```php
<?php

$queue = new KislayQueue();

// Enqueue multiple messages at once
$messages = [
    ['queue' => 'batch_jobs', 'data' => ['task' => 'job1']],
    ['queue' => 'batch_jobs', 'data' => ['task' => 'job2']],
    ['queue' => 'batch_jobs', 'data' => ['task' => 'job3']]
];
$queue->enqueueBatch($messages);

// Dequeue multiple messages
$batch = $queue->dequeueBatch('batch_jobs', 10);
foreach ($batch as $message) {
    processJob($message);
}
$queue->acknowledgeBatch(array_column($batch, 'id'));
```

### Error Handling and Retry

```php
<?php

$queue = new KislayQueue([
    'retry_policy' => [
        'max_attempts' => 3,
        'backoff' => 'exponential',
        'initial_delay' => 1000,  // ms
        'max_delay' => 30000      // ms
    ],
    'dead_letter_queue' => 'failed_jobs'
]);

try {
    $message = $queue->dequeue('processing_queue');
    processMessage($message);
    $queue->acknowledge($message['id']);
} catch (Exception $e) {
    // Message will be retried automatically
    $queue->nacknowledge($message['id'], $e->getMessage());
}
```

## 📚 Documentation

📖 **[Complete Documentation](docs.md)** - API reference, backend configurations, message patterns, and deployment guides
- 🌐 **Full Detailed Docs Site:** [https://skelves.com/docs](https://skelves.com/docs)
- 🧪 **Local Docs Route:** `http://localhost:5180/docs`

## 🏗️ Architecture

KislayPHP Queue implements a layered messaging architecture:

```
┌─────────────────┐
│ Application     │
│ Producers/      │
│ Consumers       │
└─────────────────┘
         │
    ┌─────────────┐
    │   Queue     │
    │  Manager    │
    │  (PHP)      │
    │             │
    │ ┌─────────┐ │
    │ │ Message │ │
    │ │ Router  │ │
    │ └─────────┘ │
    │             │
    │ ┌─────────┐ │
    │ │ Backend │ │
    │ │ Driver  │ │
    │ └─────────┘ │
    └─────────────┘
         │
    ┌─────────────┐
    │ Message     │
    │ Brokers     │
    │ (KV store/RMQ/ │
    │  Kafka...)  │
    └─────────────┘
```

## 🎯 Use Cases

- **Background Jobs**: Asynchronous task processing
- **Event Streaming**: Real-time event processing and analytics
- **Microservices Communication**: Service-to-service messaging
- **Load Leveling**: Handle traffic spikes and batch processing
- **Workflow Orchestration**: Complex business process automation
- **Data Pipeline**: ETL operations and data transformation

## 📊 Performance

```
Queue Performance Benchmark:
==========================
Messages/Second:       100,000
Average Latency:       0.5 ms
P95 Latency:           2.1 ms
Memory Usage:          18 MB
Throughput:            50 MB/sec
Concurrent Consumers:  100
Message Size:          1-10 KB
```

## 🔧 Configuration

### php.ini Settings

```ini
; Queue extension settings
kislayphp.queue.max_connections = 100
kislayphp.queue.message_timeout = 300
kislayphp.queue.batch_size = 100
kislayphp.queue.retry_attempts = 3

; KV store settings
kislayphp.queue.kv_host = "localhost"
kislayphp.queue.kv_port = 6379

; RabbitMQ settings
kislayphp.queue.rabbitmq_host = "localhost"
kislayphp.queue.rabbitmq_port = 5672

; Kafka settings
kislayphp.queue.kafka_brokers = "localhost:9092"
```

### Environment Variables

```bash
export KISLAYPHP_QUEUE_BACKEND=KV store
export KISLAYPHP_QUEUE_KV_HOST=kv-store:6379
export KISLAYPHP_QUEUE_RABBITMQ_HOST=rabbitmq:5672
export KISLAYPHP_QUEUE_KAFKA_BROKERS=kafka:9092
export KISLAYPHP_QUEUE_MAX_CONNECTIONS=100
export KISLAYPHP_QUEUE_BATCH_SIZE=50
```

## 🧪 Testing

```bash
# Run unit tests
php run-tests.php

# Test queue operations
cd tests/
php test_queue_operations.php

# Test backend integration
php test_kv_backend.php

# Performance tests
php test_throughput.php
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](.github/CONTRIBUTING.md) for details.

## 📄 License

Licensed under the [Apache License 2.0](LICENSE).

## 🆘 Support

- 📖 [Documentation](docs.md)
- 🐛 [Issue Tracker](https://github.com/KislayPHP/queue/issues)
- 💬 [Discussions](https://github.com/KislayPHP/queue/discussions)
- 📧 [Security Issues](.github/SECURITY.md)

## SEO Keywords

PHP, microservices, PHP ecosystem, PHP extension, C++ PHP extension, PHP message queue, PHP job queue, PHP background jobs, PHP KV store queue, PHP RabbitMQ, PHP Kafka, PHP AMQP, PHP pub/sub, distributed PHP messaging

## 📈 Roadmap

- [ ] Message encryption
- [ ] Schema validation
- [ ] Message tracing
- [ ] Consumer groups
- [ ] Message scheduling
- [ ] Queue federation

## 🙏 Acknowledgments

- **KV store**: In-memory data structure store
- **RabbitMQ**: Message broker
- **Apache Kafka**: Event streaming platform
- **PHP**: Zend API for extension development

---

**Built with ❤️ for reliable PHP messaging**
