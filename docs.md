# KislayPHP Queue Extension Documentation

## Overview

The KislayPHP Queue extension provides high-performance, in-memory message queuing capabilities with support for pub/sub patterns, persistent queues, and pluggable storage backends. It supports multiple queue types including FIFO, priority queues, and delayed message delivery.

## Architecture

### Queue Types
- **FIFO Queue**: First-in, first-out message delivery
- **Priority Queue**: Messages delivered based on priority levels
- **Delayed Queue**: Messages scheduled for future delivery
- **Persistent Queue**: Messages survive application restarts

### Storage Backends
The extension uses a pluggable storage interface:
- In-memory storage (default)
- Redis backend
- Database backend (MySQL/PostgreSQL)
- File-based storage
- Custom implementations

## Installation

### Via PIE
```bash
pie install kislayphp/queue
```

### Manual Build
```bash
cd kislayphp_queue/
phpize && ./configure --enable-kislayphp_queue && make && make install
```

### php.ini Configuration
```ini
extension=kislayphp_queue.so
```

## API Reference

### KislayPHP\\Queue\\QueueManager Class

The main queue management class.

#### Constructor
```php
$queueManager = new KislayPHP\\Queue\\QueueManager();
```

#### Queue Operations
```php
$queueManager->createQueue(string $name, array $options = []): bool
$queueManager->deleteQueue(string $name): bool
$queueManager->getQueue(string $name): QueueInterface
$queueManager->listQueues(): array
```

#### Storage Backend
```php
$queueManager->setStorage(KislayPHP\\Queue\\StorageInterface $storage): bool
```

### KislayPHP\\Queue\\QueueInterface

Interface for queue operations.

```php
interface QueueInterface {
    public function push(string $message, array $options = []): bool;
    public function pop(): ?string;
    public function peek(): ?string;
    public function size(): int;
    public function isEmpty(): bool;
    public function clear(): bool;
}
```

### KislayPHP\\Queue\\PriorityQueue Class

Priority-based message queue.

#### Constructor
```php
$priorityQueue = new KislayPHP\\Queue\\PriorityQueue(string $name);
```

#### Operations
```php
$priorityQueue->push(string $message, array $options = []): bool
$priorityQueue->pop(): ?string
$priorityQueue->size(): int
```

Options for push:
- `priority`: Integer priority level (higher numbers = higher priority)

### KislayPHP\\Queue\\DelayedQueue Class

Queue with delayed message delivery.

#### Constructor
```php
$delayedQueue = new KislayPHP\\Queue\\DelayedQueue(string $name);
```

#### Operations
```php
$delayedQueue->push(string $message, array $options = []): bool
$delayedQueue->pop(): ?string
$delayedQueue->size(): int
```

Options for push:
- `delay`: Delay in seconds before message becomes available

### KislayPHP\\Queue\\PubSub Class

Publish-subscribe messaging.

#### Constructor
```php
$pubsub = new KislayPHP\\Queue\\PubSub();
```

#### Operations
```php
$pubsub->publish(string $channel, string $message): bool
$pubsub->subscribe(string $channel, callable $callback): bool
$pubsub->unsubscribe(string $channel): bool
$pubsub->getChannels(): array
```

## Usage Examples

### Basic FIFO Queue
```php
<?php
use KislayPHP\\Queue\\QueueManager;

$queueManager = new QueueManager();
$queueManager->createQueue('tasks');

// Get queue instance
$queue = $queueManager->getQueue('tasks');

// Push messages
$queue->push('Process user registration');
$queue->push('Send welcome email');
$queue->push('Update user statistics');

// Process messages
while (!$queue->isEmpty()) {
    $message = $queue->pop();
    echo "Processing: $message\n";

    // Simulate processing
    processTask($message);
}

function processTask(string $message): void {
    // Task processing logic
    sleep(1); // Simulate work
    echo "Completed: $message\n";
}
```

### Priority Queue for Task Scheduling
```php
<?php
use KislayPHP\\Queue\\PriorityQueue;

$taskQueue = new PriorityQueue('urgent_tasks');

// Add tasks with different priorities
$taskQueue->push('Send password reset email', ['priority' => 10]); // High priority
$taskQueue->push('Process daily reports', ['priority' => 5]);     // Medium priority
$taskQueue->push('Clean up old logs', ['priority' => 1]);         // Low priority
$taskQueue->push('Send marketing newsletter', ['priority' => 3]); // Low-medium priority

// Process tasks in priority order
while (!$taskQueue->isEmpty()) {
    $task = $taskQueue->pop();
    echo "Processing high-priority task: $task\n";

    // Process the task
    processUrgentTask($task);
}

function processUrgentTask(string $task): void {
    // Urgent task processing logic
    echo "Urgently handling: $task\n";
}
```

### Delayed Message Delivery
```php
<?php
use KislayPHP\\Queue\\DelayedQueue;

$notificationQueue = new DelayedQueue('delayed_notifications');

// Schedule notifications
$notificationQueue->push('Welcome email to user@example.com', ['delay' => 300]);   // 5 minutes
$notificationQueue->push('Follow-up email to user@example.com', ['delay' => 3600]); // 1 hour
$notificationQueue->push('Survey email to user@example.com', ['delay' => 86400]);  // 24 hours

// Process delayed messages
$app->get('/process-notifications', function($req, $res) use ($notificationQueue) {
    $processed = 0;

    while (!$notificationQueue->isEmpty()) {
        $message = $notificationQueue->pop();
        if ($message) {
            sendNotification($message);
            $processed++;
        }
    }

    $res->json(['processed' => $processed]);
});

function sendNotification(string $message): void {
    // Send notification logic
    echo "Sending notification: $message\n";
}
```

### Publish-Subscribe Pattern
```php
<?php
use KislayPHP\\Queue\\PubSub;

$pubsub = new PubSub();

// Subscribe to channels
$pubsub->subscribe('user_events', function($message) {
    echo "User event received: $message\n";
    // Process user event
    processUserEvent($message);
});

$pubsub->subscribe('system_alerts', function($message) {
    echo "System alert: $message\n";
    // Handle system alert
    handleSystemAlert($message);
});

$pubsub->subscribe('order_updates', function($message) {
    echo "Order update: $message\n";
    // Update order status
    updateOrderStatus($message);
});

// Publish messages
$pubsub->publish('user_events', json_encode([
    'event' => 'user_registered',
    'user_id' => 12345,
    'timestamp' => time()
]));

$pubsub->publish('system_alerts', json_encode([
    'alert' => 'high_memory_usage',
    'usage' => '85%',
    'server' => 'web-01'
]));

$pubsub->publish('order_updates', json_encode([
    'order_id' => 'ORD-001',
    'status' => 'shipped',
    'tracking_number' => '1Z999AA1234567890'
]));

function processUserEvent(string $message): void {
    $data = json_decode($message, true);
    // Handle user event
}

function handleSystemAlert(string $message): void {
    $data = json_decode($message, true);
    // Handle system alert
}

function updateOrderStatus(string $message): void {
    $data = json_decode($message, true);
    // Update order status
}
```

### Job Queue with Retry Logic
```php
<?php
use KislayPHP\\Queue\\QueueManager;

class JobProcessor {
    private $queueManager;
    private $maxRetries = 3;

    public function __construct(QueueManager $queueManager) {
        $this->queueManager = $queueManager;
        $this->queueManager->createQueue('jobs');
        $this->queueManager->createQueue('failed_jobs');
    }

    public function addJob(string $jobType, array $payload, int $priority = 5): void {
        $job = [
            'id' => uniqid('job_', true),
            'type' => $jobType,
            'payload' => $payload,
            'priority' => $priority,
            'created_at' => time(),
            'attempts' => 0
        ];

        $queue = $this->queueManager->getQueue('jobs');
        $queue->push(json_encode($job), ['priority' => $priority]);
    }

    public function processJobs(): void {
        $queue = $this->queueManager->getQueue('jobs');
        $failedQueue = $this->queueManager->getQueue('failed_jobs');

        while (!$queue->isEmpty()) {
            $jobData = $queue->pop();
            $job = json_decode($jobData, true);

            try {
                $this->executeJob($job);
                echo "Job {$job['id']} completed successfully\n";
            } catch (Exception $e) {
                $job['attempts']++;
                $job['last_error'] = $e->getMessage();

                if ($job['attempts'] < $this->maxRetries) {
                    // Retry with exponential backoff
                    $delay = pow(2, $job['attempts']) * 60; // 2, 4, 8 minutes
                    $queue->push(json_encode($job), [
                        'priority' => $job['priority'],
                        'delay' => $delay
                    ]);
                    echo "Job {$job['id']} failed, retrying in {$delay} seconds\n";
                } else {
                    // Move to failed jobs queue
                    $failedQueue->push(json_encode($job));
                    echo "Job {$job['id']} failed permanently\n";
                }
            }
        }
    }

    private function executeJob(array $job): void {
        switch ($job['type']) {
            case 'send_email':
                $this->sendEmail($job['payload']);
                break;
            case 'process_payment':
                $this->processPayment($job['payload']);
                break;
            case 'generate_report':
                $this->generateReport($job['payload']);
                break;
            default:
                throw new Exception("Unknown job type: {$job['type']}");
        }
    }

    private function sendEmail(array $payload): void {
        // Email sending logic
        if (!isset($payload['to']) || !isset($payload['subject'])) {
            throw new Exception("Invalid email payload");
        }
        // Simulate email sending
        sleep(1);
    }

    private function processPayment(array $payload): void {
        // Payment processing logic
        if (!isset($payload['amount']) || $payload['amount'] <= 0) {
            throw new Exception("Invalid payment amount");
        }
        // Simulate payment processing
        sleep(2);
    }

    private function generateReport(array $payload): void {
        // Report generation logic
        if (!isset($payload['type'])) {
            throw new Exception("Invalid report type");
        }
        // Simulate report generation
        sleep(3);
    }
}

// Usage
$queueManager = new QueueManager();
$processor = new JobProcessor($queueManager);

// Add various jobs
$processor->addJob('send_email', [
    'to' => 'user@example.com',
    'subject' => 'Welcome!',
    'body' => 'Welcome to our service!'
], 8);

$processor->addJob('process_payment', [
    'user_id' => 123,
    'amount' => 99.99,
    'currency' => 'USD'
], 10);

$processor->addJob('generate_report', [
    'type' => 'monthly_sales',
    'month' => '2024-01'
], 3);

// Process jobs
$app->get('/process-jobs', function($req, $res) use ($processor) {
    $processor->processJobs();
    $res->json(['status' => 'Jobs processed']);
});
```

## Storage Backends

### Redis Backend
```php
<?php
class RedisQueueStorage implements KislayPHP\\Queue\\StorageInterface {
    private $redis;

    public function __construct(string $host = 'localhost', int $port = 6379) {
        $this->redis = new Redis();
        $this->redis->connect($host, $port);
    }

    public function push(string $queueName, string $message, array $options = []): bool {
        $key = "queue:{$queueName}";

        if (isset($options['priority'])) {
            // Use sorted set for priority queue
            $priority = $options['priority'];
            $timestamp = isset($options['delay']) ? time() + $options['delay'] : time();
            return $this->redis->zAdd($key, $priority, $message) !== false;
        } else {
            // Use list for FIFO queue
            if (isset($options['delay'])) {
                // Delayed message - use sorted set with timestamp
                $deliverAt = time() + $options['delay'];
                return $this->redis->zAdd($key, $deliverAt, $message) !== false;
            } else {
                return $this->redis->lPush($key, $message) !== false;
            }
        }
    }

    public function pop(string $queueName): ?string {
        $key = "queue:{$queueName}";

        // Check if it's a priority/delayed queue (sorted set)
        $cardinality = $this->redis->zCard($key);
        if ($cardinality > 0) {
            // Get the highest priority item ready for delivery
            $items = $this->redis->zRangeByScore($key, 0, time(), ['limit' => [0, 1]]);
            if (!empty($items)) {
                $message = $items[0];
                $this->redis->zRem($key, $message);
                return $message;
            }
            return null;
        } else {
            // Regular FIFO queue
            return $this->redis->rPop($key);
        }
    }

    public function size(string $queueName): int {
        $key = "queue:{$queueName}";
        $zcard = $this->redis->zCard($key);
        return $zcard > 0 ? $zcard : $this->redis->lLen($key);
    }

    public function clear(string $queueName): bool {
        $key = "queue:{$queueName}";
        return $this->redis->del($key) !== false;
    }
}

// Usage
$queueManager = new QueueManager();
$redisStorage = new RedisQueueStorage('redis-server', 6379);
$queueManager->setStorage($redisStorage);
```

### Database Backend
```php
<?php
class DatabaseQueueStorage implements KislayPHP\\Queue\\StorageInterface {
    private $pdo;

    public function __construct(PDO $pdo) {
        $this->pdo = $pdo;
        $this->createTables();
    }

    private function createTables(): void {
        $this->pdo->exec("
            CREATE TABLE IF NOT EXISTS queues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name TEXT NOT NULL,
                message TEXT NOT NULL,
                priority INTEGER DEFAULT 0,
                available_at INTEGER DEFAULT 0,
                created_at INTEGER DEFAULT (strftime('%s', 'now'))
            );
            CREATE INDEX IF NOT EXISTS idx_queue_name_available_at
            ON queues (queue_name, available_at);
        ");
    }

    public function push(string $queueName, string $message, array $options = []): bool {
        $priority = $options['priority'] ?? 0;
        $availableAt = isset($options['delay']) ? time() + $options['delay'] : 0;

        $stmt = $this->pdo->prepare("
            INSERT INTO queues (queue_name, message, priority, available_at)
            VALUES (?, ?, ?, ?)
        ");

        return $stmt->execute([$queueName, $message, $priority, $availableAt]);
    }

    public function pop(string $queueName): ?string {
        $this->pdo->beginTransaction();

        try {
            // Find the highest priority message that's available
            $stmt = $this->pdo->prepare("
                SELECT id, message FROM queues
                WHERE queue_name = ? AND available_at <= ?
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
            ");

            $stmt->execute([$queueName, time()]);
            $row = $stmt->fetch(PDO::FETCH_ASSOC);

            if (!$row) {
                $this->pdo->rollBack();
                return null;
            }

            // Delete the message
            $deleteStmt = $this->pdo->prepare("DELETE FROM queues WHERE id = ?");
            $deleteStmt->execute([$row['id']]);

            $this->pdo->commit();
            return $row['message'];

        } catch (Exception $e) {
            $this->pdo->rollBack();
            return null;
        }
    }

    public function size(string $queueName): int {
        $stmt = $this->pdo->prepare("
            SELECT COUNT(*) as count FROM queues
            WHERE queue_name = ? AND available_at <= ?
        ");
        $stmt->execute([$queueName, time()]);
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        return (int) $row['count'];
    }

    public function clear(string $queueName): bool {
        $stmt = $this->pdo->prepare("DELETE FROM queues WHERE queue_name = ?");
        return $stmt->execute([$queueName]);
    }
}

// Usage
$pdo = new PDO('sqlite:queues.db');
$dbStorage = new DatabaseQueueStorage($pdo);
$queueManager = new QueueManager();
$queueManager->setStorage($dbStorage);
```

### File-Based Storage
```php
<?php
class FileQueueStorage implements KislayPHP\\Queue\\StorageInterface {
    private $storageDir;

    public function __construct(string $storageDir = '/tmp/queue_storage') {
        $this->storageDir = $storageDir;
        if (!is_dir($this->storageDir)) {
            mkdir($this->storageDir, 0755, true);
        }
    }

    public function push(string $queueName, string $message, array $options = []): bool {
        $queueDir = $this->storageDir . '/' . $queueName;
        if (!is_dir($queueDir)) {
            mkdir($queueDir, 0755, true);
        }

        $priority = $options['priority'] ?? 0;
        $availableAt = isset($options['delay']) ? time() + $options['delay'] : 0;

        $filename = sprintf(
            '%s/%020d_%05d_%s.json',
            $queueDir,
            $availableAt,
            $priority,
            uniqid('', true)
        );

        $data = [
            'message' => $message,
            'priority' => $priority,
            'available_at' => $availableAt,
            'created_at' => time()
        ];

        return file_put_contents($filename, json_encode($data)) !== false;
    }

    public function pop(string $queueName): ?string {
        $queueDir = $this->storageDir . '/' . $queueName;
        if (!is_dir($queueDir)) {
            return null;
        }

        $files = glob($queueDir . '/*.json');
        if (empty($files)) {
            return null;
        }

        // Sort by available_at, then priority, then filename
        usort($files, function($a, $b) {
            $dataA = json_decode(file_get_contents($a), true);
            $dataB = json_decode(file_get_contents($b), true);

            // First by available time
            if ($dataA['available_at'] !== $dataB['available_at']) {
                return $dataA['available_at'] <=> $dataB['available_at'];
            }

            // Then by priority (higher first)
            if ($dataA['priority'] !== $dataB['priority']) {
                return $dataB['priority'] <=> $dataA['priority'];
            }

            // Finally by filename (creation order)
            return strcmp($a, $b);
        });

        $now = time();
        foreach ($files as $file) {
            $data = json_decode(file_get_contents($file), true);
            if ($data['available_at'] <= $now) {
                unlink($file);
                return $data['message'];
            }
        }

        return null;
    }

    public function size(string $queueName): int {
        $queueDir = $this->storageDir . '/' . $queueName;
        if (!is_dir($queueDir)) {
            return 0;
        }

        $files = glob($queueDir . '/*.json');
        $count = 0;
        $now = time();

        foreach ($files as $file) {
            $data = json_decode(file_get_contents($file), true);
            if ($data['available_at'] <= $now) {
                $count++;
            }
        }

        return $count;
    }

    public function clear(string $queueName): bool {
        $queueDir = $this->storageDir . '/' . $queueName;
        if (!is_dir($queueDir)) {
            return true;
        }

        $files = glob($queueDir . '/*.json');
        foreach ($files as $file) {
            unlink($file);
        }

        return rmdir($queueDir);
    }
}

// Usage
$fileStorage = new FileQueueStorage('/var/lib/kislayphp/queues');
$queueManager = new QueueManager();
$queueManager->setStorage($fileStorage);
```

## Advanced Usage

### Message Serialization
```php
<?php
interface MessageSerializer {
    public function serialize($message): string;
    public function deserialize(string $message);
}

class JsonMessageSerializer implements MessageSerializer {
    public function serialize($message): string {
        return json_encode($message);
    }

    public function deserialize(string $message) {
        return json_decode($message, true);
    }
}

class EncryptedMessageSerializer implements MessageSerializer {
    private $key;
    private $cipher;

    public function __construct(string $key) {
        $this->key = $key;
        $this->cipher = 'aes-256-cbc';
    }

    public function serialize($message): string {
        $data = json_encode($message);
        $iv = openssl_random_pseudo_bytes(openssl_cipher_iv_length($this->cipher));
        $encrypted = openssl_encrypt($data, $this->cipher, $this->key, 0, $iv);
        return base64_encode($iv . $encrypted);
    }

    public function deserialize(string $message) {
        $data = base64_decode($message);
        $ivLength = openssl_cipher_iv_length($this->cipher);
        $iv = substr($data, 0, $ivLength);
        $encrypted = substr($data, $ivLength);
        $decrypted = openssl_decrypt($encrypted, $this->cipher, $this->key, 0, $iv);
        return json_decode($decrypted, true);
    }
}

class QueueWithSerialization {
    private $queue;
    private $serializer;

    public function __construct(KislayPHP\\Queue\\QueueInterface $queue, MessageSerializer $serializer) {
        $this->queue = $queue;
        $this->serializer = $serializer;
    }

    public function push($message, array $options = []): bool {
        $serialized = $this->serializer->serialize($message);
        return $this->queue->push($serialized, $options);
    }

    public function pop() {
        $serialized = $this->queue->pop();
        return $serialized ? $this->serializer->deserialize($serialized) : null;
    }

    public function peek() {
        $serialized = $this->queue->peek();
        return $serialized ? $this->serializer->deserialize($serialized) : null;
    }

    public function size(): int {
        return $this->queue->size();
    }

    public function isEmpty(): bool {
        return $this->queue->isEmpty();
    }

    public function clear(): bool {
        return $this->queue->clear();
    }
}

// Usage
$queueManager = new QueueManager();
$queueManager->createQueue('secure_messages');
$baseQueue = $queueManager->getQueue('secure_messages');

$serializer = new EncryptedMessageSerializer('your-secret-key-here');
$secureQueue = new QueueWithSerialization($baseQueue, $serializer);

// Push secure messages
$secureQueue->push([
    'user_id' => 12345,
    'action' => 'password_change',
    'timestamp' => time()
]);

// Pop and decrypt messages
$message = $secureQueue->pop();
print_r($message);
```

### Queue Monitoring and Metrics
```php
<?php
use KislayPHP\\Metrics\\Metrics;

class MonitoredQueueManager extends KislayPHP\\Queue\\QueueManager {
    private $metrics;

    public function __construct(Metrics $metrics) {
        parent::__construct();
        $this->metrics = $metrics;
    }

    public function createQueue(string $name, array $options = []): bool {
        $result = parent::createQueue($name, $options);
        if ($result) {
            $this->metrics->increment('queues_created_total');
        }
        return $result;
    }

    public function getQueue(string $name): KislayPHP\\Queue\\QueueInterface {
        $queue = parent::getQueue($name);
        return new MonitoredQueue($queue, $name, $this->metrics);
    }
}

class MonitoredQueue implements KislayPHP\\Queue\\QueueInterface {
    private $queue;
    private $name;
    private $metrics;

    public function __construct(KislayPHP\\Queue\\QueueInterface $queue, string $name, Metrics $metrics) {
        $this->queue = $queue;
        $this->name = $name;
        $this->metrics = $metrics;
    }

    public function push(string $message, array $options = []): bool {
        $start = microtime(true);
        $result = $this->queue->push($message, $options);
        $duration = microtime(true) - $start;

        if ($result) {
            $this->metrics->increment('queue_messages_pushed_total', 1);
            $this->metrics->histogram('queue_push_duration_seconds', $duration);
            $this->metrics->gauge("queue_{$this->name}_size", $this->size());
        }

        return $result;
    }

    public function pop(): ?string {
        $start = microtime(true);
        $message = $this->queue->pop();
        $duration = microtime(true) - $start;

        $this->metrics->histogram('queue_pop_duration_seconds', $duration);
        $this->metrics->gauge("queue_{$this->name}_size", $this->size());

        if ($message !== null) {
            $this->metrics->increment('queue_messages_popped_total', 1);
        }

        return $message;
    }

    public function peek(): ?string {
        return $this->queue->peek();
    }

    public function size(): int {
        $size = $this->queue->size();
        $this->metrics->gauge("queue_{$this->name}_size", $size);
        return $size;
    }

    public function isEmpty(): bool {
        return $this->queue->isEmpty();
    }

    public function clear(): bool {
        $result = $this->queue->clear();
        if ($result) {
            $this->metrics->gauge("queue_{$this->name}_size", 0);
        }
        return $result;
    }
}

// Usage
$metrics = new KislayPHP\\Metrics\\Metrics();
$queueManager = new MonitoredQueueManager($metrics);

// All queue operations are now monitored
$queue = $queueManager->getQueue('monitored_queue');
$queue->push('test message');
$message = $queue->pop();
```

### Dead Letter Queue
```php
<?php
class DeadLetterQueue {
    private $mainQueue;
    private $dlq;
    private $maxRetries;

    public function __construct(
        KislayPHP\\Queue\\QueueInterface $mainQueue,
        KislayPHP\\Queue\\QueueInterface $dlq,
        int $maxRetries = 3
    ) {
        $this->mainQueue = $mainQueue;
        $this->dlq = $dlq;
        $this->maxRetries = $maxRetries;
    }

    public function processWithDLQ(callable $processor): void {
        while (!$this->mainQueue->isEmpty()) {
            $message = $this->mainQueue->pop();
            if (!$message) continue;

            $messageData = json_decode($message, true);
            $retryCount = $messageData['retry_count'] ?? 0;

            try {
                $processor($messageData);
                echo "Message processed successfully\n";
            } catch (Exception $e) {
                $retryCount++;

                if ($retryCount < $this->maxRetries) {
                    // Retry with backoff
                    $delay = pow(2, $retryCount) * 60; // Exponential backoff
                    $messageData['retry_count'] = $retryCount;
                    $messageData['last_error'] = $e->getMessage();

                    $this->mainQueue->push(json_encode($messageData), ['delay' => $delay]);
                    echo "Message failed, retrying in {$delay} seconds (attempt {$retryCount})\n";
                } else {
                    // Move to dead letter queue
                    $messageData['failed_at'] = time();
                    $messageData['final_error'] = $e->getMessage();
                    $messageData['total_retries'] = $retryCount;

                    $this->dlq->push(json_encode($messageData));
                    echo "Message moved to dead letter queue after {$retryCount} attempts\n";
                }
            }
        }
    }

    public function getDLQSize(): int {
        return $this->dlq->size();
    }

    public function replayDLQ(callable $replayProcessor): int {
        $replayed = 0;

        while (!$this->dlq->isEmpty()) {
            $message = $this->dlq->pop();
            if (!$message) continue;

            $messageData = json_decode($message, true);

            try {
                $replayProcessor($messageData);
                $replayed++;
                echo "DLQ message replayed successfully\n";
            } catch (Exception $e) {
                // If replay fails, put back in DLQ
                $this->dlq->push($message);
                echo "DLQ message replay failed, keeping in DLQ\n";
                break;
            }
        }

        return $replayed;
    }
}

// Usage
$queueManager = new QueueManager();
$queueManager->createQueue('orders');
$queueManager->createQueue('orders_dlq');

$mainQueue = $queueManager->getQueue('orders');
$dlq = $queueManager->getQueue('orders_dlq');

$dlqProcessor = new DeadLetterQueue($mainQueue, $dlq, 3);

// Add some orders
$mainQueue->push(json_encode(['order_id' => 1, 'amount' => 99.99]));
$mainQueue->push(json_encode(['order_id' => 2, 'amount' => 149.99]));

// Process with DLQ
$dlqProcessor->processWithDLQ(function($order) {
    if ($order['amount'] > 100) {
        throw new Exception("High value order requires manual approval");
    }
    // Process order
    echo "Processing order {$order['order_id']}\n";
});

// Check DLQ
echo "DLQ size: " . $dlqProcessor->getDLQSize() . "\n";

// Replay DLQ messages (after fixing the issue)
$replayed = $dlqProcessor->replayDLQ(function($order) {
    // Now process high-value orders
    echo "Replaying order {$order['order_id']}\n";
});

echo "Replayed {$replayed} messages from DLQ\n";
```

### Batch Processing
```php
<?php
class BatchQueueProcessor {
    private $queue;
    private $batchSize;
    private $timeout;

    public function __construct(KislayPHP\\Queue\\QueueInterface $queue, int $batchSize = 10, int $timeout = 30) {
        $this->queue = $queue;
        $this->batchSize = $batchSize;
        $this->timeout = $timeout;
    }

    public function processBatch(callable $batchProcessor): int {
        $batch = [];
        $processed = 0;
        $startTime = time();

        while (count($batch) < $this->batchSize && (time() - $startTime) < $this->timeout) {
            $message = $this->queue->pop();
            if ($message === null) {
                // No more messages, but we have a partial batch
                if (!empty($batch)) {
                    break;
                }
                // No messages at all, wait a bit
                sleep(1);
                continue;
            }

            $batch[] = $message;

            // If we hit batch size, process immediately
            if (count($batch) >= $this->batchSize) {
                break;
            }
        }

        if (!empty($batch)) {
            try {
                $batchProcessor($batch);
                $processed = count($batch);
                echo "Processed batch of {$processed} messages\n";
            } catch (Exception $e) {
                // On batch failure, put messages back (in reverse order to maintain order)
                foreach (array_reverse($batch) as $message) {
                    $this->queue->push($message);
                }
                echo "Batch processing failed: " . $e->getMessage() . "\n";
                throw $e;
            }
        }

        return $processed;
    }

    public function processContinuously(callable $batchProcessor): void {
        while (true) {
            $processed = $this->processBatch($batchProcessor);
            if ($processed === 0) {
                // No messages processed, wait before checking again
                sleep(5);
            }
        }
    }
}

// Usage
$queueManager = new QueueManager();
$queueManager->createQueue('batch_jobs');
$batchQueue = $queueManager->getQueue('batch_jobs');

$batchProcessor = new BatchQueueProcessor($batchQueue, 5, 60);

// Add batch jobs
for ($i = 1; $i <= 20; $i++) {
    $batchQueue->push(json_encode(['job_id' => $i, 'data' => "Job data {$i}"]));
}

// Process in batches
$batchProcessor->processBatch(function($batch) {
    foreach ($batch as $message) {
        $job = json_decode($message, true);
        // Process job
        echo "Processing job {$job['job_id']}\n";
        sleep(1); // Simulate processing time
    }

    // Batch database insert or API call
    echo "Batch of " . count($batch) . " jobs completed\n";
});

// Or process continuously
// $batchProcessor->processContinuously($batchProcessorCallback);
```

## Integration Examples

### Laravel Queue Integration
```php
<?php
// config/queue.php
'kislayphp' => [
    'driver' => 'kislayphp',
    'queue' => 'default',
    'retry_after' => 90,
],

// app/Queue/KislayPHPQueue.php
class KislayPHPQueue extends Illuminate\\Queue\\Queue implements Illuminate\\Contracts\\Queue\\Queue {
    private $queueManager;
    private $defaultQueue;

    public function __construct(KislayPHP\\Queue\\QueueManager $queueManager, string $defaultQueue = 'default') {
        $this->queueManager = $queueManager;
        $this->queueManager->createQueue($defaultQueue);
        $this->defaultQueue = $defaultQueue;
    }

    public function size($queue = null): int {
        $queueName = $queue ?: $this->defaultQueue;
        $q = $this->queueManager->getQueue($queueName);
        return $q->size();
    }

    public function push($job, $data = '', $queue = null): mixed {
        $queueName = $queue ?: $this->defaultQueue;
        $q = $this->queueManager->getQueue($queueName);

        $payload = [
            'job' => $job,
            'data' => $data,
            'attempts' => 0,
            'id' => uniqid('', true),
            'queued_at' => time()
        ];

        return $q->push(json_encode($payload)) ? $payload['id'] : false;
    }

    public function pushRaw($payload, $queue = null, array $options = []): mixed {
        $queueName = $queue ?: $this->defaultQueue;
        $q = $this->queueManager->getQueue($queueName);
        return $q->push($payload, $options) ? uniqid('', true) : false;
    }

    public function later($delay, $job, $data = '', $queue = null): mixed {
        $queueName = $queue ?: $this->defaultQueue;
        $q = $this->queueManager->getQueue($queueName);

        $payload = [
            'job' => $job,
            'data' => $data,
            'attempts' => 0,
            'id' => uniqid('', true),
            'queued_at' => time()
        ];

        return $q->push(json_encode($payload), ['delay' => $delay]) ? $payload['id'] : false;
    }

    public function pop($queue = null): ?Illuminate\\Queue\\Jobs\\Job {
        $queueName = $queue ?: $this->defaultQueue;
        $q = $this->queueManager->getQueue($queueName);

        $message = $q->pop();
        if (!$message) {
            return null;
        }

        $payload = json_decode($message, true);
        return new KislayPHPJob($this->container, $this, $payload, $queueName);
    }
}
```

### Symfony Messenger Integration
```php
<?php
// src/Messenger/Transport/KislayPHPTransport.php
class KislayPHPTransport implements Symfony\\Component\\Messenger\\Transport\\TransportInterface {
    private $queueManager;
    private $queueName;

    public function __construct(KislayPHP\\Queue\\QueueManager $queueManager, string $queueName = 'messenger') {
        $this->queueManager = $queueManager;
        $this->queueManager->createQueue($queueName);
        $this->queueName = $queueName;
    }

    public function get(): iterable {
        $queue = $this->queueManager->getQueue($this->queueName);
        $message = $queue->pop();

        if (!$message) {
            return [];
        }

        $envelope = unserialize($message);
        return [$envelope];
    }

    public function ack(Symfony\\Component\\Messenger\\Envelope $envelope): void {
        // KislayPHP queues auto-remove messages on pop, so no explicit ack needed
    }

    public function reject(Symfony\\Component\\Messenger\\Envelope $envelope): void {
        // Put back in queue for retry
        $queue = $this->queueManager->getQueue($this->queueName);
        $queue->push(serialize($envelope));
    }

    public function send(Symfony\\Component\\Messenger\\Envelope $envelope): void {
        $queue = $this->queueManager->getQueue($this->queueName);
        $queue->push(serialize($envelope));
    }
}

// config/packages/messenger.yaml
framework:
    messenger:
        transports:
            kislayphp: 'kislayphp://default'

        routing:
            'App\\Message\\ImportantMessage': kislayphp
```

## Testing

### Unit Testing
```php
<?php
use PHPUnit\\Framework\\TestCase;
use KislayPHP\\Queue\\QueueManager;

class QueueTest extends TestCase {
    private $queueManager;

    protected function setUp(): void {
        $this->queueManager = new QueueManager();
        $this->queueManager->createQueue('test_queue');
    }

    public function testQueueOperations() {
        $queue = $this->queueManager->getQueue('test_queue');

        // Test empty queue
        $this->assertTrue($queue->isEmpty());
        $this->assertEquals(0, $queue->size());
        $this->assertNull($queue->pop());

        // Test push and pop
        $queue->push('message1');
        $this->assertFalse($queue->isEmpty());
        $this->assertEquals(1, $queue->size());

        $queue->push('message2');
        $this->assertEquals(2, $queue->size());

        $this->assertEquals('message1', $queue->pop());
        $this->assertEquals(1, $queue->size());

        $this->assertEquals('message2', $queue->pop());
        $this->assertTrue($queue->isEmpty());
    }

    public function testPriorityQueue() {
        $priorityQueue = new KislayPHP\\Queue\\PriorityQueue('priority_test');

        $priorityQueue->push('low priority', ['priority' => 1]);
        $priorityQueue->push('high priority', ['priority' => 10]);
        $priorityQueue->push('medium priority', ['priority' => 5]);

        // Should pop high priority first
        $this->assertEquals('high priority', $priorityQueue->pop());
        $this->assertEquals('medium priority', $priorityQueue->pop());
        $this->assertEquals('low priority', $priorityQueue->pop());
    }

    public function testDelayedQueue() {
        $delayedQueue = new KislayPHP\\Queue\\DelayedQueue('delayed_test');

        $delayedQueue->push('immediate', ['delay' => 0]);
        $delayedQueue->push('delayed', ['delay' => 1]); // 1 second delay

        // Should get immediate message
        $this->assertEquals('immediate', $delayedQueue->pop());

        // Should not get delayed message yet
        $this->assertNull($delayedQueue->pop());

        // Wait for delay
        sleep(2);

        // Should now get delayed message
        $this->assertEquals('delayed', $delayedQueue->pop());
    }
}
```

### Mock Storage for Testing
```php
<?php
class MockQueueStorage implements KislayPHP\\Queue\\StorageInterface {
    private $queues = [];

    public function push(string $queueName, string $message, array $options = []): bool {
        if (!isset($this->queues[$queueName])) {
            $this->queues[$queueName] = [];
        }

        $item = [
            'message' => $message,
            'options' => $options,
            'timestamp' => time()
        ];

        $this->queues[$queueName][] = $item;
        return true;
    }

    public function pop(string $queueName): ?string {
        if (!isset($this->queues[$queueName]) || empty($this->queues[$queueName])) {
            return null;
        }

        // Sort by priority if needed
        usort($this->queues[$queueName], function($a, $b) {
            $priorityA = $a['options']['priority'] ?? 0;
            $priorityB = $b['options']['priority'] ?? 0;
            return $priorityB <=> $priorityA; // Higher priority first
        });

        $item = array_shift($this->queues[$queueName]);
        return $item['message'];
    }

    public function size(string $queueName): int {
        return isset($this->queues[$queueName]) ? count($this->queues[$queueName]) : 0;
    }

    public function clear(string $queueName): bool {
        if (isset($this->queues[$queueName])) {
            $this->queues[$queueName] = [];
        }
        return true;
    }

    public function getAllMessages(string $queueName): array {
        return $this->queues[$queueName] ?? [];
    }
}

// Usage in tests
class QueueIntegrationTest extends TestCase {
    private $queueManager;

    protected function setUp(): void {
        $this->queueManager = new QueueManager();
        $mockStorage = new MockQueueStorage();
        $this->queueManager->setStorage($mockStorage);
    }

    public function testQueueWithMockStorage() {
        $this->queueManager->createQueue('integration_test');
        $queue = $this->queueManager->getQueue('integration_test');

        $queue->push('test message');
        $this->assertEquals(1, $queue->size());

        $message = $queue->pop();
        $this->assertEquals('test message', $message);
        $this->assertEquals(0, $queue->size());
    }
}
```

## Troubleshooting

### Common Issues

#### Messages Not Being Processed
**Symptoms:** Messages remain in queue, not being consumed

**Solutions:**
1. Check queue storage backend connectivity
2. Verify consumer processes are running
3. Check for message serialization issues
4. Monitor queue size and processing rates

#### High Memory Usage
**Symptoms:** Memory consumption grows during queue operations

**Solutions:**
1. Implement message batching
2. Use external storage backends (Redis, Database)
3. Monitor queue depth and implement backpressure
4. Clear processed messages promptly

#### Message Ordering Issues
**Symptoms:** Messages processed out of order

**Solutions:**
1. Use FIFO queues for strict ordering
2. Implement message sequencing
3. Avoid concurrent consumers for ordered processing
4. Use priority queues appropriately

### Performance Tuning

#### Queue Throughput Optimization
```php
<?php
class OptimizedQueueProcessor {
    private $queue;
    private $batchSize;
    private $workers;

    public function __construct(KislayPHP\\Queue\\QueueInterface $queue, int $batchSize = 100) {
        $this->queue = $queue;
        $this->batchSize = $batchSize;
        $this->workers = [];
    }

    public function startWorkers(int $numWorkers = 4): void {
        for ($i = 0; $i < $numWorkers; $i++) {
            $pid = pcntl_fork();

            if ($pid == -1) {
                die('Could not fork worker');
            } elseif ($pid == 0) {
                // Child process
                $this->workerLoop();
                exit(0);
            } else {
                // Parent process
                $this->workers[] = $pid;
            }
        }
    }

    private function workerLoop(): void {
        while (true) {
            $messages = $this->getBatch();

            if (empty($messages)) {
                sleep(1); // Wait before checking again
                continue;
            }

            foreach ($messages as $message) {
                $this->processMessage($message);
            }
        }
    }

    private function getBatch(): array {
        $messages = [];

        for ($i = 0; $i < $this->batchSize; $i++) {
            $message = $this->queue->pop();
            if ($message === null) {
                break;
            }
            $messages[] = $message;
        }

        return $messages;
    }

    private function processMessage(string $message): void {
        // Process message logic
        $data = json_decode($message, true);
        // ... processing logic ...
    }

    public function stopWorkers(): void {
        foreach ($this->workers as $pid) {
            posix_kill($pid, SIGTERM);
            pcntl_waitpid($pid, $status);
        }
    }
}

// Usage
$queueManager = new QueueManager();
$queueManager->createQueue('high_volume');
$queue = $queueManager->getQueue('high_volume');

$processor = new OptimizedQueueProcessor($queue, 50);
$processor->startWorkers(8); // 8 worker processes

// Add thousands of messages
for ($i = 0; $i < 10000; $i++) {
    $queue->push(json_encode(['id' => $i, 'data' => "Message {$i}"]));
}

// Workers will process messages concurrently
```

#### Monitoring Queue Performance
```php
<?php
class QueueMonitor {
    private $metrics;
    private $queues;

    public function __construct(KislayPHP\\Metrics\\Metrics $metrics) {
        $this->metrics = $metrics;
        $this->queues = [];
    }

    public function registerQueue(string $name, KislayPHP\\Queue\\QueueInterface $queue): void {
        $this->queues[$name] = $queue;
    }

    public function collectMetrics(): void {
        foreach ($this->queues as $name => $queue) {
            $size = $queue->size();
            $this->metrics->gauge("queue_{$name}_size", $size);

            // Calculate processing rate
            static $lastSize = [];
            static $lastTime = [];

            $currentTime = microtime(true);

            if (isset($lastSize[$name]) && isset($lastTime[$name])) {
                $timeDiff = $currentTime - $lastTime[$name];
                $sizeDiff = $lastSize[$name] - $size;

                if ($timeDiff > 0) {
                    $rate = $sizeDiff / $timeDiff;
                    $this->metrics->gauge("queue_{$name}_processing_rate", $rate);
                }
            }

            $lastSize[$name] = $size;
            $lastTime[$name] = $currentTime;
        }
    }

    public function getHealthStatus(): array {
        $status = [];

        foreach ($this->queues as $name => $queue) {
            $size = $queue->size();

            if ($size > 1000) {
                $status[$name] = 'critical';
            } elseif ($size > 100) {
                $status[$name] = 'warning';
            } else {
                $status[$name] = 'healthy';
            }
        }

        return $status;
    }
}

// Usage
$monitor = new QueueMonitor($metrics);
$monitor->registerQueue('orders', $orderQueue);
$monitor->registerQueue('emails', $emailQueue);

// Collect metrics periodically
$app->get('/queue-health', function($req, $res) use ($monitor) {
    $monitor->collectMetrics();
    $status = $monitor->getHealthStatus();
    $res->json($status);
});
```

## Best Practices

### Queue Design Patterns
1. **Command Pattern**: Use queues to encapsulate operations as commands
2. **Event Sourcing**: Store events in queues for replay and debugging
3. **Saga Pattern**: Use queues to coordinate distributed transactions
4. **Circuit Breaker**: Implement failure handling with dead letter queues

### Scalability Considerations
1. **Horizontal Scaling**: Use multiple consumers for high throughput
2. **Partitioning**: Split large queues into smaller, manageable partitions
3. **Load Balancing**: Distribute work evenly across consumer instances
4. **Backpressure**: Implement mechanisms to prevent queue overflow

### Reliability Patterns
1. **Idempotent Processing**: Ensure operations can be safely retried
2. **Poison Message Handling**: Automatically quarantine problematic messages
3. **Message Deduplication**: Prevent duplicate message processing
4. **Transactional Processing**: Ensure message processing consistency

### Monitoring and Observability
1. **Queue Depth Monitoring**: Alert on excessive queue buildup
2. **Processing Rate Tracking**: Monitor consumer throughput
3. **Error Rate Monitoring**: Track failed message processing
4. **Latency Measurement**: Monitor end-to-end message processing time

This comprehensive documentation covers all aspects of the KislayPHP Queue extension, from basic usage to advanced patterns and production deployment strategies.