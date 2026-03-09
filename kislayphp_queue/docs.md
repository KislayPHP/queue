# KislayPHP Queue Extension - Technical Reference

## ⚠️ CRITICAL WARNING: IN-PROCESS ONLY, NON-DURABLE

**THIS EXTENSION STORES ALL QUEUE DATA IN PROCESS HEAP MEMORY.**

- **No disk persistence**: All queues exist only in RAM
- **Process death = complete data loss**: Terminating PHP loses all enqueued messages
- **Not suitable for production critical work**: Use only for:
  - Development and testing
  - Temporary in-process job coordination
  - Non-critical background tasks
  - Systems with external durability guarantees

**If you need message durability, use Redis, RabbitMQ, or other external queue brokers.**

---

## 1. Architecture

### Overview

KislayPHP Queue is an in-memory message queue extension for PHP, providing priority-based message handling with optional delayed delivery, time-to-live (TTL) expiration, and dead-letter queue (DLQ) routing.

**Key characteristics:**
- **In-process only**: All data stored in PHP process memory
- **Priority queue**: Messages selected by highest priority (FIFO within same priority)
- **Lazy expiration**: TTL checked at dequeue time, no background cleanup thread
- **Delayed messages**: Messages held until a specified future time
- **Dead-letter routing**: Failed messages (nacked) route to alternate queues
- **Synchronous subscribers**: Callbacks invoked immediately during enqueue
- **Atomic message IDs**: Monotonic 64-bit counter → 16-char hex IDs

### Data Structures

#### Message Structure

Each enqueued message contains:

```php
[
    'value'           => mixed,        // Payload (any serializable value)
    'priority'        => int,          // 0=normal, higher=processed first
    'enqueue_time_ms' => int64,        // Milliseconds since epoch (when enqueued)
    'ttl_ms'          => int64,        // Time-to-live in milliseconds (0=never expire)
    'delay_until_ms'  => int64,        // Don't process until this epoch time (0=immediate)
    'message_id'      => string,       // 16-char lowercase hex (globally unique per process)
]
```

#### Priority Queue Container

```
std::vector<Message>
    ↓
Linear scan on dequeue (O(n) complexity)
    ↓
Select eligible message with highest priority
    ↓
Eligible = NOT ttl-expired AND delay_until_ms ≤ now
    ↓
FIFO ordering within same priority level
```

**Selection algorithm (kislayphp_find_best):**
```cpp
// Pseudocode
best_idx = -1
best_priority = INT_MIN

for each message in queue:
    if (now - message.enqueue_time_ms > message.ttl_ms)
        skip (expired)
    
    if (message.delay_until_ms > now)
        skip (not ready)
    
    if (message.priority > best_priority)
        best_idx = idx
        best_priority = message.priority

return best_idx
```

#### In-Flight Tracking

```
dequeueWithId() moves message → std::unordered_map<message_id, Message>
        ↓
    ack(id) → remove from in_flight (message consumed)
        ↓
    nack(id, true) → back to queue with delay_until_ms=0
        ↓
    nack(id, false) → route to DLQ or discard
```

#### Dead-Letter Queue Mapping

```
std::unordered_map<queue_name, dlq_name>
        ↓
When setDLQ("orders", "orders_failed") called:
    ↓ saves "orders" → "orders_failed"
        ↓
When nack(id, false) on message from "orders":
    ↓ creates NEW message in "orders_failed" queue:
        - Same payload
        - Same priority
        - New enqueue_time_ms (now)
        - ttl_ms = 0 (never expire in DLQ)
        - delay_until_ms = 0 (immediate)
        - New message_id (atomic counter++)
```

### TTL and Expiration

**Lazy expiration strategy:**
- No background thread
- Check at dequeue time: `now - enqueue_time_ms > ttl_ms`
- Expired messages skipped during selection
- Active cleanup: `purgeExpired(queue_name)` uses `std::remove_if`

**Example: 5-second TTL**
```
Enqueue at 1000ms with ttl_ms=5000
    ↓
Expiry threshold: 1000 + 5000 = 6000ms
    ↓
At 5500ms: still valid (5500 < 6000)
    ↓
At 7000ms: expired (7000 >= 6000), skipped in selection
```

### Delayed Messages

**Mechanism:**
```
enqueue(queue, payload, ttl=0, priority=0, delay_ms=500)
    ↓
delay_until_ms = now_epoch_ms + delay_ms (500ms from now)
    ↓
On dequeue: skip if now < delay_until_ms
    ↓
When delay passes: eligible for selection
```

**Requeue behavior:**
```
dequeueWithId(queue) → [value => ..., id => "abc123"]
    ↓
nack(id, true)  // requeue
    ↓
Sets delay_until_ms = 0 (immediate, removes original delay)
    ↓
Message back in queue, eligible for next dequeue
```

### Subscriber Callbacks

**Synchronous execution:**
```
subscribe(queue_name, callable)
    ↓
Callbacks stored per queue under lock
    ↓
On enqueue(queue, payload):
    - Add message to queue
    - Collect callbacks (still under lock)
    - Release lock
    - Execute callbacks (OUTSIDE lock)
    - Callback signature: function(mixed $payload, string $messageId): void
```

**Multiple subscribers:**
- Registration order preserved
- All called for each enqueue
- Callback exception doesn't prevent message enqueueing
- Called with same $payload and $messageId for each subscriber

### Message ID Generation

**Global monotonic counter:**
```
std::atomic<uint64_t> message_counter (initial = 0)
    ↓
Each enqueue/DLQ creation: counter++
    ↓
Convert uint64 → 16-char lowercase hex
    ↓
Example: 255 → "00000000000000ff"
```

---

## 2. Configuration Reference

### Environment Variables

All settings via environment variables (read at module load time):

| Variable | Type | Default | Purpose |
|----------|------|---------|---------|
| `KISLAY_RPC_ENABLED` | bool | `false` | Enable RPC client mode (routes to external endpoint) |
| `KISLAY_RPC_TIMEOUT_MS` | long | `200` | RPC call timeout in milliseconds |
| `KISLAY_RPC_PLATFORM_ENDPOINT` | string | `127.0.0.1:9100` | RPC server address (host:port) |

### RPC Mode

When `KISLAY_RPC_ENABLED=true`:
- `setClient(ClientInterface)` required for enqueue/dequeue
- Operations routed to `ClientInterface::enqueue()`, `ClientInterface::dequeue()`, etc.
- Allows external queue backends (Redis, gRPC, etc.)
- Timeout applies to RPC calls

### Example: Enable RPC

```bash
export KISLAY_RPC_ENABLED=1
export KISLAY_RPC_TIMEOUT_MS=500
export KISLAY_RPC_PLATFORM_ENDPOINT=queue-server:9100
```

---

## 3. API Reference

### KislayQueue Class

#### Constructor

```php
public function __construct()
```

Initializes the queue manager. No parameters required.

**Example:**
```php
$qm = new KislayQueue();
```

#### setClient()

```php
public function setClient(ClientInterface $client): void
```

Set external client for RPC mode. Required when `KISLAY_RPC_ENABLED=true`.

**Parameters:**
- `$client` - Implements `ClientInterface`

**ClientInterface methods:**
```php
interface ClientInterface {
    public function enqueue(string $queue, mixed $payload): bool;
    public function dequeue(string $queue): mixed;
    public function size(string $queue): int;
}
```

**Example:**
```php
class RedisClient implements ClientInterface {
    private $redis;
    
    public function __construct() {
        $this->redis = new Redis();
        $this->redis->connect('localhost', 6379);
    }
    
    public function enqueue(string $queue, mixed $payload): bool {
        return $this->redis->rpush($queue, json_encode($payload)) !== false;
    }
    
    public function dequeue(string $queue): mixed {
        $data = $this->redis->lpop($queue);
        return $data ? json_decode($data) : null;
    }
    
    public function size(string $queue): int {
        return $this->redis->llen($queue);
    }
}

$qm = new KislayQueue();
$qm->setClient(new RedisClient());
```

#### enqueue()

```php
public function enqueue(
    string $queue,
    mixed $payload,
    int $ttlMs = 0,
    int $priority = 0,
    int $delayMs = 0
): bool
```

Enqueue a message to the specified queue. Triggers all registered subscribers.

**Parameters:**
- `$queue` - Queue name (string)
- `$payload` - Message data (any serializable value)
- `$ttlMs` - Time-to-live in milliseconds (0 = never expire)
- `$priority` - Priority level (higher = processed first)
- `$delayMs` - Delay before processing in milliseconds

**Returns:** `bool` - `true` on success, `false` if enqueue failed

**Example:**
```php
// Simple message
$qm->enqueue('tasks', ['action' => 'send_email', 'to' => 'user@example.com']);

// With 10-second TTL
$qm->enqueue('transient_jobs', $data, ttlMs: 10000);

// High priority
$qm->enqueue('alerts', $alert_data, priority: 10);

// Delayed (process in 5 minutes)
$qm->enqueue('scheduled', $task, delayMs: 5 * 60 * 1000);

// All options
$qm->enqueue(
    'critical_tasks',
    $payload,
    ttlMs: 60000,      // 60 seconds
    priority: 5,       // High priority
    delayMs: 2000      // 2 second delay
);
```

#### dequeue()

```php
public function dequeue(string $queue): mixed|null
```

**LEGACY METHOD** - Dequeue oldest message (ignores priority). Does NOT use in-flight tracking.

- Selects first eligible message (delay passed, not expired)
- Ignores priority ordering
- **Note:** Does not track in-flight; message lost if process crashes
- Use `dequeueWithId()` instead for reliable processing

**Returns:** `mixed|null` - Message payload or `null` if queue empty

**Example:**
```php
// Simple, unreliable
while ($msg = $qm->dequeue('tasks')) {
    processTask($msg);
}
```

#### dequeueWithId()

```php
public function dequeueWithId(string $queue): array|null
```

Dequeue highest-priority eligible message with ID tracking. Moves message to in-flight map.

**Returns:** `array|null` - `['value' => mixed, 'id' => string]` or `null` if queue empty

**In-flight behavior:**
- Message removed from queue
- Stored in per-process in-flight map
- Must call `ack($id)` or `nack($id, ...)` within same request
- If neither called: message lost

**Example:**
```php
$msg = $qm->dequeueWithId('orders');
if ($msg === null) {
    echo "Queue empty\n";
    exit;
}

['value' => $order, 'id' => $messageId] = $msg;

try {
    processOrder($order);
    $qm->ack($messageId);  // Mark consumed
} catch (Exception $e) {
    $qm->nack($messageId, requeue: true);  // Retry
    throw $e;
}
```

#### ack()

```php
public function ack(string $messageId): bool
```

Acknowledge a message, removing it from in-flight tracking.

**Parameters:**
- `$messageId` - Message ID from `dequeueWithId()` response

**Returns:** `bool` - `true` if message was in-flight and acknowledged, `false` if not found

**Example:**
```php
$msg = $qm->dequeueWithId('tasks');
if ($msg && processSuccess($msg['value'])) {
    $qm->ack($msg['id']);
}
```

#### nack()

```php
public function nack(string $messageId, bool $requeue = true): bool
```

Negative acknowledge a message. Either requeue or route to DLQ.

**Parameters:**
- `$messageId` - Message ID from `dequeueWithId()` response
- `$requeue` - If `true`: return to queue (FIFO, no delay). If `false`: route to DLQ or discard

**Returns:** `bool` - `true` if message was in-flight and nacked, `false` if not found

**Requeue behavior** (`$requeue = true`):
- Message returns to source queue
- `delay_until_ms` reset to 0 (immediate)
- Preserves original payload and priority
- New message ID generated

**DLQ routing** (`$requeue = false`):
- If DLQ configured via `setDLQ()`: message moves to DLQ with:
  - Same payload and priority
  - New enqueue_time_ms (current time)
  - ttl_ms = 0 (never expire)
  - delay_until_ms = 0 (immediate)
  - New message_id
- If no DLQ configured: message silently discarded

**Example:**
```php
$msg = $qm->dequeueWithId('payments');
if (!$msg) return;

try {
    chargeCard($msg['value']);
    $qm->ack($msg['id']);
} catch (PaymentFailedException $e) {
    if ($e->isRetryable()) {
        $qm->nack($msg['id'], requeue: true);  // Retry
    } else {
        $qm->nack($msg['id'], requeue: false);  // Move to DLQ
    }
}
```

#### setDLQ()

```php
public function setDLQ(string $sourceName, string $dlqName): void
```

Configure dead-letter queue routing. When a message from `$sourceName` is nacked with `requeue=false`, it routes to `$dlqName`.

**Parameters:**
- `$sourceName` - Source queue name
- `$dlqName` - Dead-letter queue name

**Example:**
```php
// Route failed payment attempts to DLQ
$qm->setDLQ('payments', 'payments_failed');
$qm->setDLQ('emails', 'emails_dlq');

$msg = $qm->dequeueWithId('payments');
if ($msg && isCardExpired($msg['value'])) {
    $qm->nack($msg['id'], requeue: false);  // → moves to payments_failed
}
```

#### purgeExpired()

```php
public function purgeExpired(string $queue): int
```

Actively remove all TTL-expired messages from queue. Runs `std::remove_if` cleanup.

**Parameters:**
- `$queue` - Queue name

**Returns:** `int` - Number of messages purged

**Note:** Automatic lazy expiry happens at dequeue time. Use this for explicit cleanup (e.g., reduce memory, monitoring).

**Example:**
```php
$purged = $qm->purgeExpired('temporary_jobs');
echo "Purged $purged expired messages\n";

// Periodic cleanup cron
if (time() % 60 === 0) {
    $qm->purgeExpired('temp_queue');
}
```

#### subscribe()

```php
public function subscribe(string $queue, callable $callback): void
```

Register callback to fire when messages enqueued to this queue. Synchronous (called during `enqueue()`).

**Callback signature:**
```php
function(mixed $payload, string $messageId): void
```

**Parameters:**
- `$queue` - Queue name
- `$callback` - Callable receiving `$payload` and `$messageId`

**Execution model:**
- Called OUTSIDE queue lock (safe to use queue methods inside callback)
- Called in registration order
- All callbacks executed even if one throws exception
- Exception doesn't prevent message enqueueing

**Example:**
```php
// Log all emails
$qm->subscribe('emails', function($payload, $id) {
    error_log("Email queued: " . json_encode($payload) . " (ID: $id)");
});

// Metrics
$qm->subscribe('tasks', function($payload, $id) {
    $metrics->increment('queue.tasks.enqueued');
});

// Trigger external service
$qm->subscribe('orders', function($order, $id) {
    try {
        $webhook->post('order_created', $order);
    } catch (Exception $e) {
        error_log("Webhook failed: " . $e->getMessage());
        // Message still enqueued, doesn't affect queue
    }
});

// Multiple subscribers (called in order)
$qm->subscribe('notifications', fn($n, $id) => logger()->info("Notif: $id"));
$qm->subscribe('notifications', fn($n, $id) => metrics()->increment('notif.sent'));
$qm->subscribe('notifications', fn($n, $id) => $cache->set("notif_$id", $n));
```

#### peek()

```php
public function peek(string $queue): mixed|null
```

Return highest-priority eligible message WITHOUT removing it from queue. Useful for inspection.

**Returns:** `mixed|null` - Message payload or `null` if queue empty

**Note:** Does NOT modify queue or create in-flight entry

**Example:**
```php
$next = $qm->peek('tasks');
if ($next && $next['priority'] > 5) {
    echo "High priority task waiting\n";
}
```

#### size()

```php
public function size(string $queue): int
```

Get count of messages in queue (eligible + delayed + unexpired).

**Returns:** `int` - Total messages

**Note:** Includes:
- Messages not yet ready (delayed)
- Messages not yet expired (TTL still active)
- Does NOT include in-flight messages

**Example:**
```php
$count = $qm->size('tasks');
if ($count > 1000) {
    echo "Queue backlog high\n";
}
```

#### clear()

```php
public function clear(string $queue): int
```

Remove all messages from queue. Empties in-flight tracking as well.

**Returns:** `int` - Number of messages removed

**Example:**
```php
$removed = $qm->clear('temp_queue');
echo "Cleared $removed messages\n";
```

---

## 4. Patterns and Recipes

### Pattern 1: Task Queue with Acknowledgment

Reliable task processing with dequeue-and-ack workflow.

```php
class TaskProcessor {
    private $qm;
    
    public function __construct(KislayQueue $qm) {
        $this->qm = $qm;
    }
    
    public function start() {
        while (true) {
            $msg = $this->qm->dequeueWithId('tasks');
            if (!$msg) {
                usleep(100_000);  // 100ms
                continue;
            }
            
            ['value' => $task, 'id' => $id] = $msg;
            
            try {
                $this->executeTask($task);
                $this->qm->ack($id);
                error_log("✓ Task $id completed");
            } catch (Exception $e) {
                error_log("✗ Task $id failed: " . $e->getMessage());
                $this->qm->nack($id, requeue: true);
            }
        }
    }
    
    private function executeTask(array $task): void {
        // Business logic here
        match($task['type']) {
            'send_email' => $this->sendEmail($task['data']),
            'generate_report' => $this->generateReport($task['data']),
            default => throw new Exception("Unknown task type")
        };
    }
    
    private function sendEmail(array $data): void {
        // Implementation
    }
    
    private function generateReport(array $data): void {
        // Implementation
    }
}

// Usage
$qm = new KislayQueue();
$processor = new TaskProcessor($qm);

// In main loop or CLI script
$processor->start();

// Enqueue from request handlers
$qm->enqueue('tasks', [
    'type' => 'send_email',
    'data' => ['to' => 'user@example.com', 'subject' => 'Welcome']
]);
```

### Pattern 2: Retry with Dead-Letter Queue

Route permanently failed messages to a separate queue for human review.

```php
class PaymentQueue {
    private $qm;
    private const MAX_RETRIES = 3;
    
    public function __construct(KislayQueue $qm) {
        $this->qm = $qm;
        // Route failed payments to review queue
        $this->qm->setDLQ('payments', 'payments_failed_dlq');
    }
    
    public function processPayments() {
        while ($msg = $this->qm->dequeueWithId('payments')) {
            ['value' => $payment, 'id' => $id] = $msg;
            
            $retries = $payment['_retries'] ?? 0;
            
            try {
                $this->chargeCard($payment);
                $this->qm->ack($id);
                error_log("✓ Payment $id processed");
            } catch (RetryableException $e) {
                if ($retries < self::MAX_RETRIES) {
                    $payment['_retries'] = $retries + 1;
                    // Requeue with exponential backoff
                    $delay_ms = 1000 * pow(2, $retries);
                    $this->qm->enqueue(
                        'payments',
                        $payment,
                        delayMs: $delay_ms
                    );
                    $this->qm->ack($id);  // Don't use nack since we re-enqueued
                    error_log("⟳ Payment $id queued for retry (attempt " . ($retries+1) . ")");
                } else {
                    // Permanent failure → DLQ
                    $this->qm->nack($id, requeue: false);
                    error_log("✗ Payment $id → DLQ after $retries retries");
                }
            } catch (PermanentException $e) {
                // Non-retryable error → DLQ
                $this->qm->nack($id, requeue: false);
                error_log("✗ Payment $id non-retryable → DLQ");
            }
        }
    }
    
    public function reviewFailedPayments() {
        while ($msg = $this->qm->dequeueWithId('payments_failed_dlq')) {
            ['value' => $payment, 'id' => $id] = $msg;
            
            // Human review or escalation
            $this->notifyFinance($payment);
            $this->qm->ack($id);
        }
    }
    
    private function chargeCard(array $payment): void {
        // Payment processing logic
    }
    
    private function notifyFinance(array $payment): void {
        // Send to support team
    }
}
```

### Pattern 3: Delayed Job Scheduling

Schedule tasks to run at specific times in the future.

```php
class ScheduledTasks {
    private $qm;
    
    public function __construct(KislayQueue $qm) {
        $this->qm = $qm;
    }
    
    // Schedule a task to run in N seconds
    public function scheduleIn(string $taskName, array $data, int $seconds): bool {
        return $this->qm->enqueue(
            'scheduled_tasks',
            ['task' => $taskName, 'data' => $data],
            delayMs: $seconds * 1000
        );
    }
    
    // Schedule a task for a specific time (Unix timestamp)
    public function scheduleAt(string $taskName, array $data, int $unixTimestamp): bool {
        $now_ms = intval(microtime(true) * 1000);
        $target_ms = $unixTimestamp * 1000;
        $delay_ms = max(0, $target_ms - $now_ms);
        
        return $this->qm->enqueue(
            'scheduled_tasks',
            ['task' => $taskName, 'data' => $data],
            delayMs: $delay_ms
        );
    }
    
    public function processScheduledTasks() {
        while ($msg = $this->qm->dequeueWithId('scheduled_tasks')) {
            ['value' => $job, 'id' => $id] = $msg;
            
            try {
                $this->executeTask($job['task'], $job['data']);
                $this->qm->ack($id);
                error_log("✓ Scheduled task {$job['task']} executed");
            } catch (Exception $e) {
                error_log("✗ Scheduled task {$job['task']} failed: " . $e->getMessage());
                $this->qm->nack($id, requeue: true);
            }
        }
    }
    
    private function executeTask(string $name, array $data): void {
        match($name) {
            'cleanup_temp_files' => $this->cleanupTempFiles(),
            'send_reminder' => $this->sendReminder($data),
            'sync_external_data' => $this->syncExternalData($data),
            default => throw new Exception("Unknown task: $name")
        };
    }
    
    private function cleanupTempFiles(): void { /* ... */ }
    private function sendReminder(array $data): void { /* ... */ }
    private function syncExternalData(array $data): void { /* ... */ }
}

// Usage
$tasks = new ScheduledTasks($qm);

// In request handler
$tasks->scheduleIn('cleanup_temp_files', [], 3600);  // Run in 1 hour
$tasks->scheduleAt('send_reminder', ['user_id' => 123], time() + 86400);  // Tomorrow

// In background worker
$tasks->processScheduledTasks();
```

### Pattern 4: Rate-Limited Processor

Process queue messages at a controlled rate.

```php
class RateLimitedProcessor {
    private $qm;
    private $rateLimit = 10;  // messages per second
    private $lastBatchTime = 0;
    
    public function __construct(KislayQueue $qm, int $messagesPerSecond = 10) {
        $this->qm = $qm;
        $this->rateLimit = $messagesPerSecond;
    }
    
    public function process(string $queue) {
        $messageCount = 0;
        $batchStartTime = microtime(true);
        
        while ($msg = $this->qm->dequeueWithId($queue)) {
            ['value' => $data, 'id' => $id] = $msg;
            
            try {
                $this->handleMessage($data);
                $this->qm->ack($id);
                $messageCount++;
            } catch (Exception $e) {
                error_log("Error processing message: " . $e->getMessage());
                $this->qm->nack($id, requeue: true);
                $messageCount++;
            }
            
            // Rate limiting
            if ($messageCount >= $this->rateLimit) {
                $elapsed = microtime(true) - $batchStartTime;
                $sleepTime = max(0, 1.0 - $elapsed);
                if ($sleepTime > 0) {
                    usleep($sleepTime * 1_000_000);
                }
                
                $messageCount = 0;
                $batchStartTime = microtime(true);
            }
        }
    }
    
    private function handleMessage(array $data): void {
        // Process message
    }
}

// Usage: 50 messages per second
$processor = new RateLimitedProcessor($qm, messagesPerSecond: 50);
$processor->process('api_calls');
```

### Pattern 5: Priority-Based Routing

Handle high-priority messages differently.

```php
class PriorityRouter {
    private $qm;
    
    public function __construct(KislayQueue $qm) {
        $this->qm = $qm;
    }
    
    public function routeRequest(array $request) {
        $priority = $this->calculatePriority($request);
        
        // Urgent: immediate execution
        if ($priority >= 8) {
            $this->handleUrgent($request);
            return;
        }
        
        // High: fast queue
        if ($priority >= 5) {
            $this->qm->enqueue('high_priority_queue', $request, priority: 10);
            return;
        }
        
        // Normal: standard queue
        $this->qm->enqueue('standard_queue', $request, priority: 5);
    }
    
    public function processQueues() {
        $this->processQueue('high_priority_queue', maxTime: 2.0);
        $this->processQueue('standard_queue', maxTime: 5.0);
    }
    
    private function processQueue(string $queue, float $maxTime) {
        $start = microtime(true);
        
        while (microtime(true) - $start < $maxTime) {
            $msg = $this->qm->dequeueWithId($queue);
            if (!$msg) {
                usleep(10_000);
                continue;
            }
            
            ['value' => $request, 'id' => $id] = $msg;
            
            try {
                $this->handle($request);
                $this->qm->ack($id);
            } catch (Exception $e) {
                error_log("Error: " . $e->getMessage());
                $this->qm->nack($id, requeue: true);
            }
        }
    }
    
    private function calculatePriority(array $request): int {
        // Custom priority logic
        if ($request['user_type'] === 'premium') return 8;
        if ($request['is_paid'] ?? false) return 6;
        return 3;
    }
    
    private function handleUrgent(array $request): void { /* ... */ }
    private function handle(array $request): void { /* ... */ }
}
```

---

## 5. Performance Notes

### Dequeue Selection Complexity

**Dequeue operation: O(n) linear scan**

```
Per dequeue(queue):
    for each message in queue:
        - Check TTL expiration: O(1)
        - Check delay eligibility: O(1)
        - Compare priority: O(1)
    return best_idx
```

**Implications:**
- **10 messages**: ~0.001ms
- **1,000 messages**: ~0.1ms
- **100,000 messages**: ~10ms

**Optimize for large queues:**
- Keep per-queue message count reasonable
- Use TTL to auto-purge old messages
- Call `purgeExpired()` before dequeuing large batches
- Split into multiple queues by category

### TTL and Memory Management

**Lazy expiration:**
- Expired messages not removed immediately
- Memory used until dequeue attempt or `purgeExpired()` call
- No background thread = no GC overhead

**Scenario: 100,000 messages enqueued with 1-hour TTL**
```php
for ($i = 0; $i < 100_000; $i++) {
    $qm->enqueue('temp', $data, ttlMs: 3600_000);
}

// All 100K in memory until:
// 1. Dequeue (skipped if expired)
// 2. purgeExpired() call
// 3. Process exit
```

**Recommendation:** Call `purgeExpired()` periodically for high-volume TTL queues.

### Synchronous Subscriber Callbacks

**Callbacks execute during enqueue, not asynchronously:**
```
enqueue('queue', $data)
    ├─ Add to queue (fast)
    └─ Execute all subscribers SYNCHRONOUSLY
        ├─ Callback 1 (must complete)
        ├─ Callback 2 (must complete)
        └─ Callback 3 (must complete)
    return to caller
```

**Performance implications:**
- Slow callbacks block `enqueue()` return
- Exception in callback doesn't prevent enqueue, but may propagate
- Multiple subscribers add latency

**Optimize:**
```php
// BAD: Slow callback during enqueue
$qm->subscribe('orders', function($order, $id) {
    $api->syncToERP($order);  // 1-2 second call!
});

// GOOD: Queue async work instead
$qm->subscribe('orders', function($order, $id) {
    $qm->enqueue('erp_sync_tasks', $order);
});
```

### In-Flight Message Limits

No built-in limit on in-flight messages (in-process only). Process memory constrains maximum:

```php
// All 10,000 in-flight simultaneously (requires ack/nack)
for ($i = 0; $i < 10_000; $i++) {
    $msg = $qm->dequeueWithId('queue');
    // Not acked/nacked yet
}
// At some point, PHP memory exhausted
```

**Best practice:** Keep in-flight count low
```php
$inFlight = 0;
$maxInFlight = 100;

while ($inFlight < $maxInFlight) {
    $msg = $qm->dequeueWithId('queue');
    if (!$msg) break;
    $inFlight++;
    
    // Process (may be async)
    handleAsync($msg, function() use (&$inFlight) {
        $inFlight--;
    });
}
```

### Memory Footprint Per Message

Rough estimate per message in queue:

```
PHP zval:           ~48 bytes (payload size varies)
Priority:            4 bytes
Enqueue time (ms):   8 bytes (int64)
TTL (ms):            8 bytes (int64)
Delay until (ms):    8 bytes (int64)
Message ID string:  16 bytes (hex) + string overhead
std::vector node:   ~32 bytes (pointer + bookkeeping)
─────────────────────────────
Total overhead:     ~80-100 bytes + payload
```

**Example: 1 million simple messages (10 bytes each)**
- Payload: 10MB
- Overhead: 100MB
- **Total: ~110MB per process**

---

## 6. Troubleshooting

### "All queue data lost after process restart"

**Expected behavior.** KislayPHP Queue is in-process only.

**Solution:** Use external queue system (Redis, RabbitMQ) for durability.

```php
// If durability needed, use RPC mode with external backend
// See Configuration Reference: RPC Mode
```

### Dequeue returns NULL (queue appears empty)

**Possible causes:**

1. **All messages delayed**
   ```php
   // Enqueued with delay_ms set, not yet eligible
   // Check with peek() or size()
   $qm->peek('queue');     // Next eligible message
   $qm->size('queue');     // Total count
   ```

2. **All messages expired (TTL passed)**
   ```php
   $qm->purgeExpired('queue');  // Remove expired
   ```

3. **Messages still in-flight (not ack'd/nack'd)**
   ```php
   // Messages after dequeueWithId() not yet returned
   $qm->dequeue('queue');  // Uses legacy path, doesn't track in-flight
   ```

### Subscriber callback not called

**Callbacks are synchronous—called during enqueue(), not later.**

```php
$qm->subscribe('queue', fn($p, $id) => echo "Called!\n");

$qm->enqueue('queue', ['data' => 'test']);
// Output: "Called!" (happens here)

// Later code
$msg = $qm->dequeueWithId('queue');
// Callback already executed
```

**If you need async callbacks:** Re-enqueue instead

```php
$qm->subscribe('queue', fn($p, $id) => $qm->enqueue('callbacks', $p));
```

### High latency during dequeue

**Likely cause:** Large queue size with many expired/delayed messages

```
dequeue() scans all messages: O(n)
```

**Solutions:**
1. Call `purgeExpired()` before high-load period
2. Split queue by category (smaller queues = faster scan)
3. Use priority > 0 for important messages (found faster)
4. Reduce TTL values

```php
// Maintenance: clean expired before peak hours
if (date('H') === '03') {  // 3 AM
    $qm->purgeExpired('temp_queue');
}
```

### Message lost after nack(id, true)

**Expected behavior.** `nack(id, true)` returns message to queue but removes delay.

**Symptom:** After `nack()`, message doesn't appear in next dequeue.

**Check:**
1. Is message stuck in-flight?
2. Did `nack()` succeed?

```php
$result = $qm->nack($id, requeue: true);
if (!$result) {
    echo "Message $id not found in-flight\n";
}
```

**Solution:** Use `dequeueWithId()` consistently; don't mix with `dequeue()`.

### Memory grows without bound

**Cause:** Messages accumulating in queue, never ack'd/nack'd.

```php
// BAD: Message stays in-flight forever
while ($msg = $qm->dequeueWithId('queue')) {
    $payload = $msg['value'];
    // Process but never ack/nack
}

// Hundreds of thousands accumulate in in-flight map
```

**Fix:** Always call `ack()` or `nack()`

```php
while ($msg = $qm->dequeueWithId('queue')) {
    try {
        process($msg['value']);
        $qm->ack($msg['id']);
    } catch (Exception $e) {
        $qm->nack($msg['id'], requeue: true);
    }
}
```

### Performance degrades over time

**Cause:** TTL-expired messages not removed, linear scan hits more records.

**Solution:** Implement periodic purge in background worker:

```php
// Run every 5 minutes
if (time() % 300 === 0) {
    foreach (['queue1', 'queue2', 'queue3'] as $q) {
        $count = $qm->purgeExpired($q);
        if ($count > 0) {
            error_log("Purged $count expired from $q");
        }
    }
}
```

### Can't set priority > 32767 or < -32768

**Limitation:** Priority is C++ `int` (32-bit signed integer).

**Valid range:** `-2,147,483,648` to `2,147,483,647`

```php
// OK
$qm->enqueue('queue', $data, priority: 100);
$qm->enqueue('queue', $data, priority: -50);

// Still valid but exceeds typical use
$qm->enqueue('queue', $data, priority: 1_000_000_000);
```

**Recommendation:** Use 0-10 scale or reserve ranges by category.

### RPC mode: connection timeout

**Symptom:** `KISLAY_RPC_ENABLED=1` but operations hang or timeout.

**Checks:**
1. Is `setClient()` called?
2. Is endpoint reachable?
3. Is timeout too short?

```php
// Verify client set
if (!$qm->setClient($client)) {
    echo "Failed to set RPC client\n";
}

// Increase timeout
putenv('KISLAY_RPC_TIMEOUT_MS=1000');  // 1 second
```

### Message stays in DLQ forever

**By design:** DLQ messages have `ttl_ms = 0` (never expire).

```php
// DLQ message won't auto-expire
$qm->nack($id, requeue: false);  // → moves to DLQ

// Manual removal required
$qm->clear('queue_dlq');
```

**If you need automatic DLQ cleanup:**

```php
// Archive DLQ messages after X days
$dlqMessages = [];
while ($msg = $qm->dequeueWithId('payments_dlq')) {
    $age_ms = intval(microtime(true) * 1000) - $msg['value']['_created_ms'];
    
    if ($age_ms > 30 * 24 * 3600 * 1000) {  // 30 days
        $this->archiveFailedPayment($msg['value']);
    } else {
        // Re-enqueue with TTL for next review
        $qm->enqueue('payments_dlq', $msg['value'], ttlMs: 86400_000);
    }
    
    $qm->ack($msg['id']);
}
```

---

## Appendix: Quick Reference

### Common Operations

```php
$qm = new KislayQueue();

// Enqueue (simple)
$qm->enqueue('tasks', ['action' => 'email']);

// Enqueue (all options)
$qm->enqueue('tasks', $data, ttlMs: 60000, priority: 5, delayMs: 1000);

// Dequeue with ID (reliable)
$msg = $qm->dequeueWithId('tasks');
if ($msg) {
    ['value' => $data, 'id' => $id] = $msg;
    $qm->ack($id);
}

// Nack and retry
$qm->nack($id, requeue: true);

// Nack to DLQ
$qm->setDLQ('tasks', 'tasks_dlq');
$qm->nack($id, requeue: false);

// Inspect
$qm->peek('tasks');           // Next message
$qm->size('tasks');           // Queue size
$qm->purgeExpired('tasks');   // Remove expired

// Subscribe
$qm->subscribe('tasks', fn($p, $id) => logger()->info("Task $id"));

// Clean up
$qm->clear('tasks');
```

### Environment Setup

```bash
# Default (in-process)
# (no setup needed)

# RPC mode
export KISLAY_RPC_ENABLED=1
export KISLAY_RPC_TIMEOUT_MS=500
export KISLAY_RPC_PLATFORM_ENDPOINT=queue-server:9100
```

---

**Document Version:** 1.0  
**Last Updated:** 2024  
**Extension:** KislayPHP Queue (In-Process Message Queue)
