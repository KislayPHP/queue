# Queue Class Reference

Runtime classes exported by `kislayphp/queue`.

## Namespace

- Primary: `Kislay\\Queue`
- Legacy alias: `KislayPHP\\Queue`

## `Kislay\\Queue\\ClientInterface`

Contract for pluggable queue backends.

- `enqueue(string $queue, mixed $payload)`
  - Push message to queue.
- `dequeue(string $queue)`
  - Pop next message.
- `size(string $queue)`
  - Return queue depth.

## `Kislay\\Queue\\Queue`

In-memory queue manager with optional backend delegation.

### Constructor

- `__construct()`
  - Create queue manager.

### Backend Injection

- `setClient(ClientInterface $client)`
  - Attach external queue backend client.

### Queue Operations

- `enqueue(string $queue, mixed $payload)`
  - Add message.
- `dequeue(string $queue)`
  - Consume next message.
- `peek(string $queue)`
  - Read next message without removing it.
- `size(string $queue)`
  - Get queue size.
- `clear(string $queue)`
  - Remove all queued items.
