# KislayPHP Queue

KislayPHP Queue is a simple queue for lightweight background jobs in PHP microservices.

## Key Features

- Enqueue, dequeue, and size operations in memory or via a custom client.
- Simple API for local processing.

## Use Cases

- Local job processing during development.
- Prototyping background workflows.

## SEO Keywords

PHP queue, in-memory queue, job queue, background jobs, C++ PHP extension, microservices

## Repository

- https://github.com/KislayPHP/queue

## Related Modules

- https://github.com/KislayPHP/core
- https://github.com/KislayPHP/eventbus
- https://github.com/KislayPHP/discovery
- https://github.com/KislayPHP/gateway
- https://github.com/KislayPHP/config
- https://github.com/KislayPHP/metrics

## Installation

### Via PECL

```bash
pecl install kislayphp_queue
```

Then add to your php.ini:

```ini
extension=kislayphp_queue.so
```

### Manual Build

```sh
phpize
./configure --enable-kislayphp_queue
make
```

## Run Locally

```sh
cd /path/to/queue
php -d extension=modules/kislayphp_queue.so example.php
```

## Custom Client Interface

Default is in-memory. To plug in Redis, MySQL, Mongo, or any other backend, provide
your own PHP client that implements `KislayPHP\Queue\ClientInterface` and call
`setClient()`.

Example:

```php
$queue = new KislayPHP\Queue\Queue();
$queue->setClient(new MyQueueClient());
```

## Example

```php
<?php
extension_loaded('kislayphp_queue') or die('kislayphp_queue not loaded');

$queue = new KislayPHP\Queue\Queue();
$queue->enqueue('jobs', ['id' => 1, 'task' => 'email']);
$queue->enqueue('jobs', ['id' => 2, 'task' => 'sms']);

var_dump($queue->size('jobs'));
var_dump($queue->dequeue('jobs'));
var_dump($queue->size('jobs'));
?>
```
