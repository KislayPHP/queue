# KislayPHP Queue

KislayPHP Queue is a simple in-memory queue for lightweight background job flows.

## Key Features

- Enqueue, dequeue, and size operations.
- Simple API for local processing.

## Use Cases

- Local job processing during development.
- Prototyping background workflows.

## SEO Keywords

PHP queue, in-memory queue, job queue, C++ PHP extension

## Repository

- https://github.com/KislayPHP/queue

## Related Modules

- https://github.com/KislayPHP/core
- https://github.com/KislayPHP/eventbus
- https://github.com/KislayPHP/discovery
- https://github.com/KislayPHP/gateway
- https://github.com/KislayPHP/config
- https://github.com/KislayPHP/metrics

## Build

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
