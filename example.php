<?php
// Run from this folder with:
// php -d extension=modules/kislayphp_queue.so example.php

function fail(string $message): void {
	echo "FAIL: {$message}\n";
	exit(1);
}

if (!extension_loaded('kislayphp_queue')) {
	fail('kislayphp_queue not loaded');
}

$queue = new KislayPHP\Queue\Queue();

class ArrayQueueClient implements KislayPHP\Queue\ClientInterface {
	private array $queues = [];

	public function enqueue(string $queue, mixed $payload): bool {
		$this->queues[$queue][] = $payload;
		return true;
	}

	public function dequeue(string $queue): mixed {
		if (empty($this->queues[$queue])) {
			return null;
		}
		return array_shift($this->queues[$queue]);
	}

	public function size(string $queue): int {
		return (int)(isset($this->queues[$queue]) ? count($this->queues[$queue]) : 0);
	}
}

$queue->setClient(new ArrayQueueClient());

$queue->enqueue('jobs', ['id' => 1, 'task' => 'email']);
$queue->enqueue('jobs', ['id' => 2, 'task' => 'sms']);

$sizeBefore = $queue->size('jobs');
if ($sizeBefore !== 2) {
	fail('size before dequeue mismatch');
}

$job = $queue->dequeue('jobs');
if (!is_array($job) || ($job['id'] ?? null) !== 1) {
	fail('dequeue returned unexpected payload');
}

$sizeAfter = $queue->size('jobs');
if ($sizeAfter !== 1) {
	fail('size after dequeue mismatch');
}

echo "OK: queue example passed\n";
