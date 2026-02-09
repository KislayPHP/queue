<?php
// Run from this folder with:
// php -d extension=modules/kislayphp_queue.so example.php

extension_loaded('kislayphp_queue') or die('kislayphp_queue not loaded');

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

$use_client = false;
if ($use_client) {
	$queue->setClient(new ArrayQueueClient());
}
$queue->enqueue('jobs', ['id' => 1, 'task' => 'email']);
$queue->enqueue('jobs', ['id' => 2, 'task' => 'sms']);

var_dump($queue->size('jobs'));
var_dump($queue->dequeue('jobs'));
var_dump($queue->size('jobs'));
