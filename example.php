<?php
// Run from this folder with:
// php -d extension=modules/kislayphp_queue.so example.php

extension_loaded('kislayphp_queue') or die('kislayphp_queue not loaded');

$queue = new KislayPHP\Queue\Queue();
$queue->enqueue('jobs', ['id' => 1, 'task' => 'email']);
$queue->enqueue('jobs', ['id' => 2, 'task' => 'sms']);

var_dump($queue->size('jobs'));
var_dump($queue->dequeue('jobs'));
var_dump($queue->size('jobs'));
