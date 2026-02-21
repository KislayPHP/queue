<?php
// php -d extension=modules/kislayphp_queue.so service_communication.php

if (!extension_loaded('kislayphp_queue')) {
    fwrite(STDERR, "kislayphp_queue extension is not loaded\n");
    exit(1);
}

$queue = new Kislay\Queue\Queue();
$traceId = bin2hex(random_bytes(8));
$requestId = bin2hex(random_bytes(8));

$queue->enqueue('svc.inventory.commands', [
    'traceId' => $traceId,
    'requestId' => $requestId,
    'source' => 'orders',
    'target' => 'inventory',
    'type' => 'ReserveStock',
    'payload' => [
        'sku' => 'ABC-1',
        'qty' => 2,
    ],
    'ts' => time(),
]);

// Worker side consumption simulation.
$command = $queue->dequeue('svc.inventory.commands');
$ok = is_array($command) && ($command['type'] ?? '') === 'ReserveStock';

$queue->enqueue('svc.orders.replies', [
    'traceId' => $traceId,
    'requestId' => $requestId,
    'source' => 'inventory',
    'target' => 'orders',
    'status' => $ok ? 'OK' : 'ERROR',
    'payload' => ['reserved' => $ok],
    'ts' => time(),
]);

$reply = $queue->dequeue('svc.orders.replies');
echo json_encode([
    'ok' => $ok,
    'command' => $command,
    'reply' => $reply,
], JSON_PRETTY_PRINT) . PHP_EOL;
