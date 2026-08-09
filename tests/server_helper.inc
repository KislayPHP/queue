<?php
function fail($message) {
    echo "FAIL: {$message}\n";
    exit(1);
}

function reserve_free_port() {
    $socket = @stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr);
    if (!$socket) {
        fail("Unable to reserve free port: {$errstr}");
    }
    $name = stream_socket_get_name($socket, false);
    fclose($socket);
    $parts = explode(':', $name);
    $port = (int) end($parts);
    if ($port <= 0) {
        fail('Reserved port is invalid');
    }
    return $port;
}

function kislay_queue_test_extension_path() {
    $fromEnv = getenv('KISLAYPHP_QUEUE_TEST_EXTENSION');
    if (is_string($fromEnv) && $fromEnv !== '' && is_file($fromEnv)) {
        return $fromEnv;
    }

    $extensionDir = ini_get('extension_dir');
    if (is_string($extensionDir) && $extensionDir !== '') {
        $candidate = rtrim($extensionDir, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . 'kislayphp_queue.so';
        if (is_file($candidate)) {
            return $candidate;
        }
    }

    fail('Unable to locate kislayphp_queue.so for subprocess server');
}

function wait_for_queue_server($host, $port, $proc, $logFile, $timeoutMs = 3000) {
    $deadline = microtime(true) + ($timeoutMs / 1000.0);
    while (microtime(true) < $deadline) {
        $fp = @fsockopen($host, $port, $errno, $errstr, 0.1);
        if ($fp) {
            fclose($fp);
            return;
        }

        $status = proc_get_status($proc);
        if (!$status['running']) {
            $log = is_file($logFile) ? trim((string) file_get_contents($logFile)) : '';
            fail('Queue server process exited early' . ($log !== '' ? ": {$log}" : ''));
        }
        usleep(50000);
    }

    $log = is_file($logFile) ? trim((string) file_get_contents($logFile)) : '';
    fail('Timed out waiting for queue server' . ($log !== '' ? ": {$log}" : ''));
}

function start_kislay_queue_server($bootstrap, $host, $port) {
    $script = tempnam(sys_get_temp_dir(), 'kislay_queue_srv_');
    $logFile = tempnam(sys_get_temp_dir(), 'kislay_queue_srv_log_');
    $php = PHP_BINARY;
    $extension = kislay_queue_test_extension_path();
    $code = "<?php\n" . $bootstrap . "\n";
    file_put_contents($script, $code);

    $command = escapeshellarg($php)
        . ' -n -d extension=' . escapeshellarg($extension)
        . ' ' . escapeshellarg($script);

    $descriptors = [
        0 => ['file', '/dev/null', 'r'],
        1 => ['file', $logFile, 'a'],
        2 => ['file', $logFile, 'a'],
    ];

    $proc = proc_open($command, $descriptors, $pipes);
    if (!is_resource($proc)) {
        fail('Unable to start Kislay queue test server process');
    }

    wait_for_queue_server($host, $port, $proc, $logFile);

    return [
        'proc' => $proc,
        'script' => $script,
        'log' => $logFile,
    ];
}

function stop_kislay_queue_server(&$server) {
    if (!is_array($server)) {
        return;
    }

    if (isset($server['proc']) && is_resource($server['proc'])) {
        @proc_terminate($server['proc']);
        $deadline = microtime(true) + 2.0;
        do {
            $status = proc_get_status($server['proc']);
            if (!$status['running']) {
                break;
            }
            usleep(50000);
        } while (microtime(true) < $deadline);

        $status = proc_get_status($server['proc']);
        if ($status['running']) {
            @proc_terminate($server['proc'], 9);
        }
        @proc_close($server['proc']);
    }

    foreach (['script', 'log'] as $key) {
        if (!empty($server[$key]) && is_file($server[$key])) {
            @unlink($server[$key]);
        }
    }

    $server = null;
}
