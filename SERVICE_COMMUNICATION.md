# Service Communication Guide (Queue)

This extension is the async communication channel between services.

## Namespace

- Primary: `Kislay\Queue\Queue`
- Backward compatible alias: `KislayPHP\Queue\Queue`

## Pattern

Use queue names as service channels:

- `svc.<service>.commands` for commands
- `svc.<service>.replies` for responses
- `evt.<domain>.<event>` for domain events

Use a common envelope in payloads:

```php
[
    'traceId' => '...',
    'requestId' => '...',
    'source' => 'orders',
    'target' => 'inventory',
    'type' => 'ReserveStock',
    'payload' => [...],
    'ts' => time(),
]
```

## Minimal Request/Reply Example

See `service_communication.php` in this repository.

## Recommended Cross-Module Setup

1. Use `kislayphp/config` for channel names and retry values.
2. Use `kislayphp/metrics` to count command success/failure.
3. Use `kislayphp/eventbus` for realtime notifications.
4. Use `kislayphp/discovery` + `kislayphp/gateway` for sync APIs.
