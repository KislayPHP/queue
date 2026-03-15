# Release Notes

## 0.0.4

- added standalone `Kislay\Queue\Server`
- added remote `Kislay\Queue\Client`
- added `Kislay\Queue\Worker`
- added `Kislay\Queue\Job`
- added queue leasing, retries, delayed jobs, and DLQ routing
- kept legacy local `Kislay\Queue\Queue` for development fallback
- documented the current limitation that queue state is still in-memory only
