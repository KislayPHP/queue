# kislayphp/queue — notes for AI assistants

Native PHP distributed job queue: standalone queue server
(`Kislay\Queue\Server`), producer/worker clients (`Client`/`Worker`),
retries, delayed jobs, DLQ. Single source file (per-`.cpp` — check the
actual filename in this checkout), civetweb-based server + `Job`/`Queue`
classes.

## The pattern other modules copy from here

`queue`'s accept loop (thread-per-connection + a per-connection I/O
deadline + a connection cap) is the reference implementation other
modules' server modes were meant to mirror. **`discovery`'s accept loop
only copied half of this pattern** (thread-per-connection, but no
deadline/cap) until it was fixed on 2026-08-30 — see `discovery/CLAUDE.md`.
If you're adding a new server mode anywhere in this ecosystem, copy
`queue`'s full pattern, not just the threading half of it.

## Idempotent push — lock scoping matters here

Client-retried pushes are deduplicated by an idempotency key. The
dedup check-then-act (lookup existing job for that key → push new job →
store the key) is **not safe to split across two lock acquisitions** — a
retry racing the original request between "lookup" and "store" would
double-create a job, defeating the whole point of the mechanism. Both the
single-push and batch-push call sites hold `server->lock` across the
entire lookup→push→store sequence (verified 2026-08-30, no gap found). If
you refactor either path, keep that invariant: the entire
check-then-act sequence under one continuous lock hold, not split into
"check" and "act" as separate critical sections. Empty idempotency keys
are deliberately excluded from the cache (an empty key isn't a real
dedup key, don't let it accidentally start matching every empty-key
request against every other one).

## Testing

Standard phpt, `make test`. 2/2 as of 2026-08-30 — audited, no changes
needed this pass.

## Known open issues

None specific to this module as of 2026-08-30.
