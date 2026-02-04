# Outbox Forwarder (Enhanced)

This project enhances a minimal Outbox Forwarder with reliability, concurrency, retry scheduling, dead-letter queue (DLQ), audit trail, and metrics.

## Features
- Batch scanning with atomic locking to avoid duplicate forwarding across instances.
- Configurable concurrency worker pool.
- Exponential backoff with jitter using `next_retry_at`.
- Dead-letter queue (`outbox_dlq`) and replay utility.
- Audit trail for lock/forward/retry/dead transitions.
- Minimal metrics/logs: counts and latencies.

## Quickstart
```
# PowerShell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m outbox.test_runner --seed 1000 --run
```

## One-click Test
Run `./run_tests.ps1`. It will:
- Ensure venv and install requirements.
- Seed sample data (default 1000 rows, with some intentionally failing).
- Run forwarder to completion, optionally start a second instance to validate locking.
- Print a compact summary (forwarded, failed, DLQ, timings).

## Schema
- `event_outbox`: adds `next_retry_at`, `locked_at`, `lock_owner`, `status` (new|locked|forwarded|failed|dead), `attempt`.
- `events`: unchanged.
- `outbox_dlq`: id, topic, payload, error, attempts, created_at, failed_at, original_created_at.
- `event_outbox_audit`: id, outbox_id, action, details, created_at.

## Forwarder Flow
1. Scan eligible rows (status in new|failed, next_retry_at <= now).
2. Atomically lock a batch (`status=locked`, `locked_at`, `lock_owner`).
3. Process each record in bounded concurrency.
4. Success: insert into `events`, mark outbox `forwarded`, audit.
5. Failure: increment attempt, compute next_retry_at with backoff+jitter; if exceeds max, move to DLQ and mark `dead`, audit.
6. Stale locks: rows `locked` older than timeout reset to `new`.
7. Metrics: log counts and latencies per batch.

## DLQ Replay
Run `python -m outbox.forwarder --replay-dlq all` or `--replay-dlq id1,id2` to move DLQ records back to outbox with `attempt=0` and `status=new`.

## Configuration
Environment variables:
- `OUTBOX_DB_URL` (default `sqlite+aiosqlite:///./outbox.db`)
- `OUTBOX_BATCH_SIZE` (default 100)
- `OUTBOX_CONCURRENCY` (default 10)
- `OUTBOX_SCAN_INTERVAL` seconds (default 1)
- `OUTBOX_MAX_ATTEMPTS` (default 5)
- `OUTBOX_BACKOFF_BASE` seconds (default 1)
- `OUTBOX_BACKOFF_FACTOR` (default 2)
- `OUTBOX_BACKOFF_MAX` seconds (default 60)
- `OUTBOX_STALE_LOCK_SECONDS` (default 30)
- `OUTBOX_INSTANCE_ID` (default random UUID)

## Notes
- Uses SQLite + SQLAlchemy (async).
- Designed for local demo; atomic locking via single UPDATE…RETURNING.
- Metrics via logging; extend as needed.
