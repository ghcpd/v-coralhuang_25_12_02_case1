# Outbox Forwarder (Enhanced)

A robust async outbox forwarder using SQLite + SQLAlchemy. It batches, locks, retries with exponential backoff + jitter, writes to a DLQ on unrecoverable failure, and emits a minimal audit trail and metrics logs. Includes a one-click PowerShell test runner.

## Features
- **Batch scan + atomic lock**: `status in (new, failed)` & `next_retry_at <= now`. Uses `status=locked`, `locked_by`, `locked_at` to prevent duplicates across instances.
- **Configurable concurrency**: Bounded async worker pool.
- **Retries**: Exponential backoff with jitter (`next_retry_at`), capped delay, `MAX_ATTEMPTS` threshold.
- **DLQ**: `outbox_dlq` stores unrecoverable items; replay CLI resets them to `new`.
- **Audit trail**: `event_outbox_audit` captures lock/forward/retry/dead/replay/stale_reset actions.
- **Metrics (logs)**: Batch counts and avg per-record latency.
- **Stale lock recovery**: Periodically unlocks `locked` rows older than a timeout.

## Schema
Tables (see `outbox/models.py`):
- `event_outbox`: outbox records with `next_retry_at`, `locked_by`, `locked_at`, `status` (`new|locked|forwarded|failed|dead`).
- `events`: forwarded sink table (idempotent on `id`).
- `outbox_dlq`: dead-lettered entries with reason/attempt.
- `event_outbox_audit`: lifecycle audit log.

## CLI & Modules
- Forwarder: `python -m outbox.forwarder [--exit-on-idle] [--max-iterations N]`
- Replay DLQ: `python -m outbox.forwarder replay-dlq [--ids id1,id2] [--limit N]`
- Seeder: `python -m outbox.seeder [--count N] [--reset] [--fail-once-ratio r] [--fail-always-ratio r]`

Special test topics (seeded):
- `__fail_once__`: fails on first attempt, succeeds on retry.
- `__force_fail__`: always fails → DLQ.

## Configuration (env vars)
| Variable | Default | Description |
|----------|---------|-------------|
| `OUTBOX_DB_URL` | `sqlite+aiosqlite:///./outbox.db` | DB URL |
| `OUTBOX_BATCH_SIZE` | `200` | Max records to lock per scan |
| `OUTBOX_WORKER_CONCURRENCY` | `5` | Concurrent workers |
| `OUTBOX_SCAN_INTERVAL` | `1.0` | Seconds between scans when idle |
| `OUTBOX_MAX_ATTEMPTS` | `5` | Max attempts before DLQ |
| `OUTBOX_BACKOFF_BASE` | `1.0` | Base backoff (seconds) |
| `OUTBOX_BACKOFF_CAP` | `60.0` | Backoff cap (seconds) |
| `OUTBOX_BACKOFF_JITTER` | `0.2` | ±20% jitter fraction |
| `OUTBOX_LOCK_TIMEOUT` | `60` | Stale lock threshold (seconds) |
| `OUTBOX_CLEANUP_INTERVAL` | `30` | Stale lock sweep period (seconds) |
| `OUTBOX_LOG_LEVEL` | `INFO` | Logging level |
| `FORWARDER_ID` | auto-generated | Instance identity |

## One-click test runner
Run `./run_tests.ps1` (Windows PowerShell):
1. Creates `.venv/` if missing; installs `requirements.txt`.
2. Resets & seeds ~1,000 outbox records (incl. failure variants).
3. Runs the forwarder to completion (`--exit-on-idle`). Optionally spawns a second instance to validate locking.
4. Prints summary: forwarded, retries, DLQ, duration.

## Acceptance Criteria Mapping
- **Reliability & throughput**: batch + concurrency; per-record latency logged.
- **No duplicates**: atomic lock + idempotent sink insert.
- **Retries**: `next_retry_at` with exponential backoff + jitter.
- **DLQ & replay**: `outbox_dlq` + `replay-dlq` command.
- **Audit**: `event_outbox_audit` entries for all lifecycle actions.

## Try it manually
```powershell
# seed
python -m outbox.seeder --reset --count 500
# run forwarder once (exit when idle)
python -m outbox.forwarder --exit-on-idle
# replay DLQ (if any)
python -m outbox.forwarder replay-dlq --limit 10
```

## Notes
- SQLite simplifies demo; for production, switch to a transactional DB and keep the same semantics.
- Metrics are log-based; integrate with Prometheus/Otel as needed.
- Forwarder is safe to run multiple instances; locking is DB-coordinated.
