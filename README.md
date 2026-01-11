# Outbox Forwarder — enhanced feature (local case)

This small sandbox adds a more robust outbox forwarder into the existing minimal example located under `outbox/`.

Key features added
- Batch scanning + database-backed locking to avoid duplicates across concurrent worker instances
- Configurable concurrency (workers) and batch size
- Exponential backoff retry scheduling with jitter and a maximum cap
- Dead-letter queue (DLQ) table (`outbox_dlq`) for unrecoverable failures
- Minimal audit trail table (`event_outbox_audit`) for important lifecycle events
- Simple metrics/logging output (counts + elapsed times)

Files of interest
- `outbox/models.py` — extended OutboxEvent (next_retry_at, locked_at, locked_by) + `OutboxDLQ`, `OutboxAudit`
- `outbox/forwarder.py` — main forwarder with locking, concurrency, retry/backoff, stale-lock recovery and DLQ
- `scripts/test_runner.py` — small harness that seeds a dataset, runs forwarders and prints a summary
- `run_tests.ps1` — one-click Windows PowerShell runner (creates venv, installs dependencies and runs the test harness)
- `requirements.txt` — minimal folder-scoped dependencies

How to run (Windows PowerShell)

1. From this folder run the one-click tester (creates a .venv, installs dependencies and runs the seed+forwarder test):

```powershell
.\run_tests.ps1
```

2. Manual run from an activated venv (if preferred):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python .\scripts\test_runner.py --count 800 --fail-rate 0.03 --workers 3
```

Configuration
- All behaviour is driven by environment variables. Example values are set by `run_tests.ps1` to make the test fast.

Key environment variables
DLQ replay
- To replay all DLQ entries back into the outbox (reset attempts to 0):

```powershell
# from repo root after activating .venv
.\.venv\Scripts\python.exe .\scripts\replay_dlq.py --all
```

- OUTBOX_BATCH_SIZE — how many records to pick per scan
- OUTBOX_WORKERS — how many worker coroutines to run concurrently during processing
- OUTBOX_BASE_BACKOFF_SECONDS — base for exponential backoff (seconds)
- OUTBOX_MAX_BACKOFF_SECONDS — maximum backoff (seconds)
- OUTBOX_JITTER_SECONDS — jitter added to backoff (seconds)
- OUTBOX_MAX_ATTEMPTS — how many delivery attempts before moving to DLQ

Design notes and constraints
- Uses SQLite + SQLAlchemy (async) for portability and simplicity
- The locking mechanism is implemented with an atomic UPDATE based on selected IDs and status — not a distributed lease, but prevents duplicate processing across processes
- Stale locks are automatically recovered by the forwarder after a configurable timeout
- For now, metrics are simple logs and a short summary in the test harness; full metrics systems (Prometheus etc.) are out of scope for this task

Testing behaviour
- The test harness will intentionally seed some records with a marker in the payload so the forwarder triggers an exception and demonstrates retry scheduling and DLQ behaviour.
