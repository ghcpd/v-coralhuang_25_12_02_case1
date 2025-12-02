from __future__ import annotations
import asyncio
import os
import random
from datetime import datetime, timedelta
import logging
from typing import List, Optional
from sqlalchemy import select, update, and_, func
from sqlalchemy.ext.asyncio import AsyncSession

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, Event, OutboxDLQ, OutboxAudit

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("outbox_forwarder")

BATCH_SIZE = int(os.getenv("OUTBOX_BATCH_SIZE", "100"))
SCAN_INTERVAL_SECONDS = int(os.getenv("OUTBOX_SCAN_INTERVAL_SECONDS", "1"))
MAX_ATTEMPTS = int(os.getenv("OUTBOX_MAX_ATTEMPTS", "5"))
WORKERS = int(os.getenv("OUTBOX_WORKERS", "10"))
LOCK_TIMEOUT_SECONDS = int(os.getenv("OUTBOX_LOCK_TIMEOUT_SECONDS", "30"))
BASE_BACKOFF_SECONDS = int(os.getenv("OUTBOX_BASE_BACKOFF_SECONDS", "2"))
MAX_BACKOFF_SECONDS = int(os.getenv("OUTBOX_MAX_BACKOFF_SECONDS", "300"))
JITTER_SECONDS = int(os.getenv("OUTBOX_JITTER_SECONDS", "3"))

async def fetch_and_lock_batch(session: AsyncSession, owner: str) -> List[OutboxEvent]:
    """Find candidate outbox rows, lock them for this worker instance and return the locked rows.

    Uses a select->update approach that is safe across concurrent processes because the UPDATE
    enforces the status and next_retry_at match the selection criteria.
    """
    now = datetime.utcnow()
    # Select candidate ids
    select_stmt = (
        select(OutboxEvent.id)
        .where(
            OutboxEvent.status.in_(["new", "failed"]),
            (OutboxEvent.next_retry_at == None) | (OutboxEvent.next_retry_at <= now),
        )
        .order_by(OutboxEvent.created_at)
        .limit(BATCH_SIZE)
    )
    rows = (await session.execute(select_stmt)).scalars().all()
    if not rows:
        return []

    # Try to atomically mark those rows as locked for this owner
    upd = (
        update(OutboxEvent)
        .where(
            OutboxEvent.id.in_(rows),
            OutboxEvent.status.in_(["new", "failed"]),
            (OutboxEvent.next_retry_at == None) | (OutboxEvent.next_retry_at <= now),
        )
        .values(status="locked", locked_at=now, locked_by=owner, updated_at=now)
    )
    result = await session.execute(upd)
    await session.commit()

    # Fetch the rows that were actually locked
    if result.rowcount == 0:
        return []

    fetch = select(OutboxEvent).where(OutboxEvent.locked_by == owner, OutboxEvent.status == "locked")
    locked = (await session.execute(fetch)).scalars().all()
    return locked

async def forward_record(session: AsyncSession, record: OutboxEvent) -> None:
    # idempotent delivery — ensure we do not duplicate events
    existed = await session.get(Event, record.id)
    if existed:
        # already delivered
        record.status = "forwarded"
        record.updated_at = datetime.utcnow()
        await session.flush()
        return

    # deliver (create event)
    # (test-only) fail fast for payloads containing a magic marker so the runner can exercise retries/DLQ
    if isinstance(record.payload, str) and "__bad__" in record.payload:
        raise RuntimeError("Simulated delivery failure (test trigger)")

    evt = Event(id=record.id, topic=record.topic, payload=record.payload)
    session.add(evt)
    record.status = "forwarded"
    record.updated_at = datetime.utcnow()
    await session.flush()

async def schedule_retry_or_dlq(session: AsyncSession, r: OutboxEvent, err: Exception):
    r.attempt += 1
    now = datetime.utcnow()
    if r.attempt >= MAX_ATTEMPTS:
        # move to DLQ
        dlq = OutboxDLQ(id=r.id, topic=r.topic, payload=r.payload, failed_at=now, last_attempt=r.attempt, reason=str(err))
        session.add(dlq)
        # delete from outbox
        await session.delete(r)
        # audit
        aud = OutboxAudit(outbox_id=r.id, action="moved_to_dlq", attempt=r.attempt, detail=str(err))
        session.add(aud)
        LOGGER.warning("Moved to DLQ id=%s attempts=%d", r.id, r.attempt)
        return

    # Otherwise schedule exponential backoff with jitter
    backoff = min(BASE_BACKOFF_SECONDS * (2 ** (r.attempt - 1)), MAX_BACKOFF_SECONDS)
    jitter = timedelta(seconds=random.uniform(0, JITTER_SECONDS))
    r.next_retry_at = now + timedelta(seconds=backoff) + jitter
    r.status = "failed"
    r.updated_at = now
    aud = OutboxAudit(outbox_id=r.id, action="retry_scheduled", attempt=r.attempt, detail=f"next_retry_at={r.next_retry_at} err={err}")
    session.add(aud)


async def process_batch_concurrent(session: AsyncSession, batch: List[OutboxEvent], owner: str, concurrency: int = WORKERS):
    """Process a batch using independent sessions per worker to avoid reentrancy on a shared AsyncSession.

    The `batch` passed in is expected to be records locked for the `owner`. We will reload each record
    inside its own session before processing to avoid flushing races.
    """
    sem = asyncio.Semaphore(concurrency)
    results = {"forwarded": 0, "failed": 0}
    lock = asyncio.Lock()

    async def _handle(record: OutboxEvent):
        async with sem:
            async with SessionLocal() as worker_session:
                # reload fresh in worker session
                r = await worker_session.get(OutboxEvent, record.id)
                if not r:
                    return
                # ensure this worker owns the lock
                if r.locked_by != owner or r.status != "locked":
                    LOGGER.debug("Skipping id=%s; owner mismatch or not locked", record.id)
                    return

                try:
                    await forward_record(worker_session, r)
                    aud = OutboxAudit(outbox_id=r.id, action="forwarded", attempt=r.attempt, detail=None)
                    worker_session.add(aud)
                    await worker_session.commit()
                    async with lock:
                        results["forwarded"] += 1
                except Exception as e:
                    LOGGER.exception("Forward failed id=%s", r.id)
                    await schedule_retry_or_dlq(worker_session, r, e)
                    await worker_session.commit()
                    async with lock:
                        results["failed"] += 1

    tasks = [asyncio.create_task(_handle(r)) for r in batch]
    await asyncio.gather(*tasks)
    return results

async def _recover_stale_locks(session: AsyncSession):
    """Reset locked rows older than LOCK_TIMEOUT_SECONDS back to new so they can be picked up."""
    cutoff = datetime.utcnow() - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
    upd = (
        update(OutboxEvent)
        .where(OutboxEvent.status == "locked", OutboxEvent.locked_at <= cutoff)
        .values(status="new", locked_at=None, locked_by=None, updated_at=datetime.utcnow())
    )
    result = await session.execute(upd)
    if result.rowcount > 0:
        # add an audit row for each unlocked item
        # (keep simple: we insert a generic audit for all unlocked ones)
        LOGGER.info("Recovered %d stale locks", result.rowcount)
    await session.commit()


async def run_loop(owner: Optional[str] = None, single_pass: bool = False):
    import random

    await init_db()
    owner = owner or f"forwarder-{os.getpid()}-{random.randint(0,9999)}"
    LOGGER.info("Outbox forwarder started owner=%s (batch=%d interval=%ds workers=%d)", owner, BATCH_SIZE, SCAN_INTERVAL_SECONDS, WORKERS)

    while True:
        async with SessionLocal() as session:
            # recover stale locks occasionally
            await _recover_stale_locks(session)

            batch = await fetch_and_lock_batch(session, owner)
            if batch:
                LOGGER.info("Picked up %d records (owner=%s)", len(batch), owner)
                start = datetime.utcnow()
                res = await process_batch_concurrent(session, batch, owner, concurrency=WORKERS)
                elapsed = (datetime.utcnow() - start).total_seconds()
                LOGGER.info("Batch result forwarded=%d failed=%d elapsed=%.3fs", res.get("forwarded",0), res.get("failed",0), elapsed)
            else:
                LOGGER.debug("No eligible outbox records to lock/pick")

        if single_pass:
            LOGGER.info("Single-pass mode - exiting")
            break

        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        LOGGER.info("Forwarder stopped by user")
