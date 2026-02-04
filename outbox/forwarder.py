from __future__ import annotations
import asyncio
import os
import logging
import random
import time
import uuid
from datetime import datetime, timedelta
from typing import List, Optional, Dict
from sqlalchemy import select, update, or_
from sqlalchemy.ext.asyncio import AsyncSession

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, Event, OutboxDLQ, EventOutboxAudit

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("outbox_forwarder")

# Config
BATCH_SIZE = int(os.getenv("OUTBOX_BATCH_SIZE", 100))
SCAN_INTERVAL_SECONDS = float(os.getenv("OUTBOX_SCAN_INTERVAL", 1))
MAX_ATTEMPTS = int(os.getenv("OUTBOX_MAX_ATTEMPTS", 5))
CONCURRENCY = int(os.getenv("OUTBOX_CONCURRENCY", 10))
BACKOFF_BASE = float(os.getenv("OUTBOX_BACKOFF_BASE", 1))
BACKOFF_FACTOR = float(os.getenv("OUTBOX_BACKOFF_FACTOR", 2))
BACKOFF_MAX = float(os.getenv("OUTBOX_BACKOFF_MAX", 60))
BACKOFF_JITTER = float(os.getenv("OUTBOX_BACKOFF_JITTER", 0.3))
STALE_LOCK_SECONDS = float(os.getenv("OUTBOX_STALE_LOCK_SECONDS", 30))
INSTANCE_ID = os.getenv("OUTBOX_INSTANCE_ID", str(uuid.uuid4()))

# Helpers
def _utcnow() -> datetime:
    return datetime.utcnow()

def compute_next_retry(attempt: int) -> datetime:
    base = BACKOFF_BASE * (BACKOFF_FACTOR ** max(attempt - 1, 0))
    backoff = min(base, BACKOFF_MAX)
    jitter = backoff * random.uniform(0, BACKOFF_JITTER)
    return _utcnow() + timedelta(seconds=backoff + jitter)

async def add_audit(session: AsyncSession, outbox_id: str, action: str, details: Optional[str] = None):
    session.add(EventOutboxAudit(outbox_id=outbox_id, action=action, details=details))

async def lock_batch(session: AsyncSession, now: datetime, instance_id: str = INSTANCE_ID) -> List[str]:
    """Atomically lock a batch of eligible rows, returning their ids."""
    subq = (
        select(OutboxEvent.id)
        .where(
            OutboxEvent.status.in_(["new", "failed"]),
            or_(OutboxEvent.next_retry_at.is_(None), OutboxEvent.next_retry_at <= now),
        )
        .order_by(OutboxEvent.created_at)
        .limit(BATCH_SIZE)
        .scalar_subquery()
    )
    result = await session.execute(
        update(OutboxEvent)
        .where(OutboxEvent.id.in_(subq))
        .values(status="locked", locked_at=now, lock_owner=instance_id, updated_at=now)
        .returning(OutboxEvent.id)
    )
    ids = [r[0] for r in result.fetchall()]
    await session.commit()
    if not ids:
        return []
    # audit lock
    async with SessionLocal() as audit_sess:
        for oid in ids:
            await add_audit(audit_sess, oid, "locked", f"owner={instance_id}")
        await audit_sess.commit()
    return ids

async def recover_stale_locks(session: AsyncSession, now: datetime) -> int:
    cutoff = now - timedelta(seconds=STALE_LOCK_SECONDS)
    result = await session.execute(
        update(OutboxEvent)
        .where(OutboxEvent.status == "locked", OutboxEvent.locked_at < cutoff)
        .values(status="new", locked_at=None, lock_owner=None, updated_at=now)
        .returning(OutboxEvent.id)
    )
    ids = [r[0] for r in result.fetchall()]
    await session.commit()
    if ids:
        async with SessionLocal() as audit_sess:
            for oid in ids:
                await add_audit(audit_sess, oid, "stale_unlock")
            await audit_sess.commit()
    if ids:
        LOGGER.warning("Recovered %d stale locks", len(ids))
    return len(ids)

async def forward_one(outbox_id: str, instance_id: str = INSTANCE_ID) -> Dict:
    start = time.monotonic()
    async with SessionLocal() as session:
        record = await session.get(OutboxEvent, outbox_id)
        if not record or record.status != "locked":
            return {"id": outbox_id, "status": "skipped"}
        now = _utcnow()
        try:
            existed = await session.get(Event, record.id)
            if not existed:
                evt = Event(id=record.id, topic=record.topic, payload=record.payload)
                session.add(evt)
            record.status = "forwarded"
            record.next_retry_at = None
            record.lock_owner = None
            record.locked_at = None
            record.updated_at = now
            await add_audit(session, record.id, "forwarded")
            await session.commit()
            latency = now - record.created_at
            return {"id": record.id, "status": "forwarded", "latency": latency.total_seconds()}
        except Exception as e:
            record.attempt += 1
            is_dead = record.attempt >= MAX_ATTEMPTS or isinstance(e, ValueError)
            if is_dead:
                # dead-letter
                dlq = OutboxDLQ(
                    id=record.id,
                    topic=record.topic,
                    payload=record.payload,
                    error=str(e),
                    attempts=record.attempt,
                    original_created_at=record.created_at,
                )
                session.add(dlq)
                record.status = "dead"
                await add_audit(session, record.id, "dead", str(e))
            else:
                record.status = "failed"
                record.next_retry_at = compute_next_retry(record.attempt)
                await add_audit(session, record.id, "retry_scheduled", str(e))
            record.lock_owner = None
            record.locked_at = None
            record.updated_at = _utcnow()
            await session.commit()
            return {"id": record.id, "status": record.status, "error": str(e)}

async def process_locked(ids: List[str], instance_id: str = INSTANCE_ID) -> Dict[str, int]:
    sem = asyncio.Semaphore(CONCURRENCY)
    results = []

    async def worker(oid: str):
        async with sem:
            res = await forward_one(oid, instance_id=instance_id)
            results.append(res)

    await asyncio.gather(*(worker(i) for i in ids))
    # aggregate metrics
    forwarded = sum(1 for r in results if r.get("status") == "forwarded")
    failed = sum(1 for r in results if r.get("status") == "failed")
    dead = sum(1 for r in results if r.get("status") == "dead")
    latencies = [r["latency"] for r in results if r.get("latency") is not None]
    if latencies:
        avg_latency = sum(latencies) / len(latencies)
    else:
        avg_latency = None
    LOGGER.info("Batch processed: forwarded=%d failed=%d dead=%d avg_latency=%s", forwarded, failed, dead, f"{avg_latency:.3f}s" if avg_latency else "n/a")
    return {
        "forwarded": forwarded,
        "failed": failed,
        "dead": dead,
        "count": len(results),
    }

async def run_once(instance_id: str = INSTANCE_ID) -> Dict[str, int]:
    now = _utcnow()
    async with SessionLocal() as session:
        await recover_stale_locks(session, now)
        ids = await lock_batch(session, now, instance_id=instance_id)
    if not ids:
        return {"forwarded": 0, "failed": 0, "dead": 0, "count": 0}
    return await process_locked(ids, instance_id=instance_id)

async def run_loop(duration_seconds: Optional[int] = None, instance_id: str = INSTANCE_ID):
    await init_db()
    LOGGER.info(
        "Outbox forwarder started (batch=%d interval=%ss concurrency=%d instance=%s)",
        BATCH_SIZE, SCAN_INTERVAL_SECONDS, CONCURRENCY, instance_id,
    )
    start_time = time.monotonic()
    while True:
        stats = await run_once(instance_id=instance_id)
        if stats.get("count", 0) == 0:
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)
        if duration_seconds and (time.monotonic() - start_time) > duration_seconds:
            LOGGER.info("Duration reached, stopping loop")
            break

async def run_until_idle(max_idle_loops: int = 3, instance_id: str = INSTANCE_ID) -> Dict[str, int]:
    await init_db()
    idle = 0
    aggregate = {"forwarded": 0, "failed": 0, "dead": 0, "count": 0}
    while idle < max_idle_loops:
        stats = await run_once(instance_id=instance_id)
        for k in aggregate:
            aggregate[k] += stats.get(k, 0)
        if stats.get("count", 0) == 0:
            idle += 1
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)
        else:
            idle = 0
    LOGGER.info("Idle reached: %s", aggregate)
    return aggregate

async def replay_dlq(ids: Optional[List[str]] = None):
    async with SessionLocal() as session:
        if ids:
            rows = (await session.execute(select(OutboxDLQ).where(OutboxDLQ.id.in_(ids)))).scalars().all()
        else:
            rows = (await session.execute(select(OutboxDLQ))).scalars().all()
        now = _utcnow()
        for r in rows:
            ob = OutboxEvent(
                id=r.id,
                topic=r.topic,
                payload=r.payload,
                status="new",
                attempt=0,
                created_at=r.original_created_at or now,
                updated_at=now,
            )
            session.merge(ob)
            await add_audit(session, r.id, "dlq_replayed")
            await session.delete(r)
        await session.commit()
    LOGGER.info("Replayed %d DLQ records", len(rows))

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Outbox forwarder")
    parser.add_argument("--run-once", action="store_true", help="Run one scan cycle")
    parser.add_argument("--duration", type=int, default=None, help="Run loop for duration seconds")
    parser.add_argument("--replay-dlq", type=str, default=None, help="Replay DLQ ids (comma-separated) or 'all'")
    parser.add_argument("--instance-id", type=str, default=INSTANCE_ID, help="Override instance id")
    args = parser.parse_args()

    if args.replay_dlq:
        ids = None if args.replay_dlq == "all" else args.replay_dlq.split(",")
        asyncio.run(replay_dlq(ids))
    elif args.run_once:
        asyncio.run(run_once(instance_id=args.instance_id))
    else:
        asyncio.run(run_loop(duration_seconds=args.duration, instance_id=args.instance_id))
