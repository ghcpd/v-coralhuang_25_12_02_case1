from __future__ import annotations
import asyncio
import logging
import os
import random
import socket
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from sqlalchemy import select, update, and_, or_, func

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, Event, OutboxDLQ, OutboxAudit

logging.basicConfig(level=os.getenv("OUTBOX_LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("outbox_forwarder")

# Configurable via env
BATCH_SIZE = int(os.getenv("OUTBOX_BATCH_SIZE", 200))
SCAN_INTERVAL_SECONDS = float(os.getenv("OUTBOX_SCAN_INTERVAL", 1.0))
WORKER_CONCURRENCY = int(os.getenv("OUTBOX_WORKER_CONCURRENCY", 5))
MAX_ATTEMPTS = int(os.getenv("OUTBOX_MAX_ATTEMPTS", 5))
LOCK_TIMEOUT_SECONDS = float(os.getenv("OUTBOX_LOCK_TIMEOUT", 60))
CLEANUP_INTERVAL_SECONDS = float(os.getenv("OUTBOX_CLEANUP_INTERVAL", 30))
BACKOFF_BASE_SECONDS = float(os.getenv("OUTBOX_BACKOFF_BASE", 1.0))
BACKOFF_CAP_SECONDS = float(os.getenv("OUTBOX_BACKOFF_CAP", 60.0))
BACKOFF_JITTER_FACTOR = float(os.getenv("OUTBOX_BACKOFF_JITTER", 0.2))

FORWARDER_ID = os.getenv("FORWARDER_ID", f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:8]}")


def compute_backoff(attempt: int) -> float:
    """Exponential backoff (attempt starts at 1)."""
    base = BACKOFF_BASE_SECONDS * (2 ** max(attempt - 1, 0))
    return min(base, BACKOFF_CAP_SECONDS)


def jitter_delay(base_delay: float, jitter_factor: float = BACKOFF_JITTER_FACTOR) -> float:
    if jitter_factor <= 0:
        return max(base_delay, 0)
    jitter = random.uniform(-jitter_factor, jitter_factor) * base_delay
    return max(base_delay + jitter, 0)


async def add_audit_entries(session, ids: List[str], action: str, status_before: Optional[str] = None,
                            status_after: Optional[str] = None, attempt: Optional[int] = None, note: Optional[str] = None):
    now = datetime.utcnow()
    for oid in ids:
        session.add(OutboxAudit(outbox_id=oid, action=action, status_before=status_before,
                                status_after=status_after, attempt=attempt, note=note, created_at=now))


async def audit_single(session, oid: str, action: str, status_before: Optional[str] = None,
                       status_after: Optional[str] = None, attempt: Optional[int] = None, note: Optional[str] = None):
    await add_audit_entries(session, [oid], action, status_before, status_after, attempt, note)


async def lock_batch(batch_size: int) -> List[str]:
    now = datetime.utcnow()
    stale_threshold = now - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
    async with SessionLocal() as session:
        candidate_ids = (
            await session.execute(
                select(OutboxEvent.id)
                .where(
                    OutboxEvent.status.in_(["new", "failed"]),
                    or_(OutboxEvent.next_retry_at == None, OutboxEvent.next_retry_at <= now),
                    or_(OutboxEvent.locked_by == None, OutboxEvent.locked_at <= stale_threshold),
                )
                .order_by(OutboxEvent.created_at)
                .limit(batch_size)
            )
        ).scalars().all()

        if not candidate_ids:
            return []

        upd = (
            update(OutboxEvent)
            .where(
                OutboxEvent.id.in_(candidate_ids),
                OutboxEvent.status.in_(["new", "failed"]),
                or_(OutboxEvent.locked_by == None, OutboxEvent.locked_at <= stale_threshold),
            )
            .values(status="locked", locked_by=FORWARDER_ID, locked_at=now, updated_at=now)
            .returning(OutboxEvent.id)
        )
        res = await session.execute(upd)
        locked_ids = [row[0] for row in res.fetchall()]
        if locked_ids:
            await add_audit_entries(session, locked_ids, action="lock", status_after="locked")
        await session.commit()
        return locked_ids


async def recover_stale_locks() -> List[str]:
    async with SessionLocal() as session:
        threshold = datetime.utcnow() - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
        upd = (
            update(OutboxEvent)
            .where(OutboxEvent.status == "locked", OutboxEvent.locked_at <= threshold)
            .values(status="new", locked_by=None, locked_at=None, updated_at=datetime.utcnow())
            .returning(OutboxEvent.id)
        )
        res = await session.execute(upd)
        ids = [r[0] for r in res.fetchall()]
        if ids:
            await add_audit_entries(session, ids, action="stale_reset", status_before="locked", status_after="new")
        await session.commit()
        if ids:
            LOGGER.warning("Recovered stale locks: %s", ids)
        return ids


async def pending_exists() -> bool:
    """Return True if any new/failed rows remain (ready now or scheduled)."""
    async with SessionLocal() as session:
        count = await session.scalar(select(func.count()).where(OutboxEvent.status.in_(["new", "failed"])))
        return bool(count)


async def replay_dlq(ids: Optional[List[str]] = None, limit: Optional[int] = None) -> int:
    """Replay DLQ entries back into the outbox (status=new, attempt=0). Returns count replayed."""
    async with SessionLocal() as session:
        query = select(OutboxDLQ)
        if ids:
            query = query.where(OutboxDLQ.id.in_(ids))
        if limit:
            query = query.limit(limit)
        items = (await session.execute(query)).scalars().all()
        if not items:
            LOGGER.info("No DLQ items to replay")
            return 0
        now = datetime.utcnow()
        replayed = 0
        for item in items:
            ob = await session.get(OutboxEvent, item.id)
            if not ob:
                ob = OutboxEvent(id=item.id, topic=item.topic, payload=item.payload,
                                 status="new", attempt=0, created_at=item.created_at, updated_at=now)
                session.add(ob)
                status_before = None
            else:
                status_before = ob.status
                ob.status = "new"
                ob.attempt = 0
                ob.next_retry_at = None
                ob.locked_by = None
                ob.locked_at = None
                ob.updated_at = now
            await audit_single(session, ob.id, action="replay", status_before=status_before,
                               status_after="new", attempt=0, note="replay_dlq")
            await session.delete(item)
            replayed += 1
        await session.commit()
        LOGGER.info("Replayed %d DLQ items", replayed)
        return replayed


async def handle_record(record_id: str) -> Dict[str, Any]:
    async with SessionLocal() as session:
        record = await session.get(OutboxEvent, record_id)
        if not record:
            return {"id": record_id, "status": "missing"}
        if record.status != "locked":
            return {"id": record.id, "status": record.status, "note": "not locked"}

        status_before = record.status
        try:
            # Optional simulated failures based on topic for testing
            if record.topic == "__force_fail__":
                raise RuntimeError("Forced failure for testing (always)")
            if record.topic == "__fail_once__" and record.attempt == 0:
                raise RuntimeError("Forced failure for testing (once)")

            existed = await session.get(Event, record.id)
            if existed:
                record.status = "forwarded"
                record.locked_by = None
                record.locked_at = None
                record.updated_at = datetime.utcnow()
                await audit_single(session, record.id, action="forward", status_before=status_before,
                                   status_after=record.status, attempt=record.attempt, note="idempotent")
                await session.commit()
                latency = (record.updated_at - record.created_at).total_seconds()
                return {"id": record.id, "status": "forwarded", "latency": latency, "idempotent": True}

            evt = Event(id=record.id, topic=record.topic, payload=record.payload)
            session.add(evt)
            record.status = "forwarded"
            record.locked_by = None
            record.locked_at = None
            record.updated_at = datetime.utcnow()
            await audit_single(session, record.id, action="forward", status_before=status_before,
                               status_after=record.status, attempt=record.attempt)
            await session.commit()
            latency = (record.updated_at - record.created_at).total_seconds()
            return {"id": record.id, "status": "forwarded", "latency": latency}
        except Exception as e:
            note = f"{type(e).__name__}: {e}"
            record.attempt += 1
            now = datetime.utcnow()
            delay = jitter_delay(compute_backoff(record.attempt))
            if record.attempt >= MAX_ATTEMPTS:
                # DLQ
                record.status = "dead"
                record.locked_by = None
                record.locked_at = None
                record.next_retry_at = None
                dlq = OutboxDLQ(id=record.id, topic=record.topic, payload=record.payload,
                                attempt=record.attempt, reason=note, created_at=record.created_at,
                                dead_letter_at=now)
                session.add(dlq)
                await audit_single(session, record.id, action="dead", status_before=status_before,
                                   status_after=record.status, attempt=record.attempt, note=note)
                await session.commit()
                LOGGER.error("DLQ id=%s attempt=%d reason=%s", record.id, record.attempt, note)
                return {"id": record.id, "status": "dead", "error": note}
            else:
                record.status = "failed"
                record.next_retry_at = now + timedelta(seconds=delay)
                record.locked_by = None
                record.locked_at = None
                record.updated_at = now
                await audit_single(session, record.id, action="retry", status_before=status_before,
                                   status_after=record.status, attempt=record.attempt, note=note)
                await session.commit()
                LOGGER.warning("Retry scheduled id=%s attempt=%d next_retry_at=%s reason=%s",
                               record.id, record.attempt, record.next_retry_at, note)
                return {"id": record.id, "status": "retry", "next_retry_at": record.next_retry_at.isoformat(), "error": note}


async def process_locked_ids(locked_ids: List[str]) -> Dict[str, Any]:
    sem = asyncio.Semaphore(WORKER_CONCURRENCY)
    results: List[Dict[str, Any]] = []

    async def _worker(rid: str):
        async with sem:
            res = await handle_record(rid)
            results.append(res)

    await asyncio.gather(*[_worker(rid) for rid in locked_ids])

    # Aggregate metrics
    forwarded = [r for r in results if r.get("status") == "forwarded"]
    retries = [r for r in results if r.get("status") == "retry"]
    deads = [r for r in results if r.get("status") == "dead"]
    latencies = [r.get("latency") for r in forwarded if r.get("latency") is not None]
    avg_latency = sum(latencies) / len(latencies) if latencies else None
    return {
        "forwarded_count": len(forwarded),
        "retry_count": len(retries),
        "dead_count": len(deads),
        "avg_latency": avg_latency,
        "latencies": latencies,
        "details": results,
    }


async def run_loop(exit_on_idle: bool = False, max_iterations: Optional[int] = None):
    await init_db()
    LOGGER.info("Outbox forwarder started id=%s batch=%d interval=%ss concurrency=%d", FORWARDER_ID, BATCH_SIZE, SCAN_INTERVAL_SECONDS, WORKER_CONCURRENCY)
    iteration = 0
    last_cleanup = datetime.utcnow()

    while True:
        iteration += 1
        locked_ids = await lock_batch(BATCH_SIZE)
        if not locked_ids:
            LOGGER.debug("No candidates to lock")
            if exit_on_idle:
                if not await pending_exists():
                    LOGGER.info("Exit on idle: no work left")
                    break
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)
        else:
            LOGGER.info("Locked %d records", len(locked_ids))
            metrics = await process_locked_ids(locked_ids)
            LOGGER.info("Processed batch: forwarded=%d retry=%d dead=%d avg_latency=%s", metrics["forwarded_count"], metrics["retry_count"], metrics["dead_count"], metrics["avg_latency"])

        # periodic stale lock recovery
        if (datetime.utcnow() - last_cleanup).total_seconds() >= CLEANUP_INTERVAL_SECONDS:
            await recover_stale_locks()
            last_cleanup = datetime.utcnow()

        if max_iterations and iteration >= max_iterations:
            LOGGER.info("Reached max iterations=%d", max_iterations)
            break


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Async outbox forwarder")
    parser.add_argument("command", nargs="?", default="forward", choices=["forward", "run", "replay-dlq"], help="Command to run")
    parser.add_argument("--exit-on-idle", action="store_true", help="Exit when no work remains")
    parser.add_argument("--max-iterations", type=int, default=None, help="Max loop iterations before exit")
    parser.add_argument("--ids", type=str, default=None, help="Comma-separated DLQ IDs to replay")
    parser.add_argument("--limit", type=int, default=None, help="Max DLQ items to replay")
    args = parser.parse_args()

    if args.command in ("forward", "run"):
        asyncio.run(run_loop(exit_on_idle=args.exit_on_idle, max_iterations=args.max_iterations))
    elif args.command == "replay-dlq":
        ids = [i.strip() for i in args.ids.split(",")] if args.ids else None
        asyncio.run(replay_dlq(ids=ids, limit=args.limit))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        LOGGER.info("Forwarder stopped by user")
