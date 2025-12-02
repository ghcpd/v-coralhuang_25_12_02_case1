from __future__ import annotations
import asyncio
from datetime import datetime, timedelta
import logging
import uuid
import random
from typing import List
from sqlalchemy import select, update, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, Event, OutboxDLQ, OutboxAudit

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("outbox_forwarder")

# Configuration
BATCH_SIZE = 100
SCAN_INTERVAL_SECONDS = 1
MAX_ATTEMPTS = 3
MAX_WORKERS = 5
LOCK_TIMEOUT_SECONDS = 30
STALE_LOCK_CHECK_INTERVAL = 10  # seconds

# Exponential backoff parameters
BASE_RETRY_DELAY = 2  # seconds
MAX_RETRY_DELAY = 300  # 5 minutes
JITTER_FACTOR = 0.1

def calculate_next_retry_at(attempt: int) -> datetime:
    """Calculate next retry time with exponential backoff + jitter."""
    delay = min(BASE_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
    jitter = delay * JITTER_FACTOR * random.random()
    total_delay = delay + jitter
    return datetime.utcnow() + timedelta(seconds=total_delay)

async def audit_log(session: AsyncSession, outbox_id: str, action: str, details: str = None):
    """Log an audit trail entry."""
    audit = OutboxAudit(
        id=str(uuid.uuid4()),
        outbox_id=outbox_id,
        action=action,
        details=details
    )
    session.add(audit)

async def fetch_batch_and_lock(session: AsyncSession) -> List[OutboxEvent]:
    """Fetch and atomically lock a batch of ready records."""
    # Find records that are ready to process
    stmt = select(OutboxEvent).where(
        and_(
            OutboxEvent.status.in_(["new", "failed"]),
            or_(
                OutboxEvent.next_retry_at.is_(None),
                OutboxEvent.next_retry_at <= datetime.utcnow()
            )
        )
    ).limit(BATCH_SIZE)
    
    rows = (await session.execute(stmt)).scalars().all()
    
    if not rows:
        return []
    
    # Atomically update status to 'locked' with timestamp
    ids = [r.id for r in rows]
    locked_at = datetime.utcnow()
    
    stmt = update(OutboxEvent).where(
        OutboxEvent.id.in_(ids)
    ).values(status="locked", locked_at=locked_at, updated_at=locked_at)
    
    await session.execute(stmt)
    await session.commit()
    
    # Re-fetch to get updated records
    stmt = select(OutboxEvent).where(OutboxEvent.id.in_(ids))
    locked_rows = (await session.execute(stmt)).scalars().all()
    
    for row in locked_rows:
        await audit_log(session, row.id, "lock", f"attempt={row.attempt}")
    
    await session.commit()
    return locked_rows

async def forward_record(session: AsyncSession, record: OutboxEvent) -> bool:
    """Forward a record to the events table. Returns True if successful."""
    try:
        # Check if already forwarded (idempotent)
        existed = await session.get(Event, record.id)
        if existed:
            record.status = "forwarded"
            record.updated_at = datetime.utcnow()
            record.locked_at = None
            await audit_log(session, record.id, "forward", "already_existed")
            return True
        
        # Create event
        evt = Event(id=record.id, topic=record.topic, payload=record.payload)
        session.add(evt)
        
        record.status = "forwarded"
        record.updated_at = datetime.utcnow()
        record.locked_at = None
        
        await audit_log(session, record.id, "forward", "success")
        return True
        
    except Exception as e:
        LOGGER.error("Forward failed id=%s error=%s", record.id, e)
        record.attempt += 1
        
        if record.attempt >= MAX_ATTEMPTS:
            # Move to DLQ
            dlq_entry = OutboxDLQ(
                id=record.id,
                topic=record.topic,
                payload=record.payload,
                reason=f"Max attempts ({MAX_ATTEMPTS}) reached: {str(e)}",
                attempt=record.attempt
            )
            session.add(dlq_entry)
            
            record.status = "failed"
            record.locked_at = None
            
            await audit_log(session, record.id, "dead", f"max_attempts_exceeded: {str(e)}")
            LOGGER.warning("Record moved to DLQ id=%s", record.id)
        else:
            # Schedule retry with exponential backoff
            record.next_retry_at = calculate_next_retry_at(record.attempt)
            record.status = "failed"
            record.locked_at = None
            
            await audit_log(session, record.id, "retry", 
                          f"attempt={record.attempt}, next_retry_at={record.next_retry_at}")
            LOGGER.info("Record scheduled for retry id=%s attempt=%d next_retry=%s", 
                       record.id, record.attempt, record.next_retry_at)
        
        record.updated_at = datetime.utcnow()
        return False

async def process_record_worker(session: AsyncSession, record: OutboxEvent):
    """Worker coroutine to process a single record."""
    try:
        await forward_record(session, record)
    except Exception as e:
        LOGGER.error("Worker exception for id=%s: %s", getattr(record, 'id', '?'), e, exc_info=False)

async def process_batch_concurrent(session: AsyncSession, batch: List[OutboxEvent]):
    """Process batch with concurrency (using serial for SQLite stability)."""
    if not batch:
        return
    
    LOGGER.info("Processing %d records", len(batch))
    
    # For SQLite with aiosqlite, serial processing is more stable than true concurrency
    # Use semaphore to limit parallelism safely
    for record in batch:
        try:
            await forward_record(session, record)
        except Exception as e:
            LOGGER.error("Worker exception for id=%s: %s", getattr(record, 'id', '?'), e, exc_info=False)
    
    await session.commit()

async def recover_stale_locks(session: AsyncSession):
    """Reset locks older than LOCK_TIMEOUT_SECONDS back to 'new'."""
    stale_threshold = datetime.utcnow() - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
    
    stmt = select(OutboxEvent).where(
        and_(
            OutboxEvent.status == "locked",
            OutboxEvent.locked_at < stale_threshold
        )
    )
    
    stale_records = (await session.execute(stmt)).scalars().all()
    
    if stale_records:
        LOGGER.warning("Found %d stale locks, recovering...", len(stale_records))
        
        for record in stale_records:
            record.status = "new"
            record.locked_at = None
            record.updated_at = datetime.utcnow()
            await audit_log(session, record.id, "unlock", "stale_lock_recovered")
        
        await session.commit()

async def log_metrics(session: AsyncSession):
    """Log current metrics."""
    stmt_new = select(OutboxEvent).where(OutboxEvent.status == "new")
    stmt_locked = select(OutboxEvent).where(OutboxEvent.status == "locked")
    stmt_forwarded = select(OutboxEvent).where(OutboxEvent.status == "forwarded")
    stmt_failed = select(OutboxEvent).where(OutboxEvent.status == "failed")
    
    new_count = len((await session.execute(stmt_new)).scalars().all())
    locked_count = len((await session.execute(stmt_locked)).scalars().all())
    forwarded_count = len((await session.execute(stmt_forwarded)).scalars().all())
    failed_count = len((await session.execute(stmt_failed)).scalars().all())
    
    stmt_dlq = select(OutboxDLQ)
    dlq_count = len((await session.execute(stmt_dlq)).scalars().all())
    
    LOGGER.info("METRICS: new=%d locked=%d forwarded=%d failed=%d dlq=%d", 
               new_count, locked_count, forwarded_count, failed_count, dlq_count)

async def run_loop():
    """Main forwarder loop."""
    await init_db()
    LOGGER.info("Outbox forwarder started (batch=%d workers=%d interval=%ds lock_timeout=%ds)",
               BATCH_SIZE, MAX_WORKERS, SCAN_INTERVAL_SECONDS, LOCK_TIMEOUT_SECONDS)
    
    last_stale_lock_check = datetime.utcnow()
    
    while True:
        try:
            async with SessionLocal() as session:
                # Recover stale locks periodically
                now = datetime.utcnow()
                if (now - last_stale_lock_check).total_seconds() >= STALE_LOCK_CHECK_INTERVAL:
                    await recover_stale_locks(session)
                    last_stale_lock_check = now
                
                # Fetch and lock batch
                batch = await fetch_batch_and_lock(session)
                
                if batch:
                    LOGGER.info("Fetched and locked %d outbox records", len(batch))
                    await process_batch_concurrent(session, batch)
                else:
                    LOGGER.debug("No records ready for processing")
                
                # Log metrics
                await log_metrics(session)
                
        except Exception as e:
            LOGGER.error("Loop exception: %s", e, exc_info=True)
        
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        LOGGER.info("Forwarder stopped by user")

