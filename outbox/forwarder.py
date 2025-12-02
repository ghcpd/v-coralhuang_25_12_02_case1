from __future__ import annotations
import asyncio
from datetime import datetime, timedelta
import logging
import random
import uuid
from typing import List, Optional
from sqlalchemy import select, update, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, Event, OutboxDLQ, OutboxAudit

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
LOGGER = logging.getLogger("outbox_forwarder")

# Configuration
BATCH_SIZE = 100
SCAN_INTERVAL_SECONDS = 0.5
MAX_ATTEMPTS = 5
CONCURRENCY_WORKERS = 10
LOCK_TIMEOUT_SECONDS = 30
STALE_LOCK_CHECK_INTERVAL = 10
BASE_RETRY_DELAY_SECONDS = 2
MAX_RETRY_DELAY_SECONDS = 300
JITTER_FACTOR = 0.3

# Metrics
class ForwarderMetrics:
    def __init__(self):
        self.forwarded_count = 0
        self.failed_count = 0
        self.dlq_count = 0
        self.retry_count = 0
        self.lock_count = 0
        self.total_latency_ms = 0.0
        self.processed_count = 0
    
    def log_summary(self):
        avg_latency = self.total_latency_ms / self.processed_count if self.processed_count > 0 else 0
        LOGGER.info(
            "Metrics: forwarded=%d, retried=%d, dlq=%d, failed=%d, avg_latency=%.2fms",
            self.forwarded_count, self.retry_count, self.dlq_count, self.failed_count, avg_latency
        )

METRICS = ForwarderMetrics()

# Instance identifier for this forwarder
INSTANCE_ID = str(uuid.uuid4())[:8]

def compute_next_retry(attempt: int) -> datetime:
    """Exponential backoff with jitter"""
    delay = min(BASE_RETRY_DELAY_SECONDS * (2 ** attempt), MAX_RETRY_DELAY_SECONDS)
    jitter = delay * JITTER_FACTOR * (random.random() * 2 - 1)
    total_delay = max(0, delay + jitter)
    return datetime.utcnow() + timedelta(seconds=total_delay)

async def create_audit_entry(
    session: AsyncSession, 
    outbox_id: str, 
    event_type: str, 
    status_from: Optional[str], 
    status_to: Optional[str],
    attempt: int,
    details: Optional[str] = None
):
    """Create minimal audit trail entry"""
    audit = OutboxAudit(
        outbox_id=outbox_id,
        event_type=event_type,
        status_from=status_from,
        status_to=status_to,
        attempt=attempt,
        details=details
    )
    session.add(audit)

async def fetch_and_lock_batch(session: AsyncSession) -> List[str]:
    """Atomically fetch and lock a batch of records"""
    now = datetime.utcnow()
    
    # Find eligible records: new or failed with retry time passed
    stmt = select(OutboxEvent.id).where(
        and_(
            or_(OutboxEvent.status == "new", OutboxEvent.status == "failed"),
            or_(OutboxEvent.next_retry_at.is_(None), OutboxEvent.next_retry_at <= now)
        )
    ).limit(BATCH_SIZE).with_for_update(skip_locked=True)
    
    result = await session.execute(stmt)
    ids = [row[0] for row in result.fetchall()]
    
    if not ids:
        return []
    
    # Atomic lock update
    update_stmt = (
        update(OutboxEvent)
        .where(OutboxEvent.id.in_(ids))
        .values(
            status="locked",
            locked_at=now,
            locked_by=INSTANCE_ID,
            updated_at=now
        )
    )
    await session.execute(update_stmt)
    await session.commit()
    
    METRICS.lock_count += len(ids)
    LOGGER.debug(f"Locked {len(ids)} records")
    
    return ids

async def forward_record(session: AsyncSession, record: OutboxEvent) -> bool:
    """Forward a single record. Returns True on success, False on failure."""
    start_time = datetime.utcnow()
    
    try:
        # Check if already forwarded (idempotent)
        existed = await session.get(Event, record.id)
        if existed:
            record.status = "forwarded"
            record.updated_at = datetime.utcnow()
            await create_audit_entry(session, record.id, "forwarded", "locked", "forwarded", record.attempt, "duplicate_skip")
            return True
        
        # Create event
        evt = Event(id=record.id, topic=record.topic, payload=record.payload)
        session.add(evt)
        
        record.status = "forwarded"
        record.updated_at = datetime.utcnow()
        record.next_retry_at = None
        
        await create_audit_entry(session, record.id, "forwarded", "locked", "forwarded", record.attempt)
        
        end_time = datetime.utcnow()
        latency_ms = (end_time - start_time).total_seconds() * 1000
        METRICS.total_latency_ms += latency_ms
        METRICS.processed_count += 1
        METRICS.forwarded_count += 1
        
        return True
        
    except Exception as e:
        LOGGER.error(f"Forward failed id={record.id} error={e}", exc_info=False)
        return False

async def handle_failure(session: AsyncSession, record: OutboxEvent, error: str):
    """Handle failure: retry with backoff or move to DLQ"""
    record.attempt += 1
    record.updated_at = datetime.utcnow()
    
    if record.attempt >= MAX_ATTEMPTS:
        # Move to DLQ
        dlq = OutboxDLQ(
            id=str(uuid.uuid4()),
            original_id=record.id,
            topic=record.topic,
            payload=record.payload,
            failure_reason=str(error)[:500],
            attempts=record.attempt
        )
        session.add(dlq)
        record.status = "failed"
        record.next_retry_at = None
        
        await create_audit_entry(session, record.id, "dead", "locked", "failed", record.attempt, f"max_attempts:{error[:200]}")
        
        METRICS.dlq_count += 1
        LOGGER.warning(f"Moved to DLQ: id={record.id} attempts={record.attempt}")
    else:
        # Schedule retry with exponential backoff
        record.status = "failed"
        record.next_retry_at = compute_next_retry(record.attempt)
        
        await create_audit_entry(session, record.id, "retry", "locked", "failed", record.attempt, f"retry_at:{record.next_retry_at}")
        
        METRICS.retry_count += 1
        LOGGER.info(f"Scheduled retry: id={record.id} attempt={record.attempt} next_retry={record.next_retry_at}")

async def process_record_worker(session: AsyncSession, record_id: str):
    """Process a single locked record"""
    try:
        # Fetch the locked record
        record = await session.get(OutboxEvent, record_id)
        if not record or record.status != "locked":
            LOGGER.warning(f"Record {record_id} not locked, skipping")
            return
        
        success = await forward_record(session, record)
        
        if not success:
            await handle_failure(session, record, "forward_exception")
        
        await session.commit()
        
    except Exception as e:
        LOGGER.error(f"Worker error for {record_id}: {e}", exc_info=True)
        await session.rollback()
        METRICS.failed_count += 1

async def process_batch_concurrent(locked_ids: List[str]):
    """Process locked records with bounded concurrency"""
    semaphore = asyncio.Semaphore(CONCURRENCY_WORKERS)
    
    async def worker(record_id: str):
        async with semaphore:
            async with SessionLocal() as session:
                await process_record_worker(session, record_id)
    
    tasks = [worker(record_id) for record_id in locked_ids]
    await asyncio.gather(*tasks, return_exceptions=True)

async def recover_stale_locks(session: AsyncSession):
    """Recover records with stale locks"""
    cutoff = datetime.utcnow() - timedelta(seconds=LOCK_TIMEOUT_SECONDS)
    
    stmt = (
        update(OutboxEvent)
        .where(
            and_(
                OutboxEvent.status == "locked",
                OutboxEvent.locked_at < cutoff
            )
        )
        .values(
            status="new",
            locked_at=None,
            locked_by=None,
            updated_at=datetime.utcnow()
        )
    )
    
    result = await session.execute(stmt)
    await session.commit()
    
    if result.rowcount > 0:
        LOGGER.warning(f"Recovered {result.rowcount} stale locks")

async def run_loop():
    await init_db()
    LOGGER.info(f"Outbox forwarder started (instance={INSTANCE_ID}, batch={BATCH_SIZE}, workers={CONCURRENCY_WORKERS})")
    
    last_stale_check = datetime.utcnow()
    
    while True:
        try:
            # Periodically check for stale locks
            if (datetime.utcnow() - last_stale_check).total_seconds() >= STALE_LOCK_CHECK_INTERVAL:
                async with SessionLocal() as session:
                    await recover_stale_locks(session)
                last_stale_check = datetime.utcnow()
            
            # Fetch and lock batch
            async with SessionLocal() as session:
                locked_ids = await fetch_and_lock_batch(session)
            
            if locked_ids:
                LOGGER.info(f"Processing batch of {len(locked_ids)} records")
                await process_batch_concurrent(locked_ids)
                METRICS.log_summary()
            else:
                LOGGER.debug("No eligible records")
            
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)
            
        except Exception as e:
            LOGGER.error(f"Error in main loop: {e}", exc_info=True)
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    try:
        asyncio.run(run_loop())
    except KeyboardInterrupt:
        LOGGER.info("Forwarder stopped by user")
        METRICS.log_summary()
