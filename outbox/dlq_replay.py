"""DLQ Replay Utility - Move dead-lettered records back to outbox for reprocessing."""
from __future__ import annotations
import asyncio
import logging
from datetime import datetime
from typing import Optional
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, OutboxDLQ, OutboxAudit
import uuid

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("dlq_replay")

async def audit_log(session: AsyncSession, outbox_id: str, action: str, details: str = None):
    """Log an audit trail entry."""
    audit = OutboxAudit(
        id=str(uuid.uuid4()),
        outbox_id=outbox_id,
        action=action,
        details=details
    )
    session.add(audit)

async def replay_all_dlq(session: AsyncSession) -> int:
    """Replay all DLQ records back to outbox with attempt=0."""
    stmt = select(OutboxDLQ)
    dlq_records = (await session.execute(stmt)).scalars().all()
    
    count = 0
    for dlq_record in dlq_records:
        # Check if already in outbox
        existing = await session.get(OutboxEvent, dlq_record.id)
        if existing:
            LOGGER.warning("Record already in outbox id=%s, skipping DLQ replay", dlq_record.id)
            continue
        
        # Create new outbox record with attempt=0
        outbox_record = OutboxEvent(
            id=dlq_record.id,
            topic=dlq_record.topic,
            payload=dlq_record.payload,
            status="new",
            attempt=0,
            next_retry_at=None,
            locked_at=None,
            created_at=dlq_record.created_at,
            updated_at=datetime.utcnow()
        )
        session.add(outbox_record)
        
        await audit_log(session, dlq_record.id, "replay", 
                       f"replayed from DLQ: {dlq_record.reason}")
        
        count += 1
        LOGGER.info("Replayed DLQ record id=%s", dlq_record.id)
    
    if count > 0:
        # Delete replayed records from DLQ
        await session.execute(delete(OutboxDLQ).where(
            OutboxDLQ.id.in_([r.id for r in dlq_records])
        ))
        await session.commit()
        LOGGER.info("Replayed %d records from DLQ", count)
    else:
        LOGGER.info("No DLQ records to replay")
    
    return count

async def replay_dlq_by_id(session: AsyncSession, record_id: str) -> bool:
    """Replay a specific DLQ record by ID."""
    dlq_record = await session.get(OutboxDLQ, record_id)
    
    if not dlq_record:
        LOGGER.error("DLQ record not found id=%s", record_id)
        return False
    
    # Check if already in outbox
    existing = await session.get(OutboxEvent, record_id)
    if existing:
        LOGGER.error("Record already in outbox id=%s", record_id)
        return False
    
    # Create new outbox record
    outbox_record = OutboxEvent(
        id=dlq_record.id,
        topic=dlq_record.topic,
        payload=dlq_record.payload,
        status="new",
        attempt=0,
        next_retry_at=None,
        locked_at=None,
        created_at=dlq_record.created_at,
        updated_at=datetime.utcnow()
    )
    session.add(outbox_record)
    
    await audit_log(session, record_id, "replay", 
                   f"single replay from DLQ: {dlq_record.reason}")
    
    # Delete from DLQ
    await session.delete(dlq_record)
    await session.commit()
    
    LOGGER.info("Replayed DLQ record id=%s", record_id)
    return True

async def list_dlq(session: AsyncSession) -> int:
    """List all DLQ records and return count."""
    stmt = select(OutboxDLQ)
    dlq_records = (await session.execute(stmt)).scalars().all()
    
    if not dlq_records:
        LOGGER.info("DLQ is empty")
        return 0
    
    LOGGER.info("=== DLQ Records ===")
    for record in dlq_records:
        LOGGER.info("  ID: %s, Topic: %s, Attempt: %d, Reason: %s", 
                   record.id, record.topic, record.attempt, record.reason)
    
    LOGGER.info("Total DLQ records: %d", len(dlq_records))
    return len(dlq_records)

async def main():
    """CLI interface for DLQ replay."""
    import sys
    
    await init_db()
    
    command = sys.argv[1] if len(sys.argv) > 1 else "list"
    
    async with SessionLocal() as session:
        if command == "replay-all":
            await replay_all_dlq(session)
        elif command == "replay":
            if len(sys.argv) < 3:
                LOGGER.error("Usage: python -m outbox.dlq_replay replay <record_id>")
                sys.exit(1)
            record_id = sys.argv[2]
            await replay_dlq_by_id(session, record_id)
        elif command == "list":
            await list_dlq(session)
        else:
            LOGGER.error("Unknown command: %s", command)
            LOGGER.info("Available commands: replay-all, replay <id>, list")
            sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
