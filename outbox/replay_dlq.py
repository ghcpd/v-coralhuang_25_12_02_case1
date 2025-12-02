"""
DLQ Replay Utility - Requeue failed messages from Dead Letter Queue back to outbox
"""
from __future__ import annotations
import asyncio
import logging
from datetime import datetime
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, OutboxDLQ, OutboxAudit

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
LOGGER = logging.getLogger("dlq_replay")

async def replay_dlq_messages(limit: int = None, original_ids: list[str] = None):
    """
    Replay messages from DLQ back to outbox.
    
    Args:
        limit: Maximum number of messages to replay (None = all)
        original_ids: Specific message IDs to replay (None = all)
    """
    await init_db()
    
    async with SessionLocal() as session:
        # Build query
        stmt = select(OutboxDLQ)
        
        if original_ids:
            stmt = stmt.where(OutboxDLQ.original_id.in_(original_ids))
        
        if limit:
            stmt = stmt.limit(limit)
        
        result = await session.execute(stmt)
        dlq_records = result.scalars().all()
        
        if not dlq_records:
            LOGGER.info("No DLQ records found to replay")
            return
        
        LOGGER.info(f"Found {len(dlq_records)} DLQ records to replay")
        
        replayed_count = 0
        for dlq in dlq_records:
            try:
                # Check if original outbox record still exists
                existing = await session.get(OutboxEvent, dlq.original_id)
                
                if existing:
                    # Reset existing record
                    existing.status = "new"
                    existing.attempt = 0
                    existing.next_retry_at = None
                    existing.locked_at = None
                    existing.locked_by = None
                    existing.updated_at = datetime.utcnow()
                    
                    LOGGER.info(f"Reset existing outbox record: {dlq.original_id}")
                else:
                    # Recreate outbox record
                    new_outbox = OutboxEvent(
                        id=dlq.original_id,
                        topic=dlq.topic,
                        payload=dlq.payload,
                        status="new",
                        attempt=0,
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow()
                    )
                    session.add(new_outbox)
                    LOGGER.info(f"Recreated outbox record: {dlq.original_id}")
                
                # Create audit entry
                audit = OutboxAudit(
                    outbox_id=dlq.original_id,
                    event_type="replay",
                    status_from="dead",
                    status_to="new",
                    attempt=0,
                    details=f"replayed_from_dlq:{dlq.id}"
                )
                session.add(audit)
                
                # Remove from DLQ
                await session.delete(dlq)
                
                replayed_count += 1
                
            except Exception as e:
                LOGGER.error(f"Failed to replay DLQ record {dlq.id}: {e}", exc_info=True)
                await session.rollback()
                continue
        
        await session.commit()
        LOGGER.info(f"Successfully replayed {replayed_count}/{len(dlq_records)} messages from DLQ")

async def list_dlq_messages():
    """List all messages in DLQ"""
    await init_db()
    
    async with SessionLocal() as session:
        stmt = select(OutboxDLQ)
        result = await session.execute(stmt)
        dlq_records = result.scalars().all()
        
        if not dlq_records:
            LOGGER.info("DLQ is empty")
            return
        
        LOGGER.info(f"DLQ contains {len(dlq_records)} messages:")
        for dlq in dlq_records:
            LOGGER.info(
                f"  - ID: {dlq.id}, Original: {dlq.original_id}, Topic: {dlq.topic}, "
                f"Attempts: {dlq.attempts}, Reason: {dlq.failure_reason[:50] if dlq.failure_reason else 'N/A'}"
            )

async def clear_dlq():
    """Clear all messages from DLQ (dangerous!)"""
    await init_db()
    
    async with SessionLocal() as session:
        stmt = delete(OutboxDLQ)
        result = await session.execute(stmt)
        await session.commit()
        LOGGER.warning(f"Cleared {result.rowcount} messages from DLQ")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m outbox.replay_dlq list           # List all DLQ messages")
        print("  python -m outbox.replay_dlq replay [N]     # Replay N messages (or all)")
        print("  python -m outbox.replay_dlq clear          # Clear DLQ (dangerous!)")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "list":
        asyncio.run(list_dlq_messages())
    elif command == "replay":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else None
        asyncio.run(replay_dlq_messages(limit=limit))
    elif command == "clear":
        asyncio.run(clear_dlq())
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
