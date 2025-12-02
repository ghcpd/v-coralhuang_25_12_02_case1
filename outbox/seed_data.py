"""
Seed test data into the outbox table for testing
"""
from __future__ import annotations
import asyncio
import json
import logging
import uuid
from datetime import datetime
from sqlalchemy import select

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
LOGGER = logging.getLogger("seed_data")

TOPICS = ["user.created", "user.updated", "order.placed", "order.shipped", "payment.processed"]

async def seed_outbox(count: int = 1000):
    """Seed the outbox with test data"""
    await init_db()
    
    LOGGER.info(f"Seeding {count} outbox records...")
    
    async with SessionLocal() as session:
        # Check if data already exists
        stmt = select(OutboxEvent).limit(1)
        result = await session.execute(stmt)
        if result.scalar_one_or_none():
            LOGGER.warning("Outbox already contains data. Clear it first if you want fresh data.")
            return
        
        records = []
        for i in range(count):
            topic = TOPICS[i % len(TOPICS)]
            payload_data = {
                "id": str(uuid.uuid4()),
                "timestamp": datetime.utcnow().isoformat(),
                "data": f"Test event {i}",
                "sequence": i
            }
            
            record = OutboxEvent(
                id=str(uuid.uuid4()),
                topic=topic,
                payload=json.dumps(payload_data),
                status="new",
                attempt=0,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            records.append(record)
            
            # Batch insert every 500 records
            if len(records) >= 500:
                session.add_all(records)
                await session.commit()
                LOGGER.info(f"Inserted {i+1}/{count} records...")
                records = []
        
        # Insert remaining records
        if records:
            session.add_all(records)
            await session.commit()
        
        LOGGER.info(f"Successfully seeded {count} outbox records")

async def clear_all_data():
    """Clear all data from all tables"""
    await init_db()
    
    async with SessionLocal() as session:
        # Clear in order to avoid foreign key issues
        from outbox.models import Base
        from sqlalchemy import delete
        from outbox.models import OutboxAudit, OutboxDLQ, Event, OutboxEvent
        
        await session.execute(delete(OutboxAudit))
        await session.execute(delete(OutboxDLQ))
        await session.execute(delete(Event))
        await session.execute(delete(OutboxEvent))
        await session.commit()
        
        LOGGER.info("All data cleared")

async def get_stats():
    """Get statistics about current data"""
    await init_db()
    
    async with SessionLocal() as session:
        from sqlalchemy import func
        from outbox.models import OutboxEvent, Event, OutboxDLQ, OutboxAudit
        
        # Outbox stats by status
        stmt = select(OutboxEvent.status, func.count(OutboxEvent.id)).group_by(OutboxEvent.status)
        result = await session.execute(stmt)
        outbox_stats = dict(result.fetchall())
        
        # Event count
        stmt = select(func.count(Event.id))
        result = await session.execute(stmt)
        event_count = result.scalar()
        
        # DLQ count
        stmt = select(func.count(OutboxDLQ.id))
        result = await session.execute(stmt)
        dlq_count = result.scalar()
        
        # Audit count
        stmt = select(func.count(OutboxAudit.id))
        result = await session.execute(stmt)
        audit_count = result.scalar()
        
        LOGGER.info("=== Database Statistics ===")
        LOGGER.info(f"Outbox by status: {outbox_stats}")
        LOGGER.info(f"Events forwarded: {event_count}")
        LOGGER.info(f"DLQ entries: {dlq_count}")
        LOGGER.info(f"Audit entries: {audit_count}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m outbox.seed_data seed [N]   # Seed N records (default 1000)")
        print("  python -m outbox.seed_data clear      # Clear all data")
        print("  python -m outbox.seed_data stats      # Show statistics")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "seed":
        count = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
        asyncio.run(seed_outbox(count))
    elif command == "clear":
        asyncio.run(clear_all_data())
    elif command == "stats":
        asyncio.run(get_stats())
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
