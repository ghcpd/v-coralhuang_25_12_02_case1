"""
Quick status check - outputs simple numbers for scripting
"""
import asyncio
import sys
from sqlalchemy import select, func
from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent

async def check_status():
    try:
        await init_db()
        async with SessionLocal() as session:
            stmt = select(func.count(OutboxEvent.id)).where(OutboxEvent.status.in_(["new", "failed"]))
            pending = (await session.execute(stmt)).scalar()
            
            stmt = select(func.count(OutboxEvent.id)).where(OutboxEvent.status == "locked")
            locked = (await session.execute(stmt)).scalar()
            
            stmt = select(func.count(OutboxEvent.id)).where(OutboxEvent.status == "forwarded")
            forwarded = (await session.execute(stmt)).scalar()
            
            # Output in simple format: pending,locked,forwarded
            print(f"{pending},{locked},{forwarded}")
            return 0
    except Exception as e:
        print(f"0,0,0", file=sys.stderr)
        return 1

if __name__ == "__main__":
    sys.exit(asyncio.run(check_status()))
