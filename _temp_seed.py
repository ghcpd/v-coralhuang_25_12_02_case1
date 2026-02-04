import asyncio
import uuid
from datetime import datetime
from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent

async def seed_data(num_records):
    await init_db()
    async with SessionLocal() as session:
        # Clear existing data for clean test
        from sqlalchemy import delete
        await session.execute(delete(OutboxEvent))
        await session.commit()
        
        for i in range(num_records):
            record = OutboxEvent(
                id=str(uuid.uuid4()),
                topic='test_topic_' + str(i % 10),
                payload='{"index": ' + str(i) + ', "timestamp": "' + datetime.utcnow().isoformat() + '"}',
                status="new",
                attempt=0
            )
            session.add(record)
            if (i + 1) % 100 == 0:
                await session.commit()
        
        await session.commit()
        
        # Verify count
        from sqlalchemy import select
        stmt = select(OutboxEvent)
        count = len((await session.execute(stmt)).scalars().all())
        print('Seeded ' + str(count) + ' records')

asyncio.run(seed_data(500))
