import asyncio
import time
from datetime import datetime
from sqlalchemy import select
from outbox.database import SessionLocal, init_db
from outbox.models import OutboxEvent
from outbox.forwarder import fetch_batch_and_lock, process_batch_concurrent, recover_stale_locks, log_metrics, SCAN_INTERVAL_SECONDS

start_time = time.time()

async def test_run():
    await init_db()
    
    for iteration in range(120):  # Run for up to 60 seconds
        async with SessionLocal() as session:
            # Recover stale locks
            await recover_stale_locks(session)
            
            # Fetch and process batch
            batch = await fetch_batch_and_lock(session)
            if batch:
                print('[' + str(iteration) + '] Processing ' + str(len(batch)) + ' records')
                await process_batch_concurrent(session, batch)
            else:
                # Check if still pending
                stmt = select(OutboxEvent).where(OutboxEvent.status.in_(["new", "locked", "failed"]))
                pending = len((await session.execute(stmt)).scalars().all())
                if pending == 0:
                    print('All done! Elapsed: ' + str(round(time.time() - start_time, 1)) + 's')
                    break
            
            await log_metrics(session)
        
        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

asyncio.run(test_run())
