# -*- coding: utf-8 -*-
import asyncio
from sqlalchemy import select
from outbox.database import SessionLocal
from outbox.models import OutboxEvent, OutboxDLQ, Event

async def get_metrics():
    async with SessionLocal() as session:
        # Count by status
        new = len((await session.execute(select(OutboxEvent).where(OutboxEvent.status == "new"))).scalars().all())
        locked = len((await session.execute(select(OutboxEvent).where(OutboxEvent.status == "locked"))).scalars().all())
        forwarded = len((await session.execute(select(OutboxEvent).where(OutboxEvent.status == "forwarded"))).scalars().all())
        failed = len((await session.execute(select(OutboxEvent).where(OutboxEvent.status == "failed"))).scalars().all())
        dlq = len((await session.execute(select(OutboxDLQ))).scalars().all())
        events = len((await session.execute(select(Event))).scalars().all())
        
        print("")
        print("=== OUTBOX FORWARDER TEST SUMMARY ===")
        print("  New:         " + str(new).rjust(6))
        print("  Locked:      " + str(locked).rjust(6))
        print("  Forwarded:   " + str(forwarded).rjust(6))
        print("  Failed:      " + str(failed).rjust(6))
        print("  DLQ:         " + str(dlq).rjust(6))
        print("  Events:      " + str(events).rjust(6) + " (in events table)")
        print("-------------------------------------")
        
        total_processed = forwarded + failed + dlq
        total_seeded = new + locked + forwarded + failed + dlq
        
        print("  Total seeded: " + str(total_seeded).rjust(6))
        pct = 100.0*total_processed/max(1,total_seeded)
        print("  Total processed: " + str(total_processed).rjust(6) + " (" + str(round(pct, 1)) + "%)")
        print("=====================================")
        
        if new == 0 and locked == 0 and (forwarded + dlq) == total_seeded:
            msg = "[SUCCESS] All records processed!"
            if dlq > 0:
                msg = msg + " (DLQ count: " + str(dlq) + ")"
            print(msg)
        else:
            print("[INCOMPLETE] Some records still pending")

asyncio.run(get_metrics())
