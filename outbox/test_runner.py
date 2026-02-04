from __future__ import annotations
import asyncio
import time
import uuid
import random
from datetime import datetime
from typing import Optional
from sqlalchemy import delete, select, func

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, OutboxDLQ, Event
from outbox.forwarder import run_until_idle, SCAN_INTERVAL_SECONDS

async def seed_outbox(n: int = 1000, fail_ratio: float = 0.05, clear: bool = True):
    await init_db()
    async with SessionLocal() as session:
        if clear:
            await session.execute(delete(Event))
            await session.execute(delete(OutboxEvent))
            await session.execute(delete(OutboxDLQ))
        now = datetime.utcnow()
        for i in range(n):
            oid = str(uuid.uuid4())
            topic = "fail" if random.random() < fail_ratio else "demo"
            payload = f"payload-{i}"
            session.add(
                OutboxEvent(
                    id=oid,
                    topic=topic,
                    payload=payload,
                    status="new",
                    attempt=0,
                    created_at=now,
                    updated_at=now,
                )
            )
            if i % 500 == 0:
                await session.flush()
        await session.commit()

async def summarize():
    async with SessionLocal() as session:
        forwarded = await session.scalar(select(func.count()).select_from(Event))
        dead = await session.scalar(select(func.count()).select_from(OutboxDLQ))
        failed = await session.scalar(
            select(func.count()).select_from(OutboxEvent).where(OutboxEvent.status == "failed")
        )
        in_progress = await session.scalar(
            select(func.count()).select_from(OutboxEvent).where(OutboxEvent.status == "locked")
        )
        total_outbox = await session.scalar(select(func.count()).select_from(OutboxEvent))
    return {
        "forwarded": forwarded or 0,
        "dead": dead or 0,
        "failed": failed or 0,
        "locked": in_progress or 0,
        "total_outbox": total_outbox or 0,
    }

async def run_demo(seed: int = 1000, instances: int = 1, fail_ratio: float = 0.05):
    await seed_outbox(seed, fail_ratio=fail_ratio, clear=True)
    start = time.monotonic()
    if instances <= 1:
        await run_until_idle(instance_id="inst-1")
    else:
        # run multiple forwarder loops concurrently to validate locking
        tasks = [asyncio.create_task(run_until_idle(instance_id=f"inst-{i+1}")) for i in range(instances)]
        await asyncio.gather(*tasks)
    elapsed = time.monotonic() - start
    summary = await summarize()
    summary["elapsed_seconds"] = round(elapsed, 3)
    # compute average latency rough: cannot derive directly without extra columns; omitted
    return summary

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Outbox forwarder test runner")
    parser.add_argument("--seed", type=int, default=1000, help="Number of outbox rows to seed")
    parser.add_argument("--instances", type=int, default=1, help="Number of forwarder instances to run concurrently")
    parser.add_argument("--fail-ratio", type=float, default=0.05, help="Ratio of records that intentionally fail (topic=fail)")
    args = parser.parse_args()

    summary = await run_demo(seed=args.seed, instances=args.instances, fail_ratio=args.fail_ratio)
    print("--- Test Summary ---")
    for k, v in summary.items():
        print(f"{k}: {v}")

if __name__ == "__main__":
    asyncio.run(main())
