from __future__ import annotations
import asyncio
import os
import random
import time
from datetime import datetime, timezone

import sys
sys.path.append(os.getcwd())

from outbox.database import init_db, SessionLocal
from sqlalchemy import text
from outbox.models import OutboxEvent, Event, OutboxDLQ


async def seed_outbox(n: int = 500, fail_rate: float = 0.05, preexisting_rate: float = 0.01):
    await init_db()
    async with SessionLocal() as session:
        # clear any existing tables to ensure predictable run
        await session.execute(text("DELETE FROM events"))
        await session.execute(text("DELETE FROM outbox_dlq"))
        await session.execute(text("DELETE FROM event_outbox_audit"))
        await session.execute(text("DELETE FROM event_outbox"))
        await session.commit()

        now = datetime.utcnow()
        created = 0
        for i in range(n):
            payload = f"payload-{i}"
            if random.random() < fail_rate:
                payload += " __bad__"

            evt = OutboxEvent(
                id=f"id-{int(time.time()*1000)}-{i}-{random.randint(0,999)}",
                topic=random.choice(["alpha", "beta", "gamma"]),
                payload=payload,
                status="new",
                created_at=now,
                updated_at=now,
            )
            session.add(evt)
            created += 1
            if created % 200 == 0:
                await session.commit()
        await session.commit()

        # Add a few pre-existing events (idempotency test)
        if preexisting_rate > 0:
            # mark some existing items as already delivered
            q = (await session.execute(text("SELECT id FROM event_outbox LIMIT 5"))).scalars().all()
            for i, rid in enumerate(q):
                e = Event(id=rid, topic="rehash", payload="preexisting")
                session.add(e)
            await session.commit()


async def run_forwarders(instances: int = 2, rounds: int = 1000):
    from outbox import forwarder

    # run rounds of single-pass forwarder instances to simulate concurrent workers
    for r in range(rounds):
        async with SessionLocal() as session:
            # check if any eligible rows remain (new/failed and next_retry_at is NULL or <= now)
            now = datetime.utcnow()
            count = (await session.execute(text("SELECT count(*) FROM event_outbox WHERE status IN ('new','failed') AND (next_retry_at IS NULL OR next_retry_at <= :now)"),
                {"now": now},
            )).scalar_one()
            if count == 0:
                break

        # start `instances` forwarders concurrently - each does a single pass
        tasks = []
        for i in range(instances):
            owner = f"test-worker-{i}-{r}"
            tasks.append(asyncio.create_task(forwarder.run_loop(owner=owner, single_pass=True)))

        await asyncio.gather(*tasks)


async def collect_summary():
    async with SessionLocal() as session:
        forwarded = (await session.execute(text("SELECT count(*) FROM events"))).scalar_one()
        dlq = (await session.execute(text("SELECT count(*) FROM outbox_dlq"))).scalar_one()
        outbox_forwarded = (await session.execute(text("SELECT count(*) FROM event_outbox WHERE status='forwarded' "))).scalar_one()
        outbox_failed = (await session.execute(text("SELECT count(*) FROM event_outbox WHERE status='failed' "))).scalar_one()
        outbox_new = (await session.execute(text("SELECT count(*) FROM event_outbox WHERE status='new' "))).scalar_one()

        # compute average latency across forwarded events using outbox.created_at -> events.created_at if present
        rows = (await session.execute(text(
            "SELECT e.created_at as evt_created, o.created_at as out_created FROM events e JOIN event_outbox o ON e.id=o.id"
        ))).all()
        latencies = []
        for evt_created, out_created in rows:
            # SQLite returns strings when using text() — normalize to datetime
            if isinstance(evt_created, str):
                try:
                    evt_created = datetime.fromisoformat(evt_created)
                except Exception:
                    evt_created = None
            if isinstance(out_created, str):
                try:
                    out_created = datetime.fromisoformat(out_created)
                except Exception:
                    out_created = None

            if evt_created and out_created:
                diff = (evt_created - out_created).total_seconds()
                latencies.append(diff)

        avg_latency = sum(latencies) / len(latencies) if latencies else None

        return {
            "forwarded_events": forwarded,
            "outbox_forwarded": outbox_forwarded,
            "outbox_failed": outbox_failed,
            "outbox_new": outbox_new,
            "dlq": dlq,
            "avg_latency_seconds": avg_latency,
        }


async def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=500, help="number of outbox rows to seed")
    parser.add_argument("--fail-rate", type=float, default=0.03, help="percent of rows that should fail (0-1)")
    parser.add_argument("--workers", type=int, default=2, help="concurrent forwarder instances per round")
    args = parser.parse_args()

    start = time.time()
    print("Initializing DB and seeding data...")
    await seed_outbox(args.count, fail_rate=args.fail_rate)
    print("Seeding finished — starting forwarders")

    await run_forwarders(instances=args.workers, rounds=2000)

    # give system one more sweep to pick up retries when eligible
    await run_forwarders(instances=args.workers, rounds=200)

    summary = await collect_summary()
    elapsed = time.time() - start

    print("\n=== Test summary ===")
    print(f"seeded_rows: {args.count}")
    print(f"forwarded_events (events table): {summary['forwarded_events']}")
    print(f"outbox_forwarded: {summary['outbox_forwarded']}")
    print(f"outbox_failed: {summary['outbox_failed']}")
    print(f"outbox_new: {summary['outbox_new']}")
    print(f"dlq_count: {summary['dlq']}")
    print(f"avg_latency_seconds (approx): {summary['avg_latency_seconds']}")
    print(f"elapsed_seconds: {elapsed:.2f}")


if __name__ == "__main__":
    asyncio.run(main())
