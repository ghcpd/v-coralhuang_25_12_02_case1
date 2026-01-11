from __future__ import annotations
import argparse
import asyncio
import uuid
from typing import Optional

from datetime import datetime

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, Event, OutboxDLQ, OutboxAudit


async def reset_tables():
    """Delete all rows from outbox-related tables."""
    await init_db()
    async with SessionLocal() as session:
        for model in (OutboxAudit, OutboxDLQ, Event, OutboxEvent):
            await session.execute(model.__table__.delete())
        await session.commit()


async def seed_outbox(count: int = 1000, fail_once_ratio: float = 0.01, fail_always_ratio: float = 0.005):
    await init_db()
    async with SessionLocal() as session:
        batch_size = 1000
        fail_once_every = int(1 / fail_once_ratio) if fail_once_ratio > 0 else None
        fail_always_every = int(1 / fail_always_ratio) if fail_always_ratio > 0 else None

        idx = 0
        to_commit = []
        while idx < count:
            idx += 1
            topic = "sample"
            if fail_always_every and idx % fail_always_every == 0:
                topic = "__force_fail__"
            elif fail_once_every and idx % fail_once_every == 0:
                topic = "__fail_once__"
            payload = f"{{\"message\": \"payload-{idx}\"}}"
            evt = OutboxEvent(id=str(uuid.uuid4()), topic=topic, payload=payload, status="new", attempt=0)
            session.add(evt)
            to_commit.append(evt)
            if len(to_commit) >= batch_size:
                await session.commit()
                to_commit.clear()
        if to_commit:
            await session.commit()
    return count


async def main_async(args):
    if args.reset:
        await reset_tables()
    seeded = await seed_outbox(count=args.count, fail_once_ratio=args.fail_once_ratio, fail_always_ratio=args.fail_always_ratio)
    print(f"Seeded {seeded} outbox records")


def main():
    parser = argparse.ArgumentParser(description="Seed the outbox table with sample data")
    parser.add_argument("--count", type=int, default=1000, help="Number of records to seed")
    parser.add_argument("--fail-once-ratio", type=float, default=0.01, help="Fraction of records that fail once")
    parser.add_argument("--fail-always-ratio", type=float, default=0.005, help="Fraction of records that always fail and go to DLQ")
    parser.add_argument("--reset", action="store_true", help="Reset tables before seeding")
    args = parser.parse_args()

    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
