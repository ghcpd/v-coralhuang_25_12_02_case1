from __future__ import annotations
import asyncio
import sys
from outbox.database import init_db, SessionLocal
from outbox.models import OutboxDLQ, OutboxEvent
from sqlalchemy import text


async def replay_all():
    await init_db()
    async with SessionLocal() as session:
        rows = (await session.execute(text("SELECT id, topic, payload FROM outbox_dlq"))).all()
        for rid, topic, payload in rows:
            # re-create an outbox row and remove from dlq
            out = OutboxEvent(id=rid, topic=topic, payload=payload, status="new", attempt=0)
            session.add(out)
            await session.execute(text("DELETE FROM outbox_dlq WHERE id = :id"), {"id": rid})
        await session.commit()


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="replay all DLQ entries")
    args = parser.parse_args()

    if args.all:
        asyncio.run(replay_all())
    else:
        print("Use --all to replay all DLQ items")


if __name__ == "__main__":
    main()
