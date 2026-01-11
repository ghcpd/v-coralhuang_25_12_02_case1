from __future__ import annotations
import json
from typing import Dict

from sqlalchemy import select, func

from outbox.database import init_db, SessionLocal
from outbox.models import OutboxEvent, OutboxDLQ, Event, OutboxAudit


async def collect_summary() -> Dict[str, float]:
    await init_db()
    async with SessionLocal() as session:
        total_outbox = await session.scalar(select(func.count()).select_from(OutboxEvent))
        forwarded = await session.scalar(select(func.count()).where(OutboxEvent.status == "forwarded"))
        failed = await session.scalar(select(func.count()).where(OutboxEvent.status == "failed"))
        dead = await session.scalar(select(func.count()).where(OutboxEvent.status == "dead"))
        locked = await session.scalar(select(func.count()).where(OutboxEvent.status == "locked"))
        new = await session.scalar(select(func.count()).where(OutboxEvent.status == "new"))
        dlq = await session.scalar(select(func.count()).select_from(OutboxDLQ))
        events = await session.scalar(select(func.count()).select_from(Event))
        audit = await session.scalar(select(func.count()).select_from(OutboxAudit))
        avg_latency = await session.scalar(
            select(func.avg(func.julianday(OutboxEvent.updated_at) - func.julianday(OutboxEvent.created_at))).where(OutboxEvent.status == "forwarded")
        )
        avg_latency_sec = avg_latency * 86400 if avg_latency is not None else None
        return {
            "total_outbox": total_outbox or 0,
            "forwarded": forwarded or 0,
            "failed": failed or 0,
            "dead": dead or 0,
            "locked": locked or 0,
            "new": new or 0,
            "dlq": dlq or 0,
            "events": events or 0,
            "audit": audit or 0,
            "avg_latency_sec": avg_latency_sec,
        }


async def print_summary(as_json: bool = False):
    summary = await collect_summary()
    if as_json:
        print(json.dumps(summary, indent=2, default=str))
    else:
        for k, v in summary.items():
            print(f"{k}: {v}")


if __name__ == "__main__":
    import asyncio
    import argparse

    parser = argparse.ArgumentParser(description="Print outbox summary")
    parser.add_argument("--json", action="store_true", help="Output JSON")
    args = parser.parse_args()

    asyncio.run(print_summary(as_json=args.json))
