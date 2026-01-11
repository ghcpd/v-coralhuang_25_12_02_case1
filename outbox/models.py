from __future__ import annotations
from datetime import datetime
from sqlalchemy import Column, String, DateTime, Text, Integer, Index
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class OutboxEvent(Base):
    __tablename__ = "event_outbox"
    id = Column(String(64), primary_key=True)
    topic = Column(String(64), nullable=False, index=True)
    payload = Column(Text, nullable=False)
    status = Column(String(16), nullable=False, default="new")  # new|locked|forwarded|failed|dead
    attempt = Column(Integer, nullable=False, default=0)
    next_retry_at = Column(DateTime, nullable=True)
    locked_by = Column(String(64), nullable=True)
    locked_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_outbox_status", "status"),
        Index("idx_outbox_next_retry", "next_retry_at"),
        Index("idx_outbox_locked_at", "locked_at"),
    )

class Event(Base):
    __tablename__ = "events"
    id = Column(String(64), primary_key=True)
    topic = Column(String(64), nullable=False, index=True)
    payload = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class OutboxDLQ(Base):
    __tablename__ = "outbox_dlq"
    id = Column(String(64), primary_key=True)
    topic = Column(String(64), nullable=False, index=True)
    payload = Column(Text, nullable=False)
    attempt = Column(Integer, nullable=False)
    reason = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    dead_letter_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class OutboxAudit(Base):
    __tablename__ = "event_outbox_audit"
    id = Column(Integer, primary_key=True, autoincrement=True)
    outbox_id = Column(String(64), nullable=False, index=True)
    action = Column(String(32), nullable=False)  # lock|forward|retry|dead|dlq|unlock|stale_reset
    status_before = Column(String(16), nullable=True)
    status_after = Column(String(16), nullable=True)
    attempt = Column(Integer, nullable=True)
    note = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
