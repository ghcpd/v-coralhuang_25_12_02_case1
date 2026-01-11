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
    status = Column(String(16), nullable=False, default="new")  # new|locked|forwarded|failed
    attempt = Column(Integer, nullable=False, default=0)
    # When to next try delivery (for retries & backoff)
    next_retry_at = Column(DateTime, nullable=True)
    # Optional lease/lock info so multiple forwarders don't double-process
    locked_at = Column(DateTime, nullable=True)
    locked_by = Column(String(64), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_outbox_status", "status"),
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
    failed_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_attempt = Column(Integer, nullable=False, default=0)
    reason = Column(Text, nullable=True)


class OutboxAudit(Base):
    __tablename__ = "event_outbox_audit"
    # simple integer PK for audit records
    id = Column(Integer, primary_key=True, autoincrement=True)
    outbox_id = Column(String(64), nullable=False, index=True)
    action = Column(String(32), nullable=False)  # locked|forwarded|retry_scheduled|moved_to_dlq|unlocked
    attempt = Column(Integer, nullable=True)
    detail = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
