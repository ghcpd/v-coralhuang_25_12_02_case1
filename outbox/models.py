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
    locked_at = Column(DateTime, nullable=True)
    lock_owner = Column(String(64), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_outbox_status", "status"),
        Index("idx_outbox_status_next_retry", "status", "next_retry_at"),
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
    error = Column(Text, nullable=True)
    attempts = Column(Integer, nullable=False, default=0)
    original_created_at = Column(DateTime, nullable=True)
    failed_at = Column(DateTime, nullable=False, default=datetime.utcnow)

class EventOutboxAudit(Base):
    __tablename__ = "event_outbox_audit"
    id = Column(Integer, primary_key=True, autoincrement=True)
    outbox_id = Column(String(64), nullable=False, index=True)
    action = Column(String(32), nullable=False)
    details = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
