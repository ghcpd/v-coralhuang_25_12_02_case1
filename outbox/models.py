from __future__ import annotations
from datetime import datetime
from sqlalchemy import Column, String, DateTime, Text, Integer, Index, Boolean
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class OutboxEvent(Base):
    __tablename__ = "event_outbox"
    id = Column(String(64), primary_key=True)
    topic = Column(String(64), nullable=False, index=True)
    payload = Column(Text, nullable=False)
    status = Column(String(16), nullable=False, default="new")  # new|locked|forwarded|failed
    attempt = Column(Integer, nullable=False, default=0)
    next_retry_at = Column(DateTime, nullable=True)  # Exponential backoff scheduling
    locked_at = Column(DateTime, nullable=True)  # Lock timestamp for lease recovery
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_outbox_status_retry", "status", "next_retry_at"),
        Index("idx_outbox_locked", "locked_at"),
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
    reason = Column(String(256), nullable=False)  # Why it was dead-lettered
    attempt = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_dlq_created", "created_at"),
    )

class OutboxAudit(Base):
    __tablename__ = "outbox_audit"
    id = Column(String(64), primary_key=True)  # UUID for audit record
    outbox_id = Column(String(64), nullable=False, index=True)
    action = Column(String(32), nullable=False)  # lock|forward|retry|dead|unlock
    details = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_audit_outbox", "outbox_id"),
        Index("idx_audit_action", "action"),
    )
