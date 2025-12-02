from __future__ import annotations
from datetime import datetime
from sqlalchemy import Column, String, DateTime, Text, Integer, Index, Float
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class OutboxEvent(Base):
    __tablename__ = "event_outbox"
    id = Column(String(64), primary_key=True)
    topic = Column(String(64), nullable=False, index=True)
    payload = Column(Text, nullable=False)
    status = Column(String(16), nullable=False, default="new")  # new|locked|forwarded|failed
    attempt = Column(Integer, nullable=False, default=0)
    next_retry_at = Column(DateTime, nullable=True, index=True)  # For exponential backoff
    locked_at = Column(DateTime, nullable=True)  # For lease management
    locked_by = Column(String(64), nullable=True)  # Instance identifier
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_outbox_status", "status"),
        Index("idx_outbox_retry", "status", "next_retry_at"),
    )

class Event(Base):
    __tablename__ = "events"
    id = Column(String(64), primary_key=True)
    topic = Column(String(64), nullable=False, index=True)
    payload = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

class OutboxDLQ(Base):
    """Dead Letter Queue for unrecoverable failures"""
    __tablename__ = "outbox_dlq"
    id = Column(String(64), primary_key=True)
    original_id = Column(String(64), nullable=False, index=True)
    topic = Column(String(64), nullable=False)
    payload = Column(Text, nullable=False)
    failure_reason = Column(Text, nullable=True)
    attempts = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

class OutboxAudit(Base):
    """Minimal audit trail for state changes"""
    __tablename__ = "event_outbox_audit"
    id = Column(Integer, primary_key=True, autoincrement=True)
    outbox_id = Column(String(64), nullable=False, index=True)
    event_type = Column(String(32), nullable=False)  # locked|forwarded|retry|dead
    status_from = Column(String(16), nullable=True)
    status_to = Column(String(16), nullable=True)
    attempt = Column(Integer, nullable=False, default=0)
    details = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("idx_audit_outbox_id", "outbox_id"),
        Index("idx_audit_event_type", "event_type"),
    )
