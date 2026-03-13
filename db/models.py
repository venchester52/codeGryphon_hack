from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.database import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    email: Mapped[str] = mapped_column(String(255), unique=True, index=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    analysis_sessions: Mapped[list["AnalysisSession"]] = relationship(
        "AnalysisSession",
        back_populates="user",
        cascade="all, delete-orphan",
    )


class AnalysisSession(Base):
    __tablename__ = "analysis_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    original_filename: Mapped[str] = mapped_column(String(255), nullable=False)
    uploaded_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    mapping_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    metrics_info_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    summary_kpis_json: Mapped[str] = mapped_column(Text, default="{}", nullable=False)
    recommendations_json: Mapped[str] = mapped_column(Text, default="[]", nullable=False)
    campaign_context: Mapped[str] = mapped_column(Text, default="", nullable=False)

    user: Mapped[User] = relationship("User", back_populates="analysis_sessions")
    chat_messages: Mapped[list["ChatMessage"]] = relationship(
        "ChatMessage",
        back_populates="analysis_session",
        cascade="all, delete-orphan",
    )


class ChatMessage(Base):
    __tablename__ = "chat_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    analysis_session_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("analysis_sessions.id"),
        nullable=False,
        index=True,
    )
    role: Mapped[str] = mapped_column(String(20), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    analysis_session: Mapped[AnalysisSession] = relationship("AnalysisSession", back_populates="chat_messages")
