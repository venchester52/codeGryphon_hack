from __future__ import annotations

import json
from typing import Any

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from db.auth import hash_password, verify_password
from db.models import AnalysisSession, ChatMessage, User


def _safe_json_dumps(payload: Any, fallback: str) -> str:
    try:
        return json.dumps(payload, ensure_ascii=False)
    except Exception:
        return fallback


def _safe_json_loads(payload: str, fallback: Any) -> Any:
    try:
        return json.loads(payload)
    except Exception:
        return fallback


def get_user_by_email(db: Session, email: str) -> User | None:
    normalized_email = email.strip().lower()
    if not normalized_email:
        return None

    return db.query(User).filter(User.email == normalized_email).first()


def create_user(db: Session, email: str, password: str) -> User:
    normalized_email = email.strip().lower()
    if not normalized_email:
        raise ValueError("Email не может быть пустым.")

    user = User(email=normalized_email, password_hash=hash_password(password))
    db.add(user)

    try:
        db.commit()
    except IntegrityError as error:
        db.rollback()
        raise ValueError("Пользователь с таким email уже существует.") from error

    db.refresh(user)
    return user


def authenticate_user(db: Session, email: str, password: str) -> User | None:
    user = get_user_by_email(db, email)
    if not user:
        return None

    if not verify_password(password, user.password_hash):
        return None

    return user


def create_analysis_session(
    db: Session,
    user_id: int,
    original_filename: str,
    mapping_data: dict[str, Any],
    metrics_info: dict[str, Any],
    summary_kpis: dict[str, Any],
    recommendations: list[str],
    campaign_context: str,
) -> AnalysisSession:
    row = AnalysisSession(
        user_id=user_id,
        original_filename=original_filename.strip() or "uploaded.csv",
        mapping_json=_safe_json_dumps(mapping_data, fallback="{}"),
        metrics_info_json=_safe_json_dumps(metrics_info, fallback="{}"),
        summary_kpis_json=_safe_json_dumps(summary_kpis, fallback="{}"),
        recommendations_json=_safe_json_dumps(recommendations, fallback="[]"),
        campaign_context=campaign_context.strip(),
    )

    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def get_user_analysis_sessions(db: Session, user_id: int, limit: int = 10) -> list[AnalysisSession]:
    safe_limit = max(1, min(limit, 100))
    return (
        db.query(AnalysisSession)
        .filter(AnalysisSession.user_id == user_id)
        .order_by(AnalysisSession.uploaded_at.desc())
        .limit(safe_limit)
        .all()
    )


def get_analysis_session(db: Session, session_id: int, user_id: int) -> AnalysisSession | None:
    return (
        db.query(AnalysisSession)
        .filter(AnalysisSession.id == session_id, AnalysisSession.user_id == user_id)
        .first()
    )


def add_chat_message(db: Session, analysis_session_id: int, role: str, content: str) -> ChatMessage:
    clean_role = role.strip().lower()
    clean_content = content.strip()
    if clean_role not in {"user", "assistant"}:
        raise ValueError("Роль сообщения должна быть user или assistant.")
    if not clean_content:
        raise ValueError("Содержимое сообщения не может быть пустым.")

    message = ChatMessage(
        analysis_session_id=analysis_session_id,
        role=clean_role,
        content=clean_content,
    )

    db.add(message)
    db.commit()
    db.refresh(message)
    return message


def get_chat_messages(db: Session, analysis_session_id: int) -> list[ChatMessage]:
    return (
        db.query(ChatMessage)
        .filter(ChatMessage.analysis_session_id == analysis_session_id)
        .order_by(ChatMessage.created_at.asc(), ChatMessage.id.asc())
        .all()
    )


def parse_analysis_session_payload(row: AnalysisSession) -> dict[str, Any]:
    return {
        "id": row.id,
        "user_id": row.user_id,
        "original_filename": row.original_filename,
        "uploaded_at": row.uploaded_at,
        "mapping": _safe_json_loads(row.mapping_json, {}),
        "metrics_info": _safe_json_loads(row.metrics_info_json, {}),
        "summary_kpis": _safe_json_loads(row.summary_kpis_json, {}),
        "recommendations": _safe_json_loads(row.recommendations_json, []),
        "campaign_context": row.campaign_context,
    }
