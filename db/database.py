from __future__ import annotations

import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

DEFAULT_DB_DIR = Path("data")
DEFAULT_DB_FILE = DEFAULT_DB_DIR / "marketing_auditor.sqlite3"


def _build_database_url() -> str:
    custom_url = os.getenv("DATABASE_URL", "").strip()
    if custom_url:
        return custom_url

    db_path = Path(os.getenv("SQLITE_DB_PATH", str(DEFAULT_DB_FILE))).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{db_path}"


DATABASE_URL = _build_database_url()

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def init_database() -> None:
    try:
        from db import models  # noqa: F401  # pylint: disable=unused-import

        Base.metadata.create_all(bind=engine)
    except Exception as error:
        raise RuntimeError(f"Не удалось инициализировать базу данных: {error}") from error


def get_db_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
