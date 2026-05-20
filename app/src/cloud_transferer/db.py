from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlmodel import Session, SQLModel, create_engine

from .config import settings

_engine = None


def get_engine():
    global _engine
    if _engine is None:
        url = f"sqlite:///{settings.db_path}"
        _engine = create_engine(
            url,
            echo=False,
            connect_args={"check_same_thread": False, "timeout": 30},
        )
    return _engine


def init_db() -> None:
    from . import models  # noqa: F401  确保表注册

    SQLModel.metadata.create_all(get_engine())


@contextmanager
def session_scope() -> Iterator[Session]:
    session = Session(get_engine())
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
