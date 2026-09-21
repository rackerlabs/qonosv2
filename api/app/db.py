from __future__ import annotations
from typing import AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from .config import get_config

_engine: Optional[AsyncEngine] = None
_session_factory: Optional[sessionmaker] = None


def _init_engine() -> None:
    global _engine, _session_factory
    if _engine is not None:
        return

    cfg = get_config()
    db_url = cfg.db_url
    if not db_url:
        raise RuntimeError("Database URL must be configured")

    _engine = create_async_engine(
        db_url,
        future=True,
        pool_pre_ping=True,
        pool_size=cfg.db_pool_size,
        max_overflow=cfg.db_max_overflow,
        pool_recycle=cfg.db_pool_recycle,
        pool_timeout=cfg.db_pool_timeout,
    )
    _session_factory = sessionmaker(
        _engine,
        expire_on_commit=False,
        class_=AsyncSession,
    )


def get_session_factory() -> sessionmaker:
    """Return the initialized SQLAlchemy AsyncSession factory."""
    if _engine is None:
        _init_engine()
    assert _session_factory is not None
    return _session_factory


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency that yields an AsyncSession."""
    if _engine is None:
        _init_engine()
    assert _session_factory is not None

    async with _session_factory() as session:
        yield session


async def close_db_engine() -> None:
    """Dispose the async engine and clear the session factory."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
