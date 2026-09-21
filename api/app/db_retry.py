from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TypeVar

from sqlalchemy.exc import DBAPIError, OperationalError

from .config import get_config

logger = logging.getLogger("qonos.db")

_TRANSIENT_DB_ERRNOS = frozenset({2003, 2006, 2013, 1205, 1213})
_TRANSIENT_DB_PHRASES = (
    "lost connection",
    "server has gone away",
    "can't connect",
    "connection refused",
    "deadlock",
    "lock wait timeout",
)

T = TypeVar("T")


def _get_dbapi_errno(err: BaseException) -> int | None:
    args = getattr(err, "args", None)
    if not args:
        return None
    try:
        return int(args[0])
    except (TypeError, ValueError):
        return None


def is_transient_db_error(exc: BaseException) -> bool:
    """Return True when the error looks like a transient DB connectivity/contention issue."""
    if not isinstance(exc, (OperationalError, DBAPIError)):
        return False

    orig = getattr(exc, "orig", exc)
    if _get_dbapi_errno(orig) in _TRANSIENT_DB_ERRNOS:
        return True

    msg = str(orig).lower()
    return any(phrase in msg for phrase in _TRANSIENT_DB_PHRASES)


async def with_db_retry(
    operation: Callable[[], Awaitable[T]],
    *,
    attempts: int | None = None,
    backoff_seconds: float | None = None,
    context: str = "db",
) -> T:
    """Run an async DB operation, retrying transient failures with exponential backoff."""
    cfg = get_config()
    max_attempts = max(1, attempts if attempts is not None else cfg.db_retry_attempts)
    base_backoff = max(
        0.0,
        backoff_seconds if backoff_seconds is not None else cfg.db_retry_backoff_seconds,
    )

    for attempt in range(1, max_attempts + 1):
        try:
            return await operation()
        except Exception as exc:
            if attempt >= max_attempts or not is_transient_db_error(exc):
                raise
            delay = base_backoff * (2 ** (attempt - 1))
            logger.warning(
                "%s decision=transient_db_retry attempt=%s/%s delay_seconds=%s error=%s",
                context,
                attempt,
                max_attempts,
                delay,
                str(exc),
            )
            if delay > 0:
                await asyncio.sleep(delay)

    raise RuntimeError("with_db_retry exhausted attempts without returning or raising")
