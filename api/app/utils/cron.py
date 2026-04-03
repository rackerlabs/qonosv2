"""Shared cron helpers for API routes and the scheduler."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from croniter import croniter

from .time import utc_now


def compute_next_run_at(cron_expression: str, *, now: Optional[datetime] = None) -> datetime:
    """Return the next cron time strictly after ``now``.

    Raises ``ValueError`` for invalid/unsupported cron syntax.
    """
    base = now or utc_now()
    try:
        itr = croniter(cron_expression, base)
        return itr.get_next(datetime)
    except Exception as exc:
        raise ValueError(
            "Invalid cron_expression: unsupported or malformed cron syntax."
        ) from exc

