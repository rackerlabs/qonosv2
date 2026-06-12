"""Shared time helpers (UTC timestamps)."""

from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def as_utc_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def as_utc_naive(value: datetime) -> datetime:
    """Normalize to UTC then strip tzinfo for MySQL/MariaDB comparisons."""
    return as_utc_aware(value).replace(tzinfo=None)


def utc_now_naive() -> datetime:
    return utc_now().replace(tzinfo=None)
