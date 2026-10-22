from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.src.models import ExecutionProfile as ExecutionProfileModel
from db.src.models import Schedule as ScheduleModel

VOLUME_BACKUP_ACTION_TYPES = ("volume_backup_full", "volume_backup_incremental")
_CRON_ORDER_LOOKAHEAD_DAYS = 14
_CRON_ORDER_MAX_OCCURRENCES = 64
_MIN_FULL_TO_INCREMENTAL_GAP_MINUTES = 60


def extract_volume_id_from_params(action_type: str, params: dict[str, Any]) -> str | None:
    if action_type not in set(VOLUME_BACKUP_ACTION_TYPES):
        return None
    value = params.get("volume_id")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def advisories_from_flags(*, has_full: bool, has_incremental: bool) -> list[str]:
    advisories: list[str] = []
    if has_full and has_incremental:
        advisories.append(
            "Both full and incremental schedules exist for this volume; "
            "recommended pattern is weekly full + daily incremental."
        )
    if has_incremental and not has_full:
        advisories.append("Incremental schedule exists without a full schedule for this volume.")
    return advisories


def _normalize_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _next_occurrences(cron_expression: str, *, now: datetime) -> list[datetime]:
    from .utils.cron import compute_next_run_at

    values: list[datetime] = []
    cursor = now
    horizon = now + timedelta(days=_CRON_ORDER_LOOKAHEAD_DAYS)
    for _ in range(_CRON_ORDER_MAX_OCCURRENCES):
        try:
            run_at = _normalize_utc(compute_next_run_at(cron_expression, now=cursor))
        except ValueError:
            return []
        if run_at > horizon:
            break
        values.append(run_at)
        cursor = run_at
    return values


def _has_incremental_before_full_same_day(
    *, full_cron_expressions: list[str], incremental_cron_expressions: list[str]
) -> bool:
    if not full_cron_expressions or not incremental_cron_expressions:
        return False

    now = datetime.now(timezone.utc)
    full_by_date: dict[date, list[datetime]] = {}
    incremental_by_date: dict[date, list[datetime]] = {}

    for expr in full_cron_expressions:
        for run_at in _next_occurrences(expr, now=now):
            full_by_date.setdefault(run_at.date(), []).append(run_at)
    for expr in incremental_cron_expressions:
        for run_at in _next_occurrences(expr, now=now):
            incremental_by_date.setdefault(run_at.date(), []).append(run_at)

    for day, full_times in full_by_date.items():
        incremental_times = incremental_by_date.get(day)
        if not incremental_times:
            continue
        if min(incremental_times) <= min(full_times):
            return True
    return False


def _has_incremental_too_soon_after_full_same_day(
    *,
    full_cron_expressions: list[str],
    incremental_cron_expressions: list[str],
    min_gap_minutes: int = _MIN_FULL_TO_INCREMENTAL_GAP_MINUTES,
) -> bool:
    if not full_cron_expressions or not incremental_cron_expressions:
        return False
    if min_gap_minutes <= 0:
        return False

    now = datetime.now(timezone.utc)
    full_by_date: dict[date, list[datetime]] = {}
    incremental_by_date: dict[date, list[datetime]] = {}

    for expr in full_cron_expressions:
        for run_at in _next_occurrences(expr, now=now):
            full_by_date.setdefault(run_at.date(), []).append(run_at)
    for expr in incremental_cron_expressions:
        for run_at in _next_occurrences(expr, now=now):
            incremental_by_date.setdefault(run_at.date(), []).append(run_at)

    min_gap_seconds = min_gap_minutes * 60
    for day, full_times in full_by_date.items():
        incremental_times = incremental_by_date.get(day)
        if not incremental_times:
            continue
        earliest_full = min(full_times)
        for inc_at in incremental_times:
            if inc_at <= earliest_full:
                continue
            if (inc_at - earliest_full).total_seconds() < min_gap_seconds:
                return True
    return False


def _append_volume_cron_advisories(
    advisories: list[str],
    *,
    full_cron_expressions: list[str],
    incremental_cron_expressions: list[str],
) -> None:
    if _has_incremental_before_full_same_day(
        full_cron_expressions=full_cron_expressions,
        incremental_cron_expressions=incremental_cron_expressions,
    ):
        advisories.append(
            "Incremental backup schedule runs before full backup on overlapping days; "
            "adjust cron timing so full runs first."
        )
    if _has_incremental_too_soon_after_full_same_day(
        full_cron_expressions=full_cron_expressions,
        incremental_cron_expressions=incremental_cron_expressions,
    ):
        advisories.append(
            "Full and incremental run too close together on the same day "
            f"(under {_MIN_FULL_TO_INCREMENTAL_GAP_MINUTES} minutes apart). "
            f"Schedule incremental at least {_MIN_FULL_TO_INCREMENTAL_GAP_MINUTES} "
            "minutes after full."
        )


async def _volume_backup_schedules(
    *,
    session: AsyncSession,
    project_id: str,
    volume_id: str,
    exclude_schedule_id: str | None = None,
) -> list[ScheduleModel]:
    result = await session.execute(
        select(ScheduleModel).where(
            ScheduleModel.project_id == project_id,
            ScheduleModel.deleted_at.is_(None),
            ScheduleModel.action_type.in_(VOLUME_BACKUP_ACTION_TYPES),
        )
    )
    schedules: list[ScheduleModel] = []
    for schedule in result.scalars().all():
        if exclude_schedule_id and schedule.id == exclude_schedule_id:
            continue
        params = schedule.action_parameters or {}
        if not isinstance(params, dict):
            continue
        if extract_volume_id_from_params(schedule.action_type, params) != volume_id:
            continue
        schedules.append(schedule)
    return schedules


async def volume_backup_schedule_flags(
    *,
    session: AsyncSession,
    project_id: str,
    volume_id: str,
    exclude_schedule_id: str | None = None,
) -> tuple[bool, bool]:
    schedules = await _volume_backup_schedules(
        session=session,
        project_id=project_id,
        volume_id=volume_id,
        exclude_schedule_id=exclude_schedule_id,
    )
    has_full = False
    has_incremental = False
    for schedule in schedules:
        if schedule.action_type == "volume_backup_full":
            has_full = True
        elif schedule.action_type == "volume_backup_incremental":
            has_incremental = True
    return has_full, has_incremental


async def resolve_volume_backup_trust_id(
    *,
    session: AsyncSession,
    project_id: str,
    volume_id: str,
) -> str | None:
    """Return trust_id from an enabled execution profile on a volume backup schedule."""
    schedules = await _volume_backup_schedules(
        session=session,
        project_id=project_id,
        volume_id=volume_id,
    )
    for schedule in schedules:
        if not schedule.execution_profile_id:
            continue
        profile_result = await session.execute(
            select(ExecutionProfileModel).where(
                ExecutionProfileModel.id == schedule.execution_profile_id,
                ExecutionProfileModel.project_id == project_id,
                ExecutionProfileModel.deleted_at.is_(None),
                ExecutionProfileModel.enabled.is_(True),
                ExecutionProfileModel.auth_type == "trust",
            )
        )
        profile = profile_result.scalar_one_or_none()
        if profile is None:
            continue
        trust_id = str(profile.trust_id or "").strip()
        if trust_id:
            return trust_id
    return None


async def volume_schedule_advisories(
    *,
    session: AsyncSession,
    project_id: str,
    action_type: str,
    action_parameters: dict[str, Any],
    cron_expression: str | None = None,
    exclude_schedule_id: str | None = None,
) -> list[str]:
    volume_id = extract_volume_id_from_params(action_type, action_parameters)
    if not volume_id:
        return []

    schedules = await _volume_backup_schedules(
        session=session,
        project_id=project_id,
        volume_id=volume_id,
        exclude_schedule_id=exclude_schedule_id,
    )
    has_full = any(s.action_type == "volume_backup_full" for s in schedules)
    has_incremental = any(s.action_type == "volume_backup_incremental" for s in schedules)

    if action_type == "volume_backup_full":
        has_full = True
    elif action_type == "volume_backup_incremental":
        has_incremental = True

    advisories = advisories_from_flags(has_full=has_full, has_incremental=has_incremental)
    full_crons = [
        str(s.cron_expression or "").strip()
        for s in schedules
        if s.action_type == "volume_backup_full" and str(s.cron_expression or "").strip()
    ]
    incremental_crons = [
        str(s.cron_expression or "").strip()
        for s in schedules
        if s.action_type == "volume_backup_incremental" and str(s.cron_expression or "").strip()
    ]
    candidate_cron = str(cron_expression or "").strip()
    if candidate_cron:
        if action_type == "volume_backup_full":
            full_crons.append(candidate_cron)
        elif action_type == "volume_backup_incremental":
            incremental_crons.append(candidate_cron)
    _append_volume_cron_advisories(
        advisories,
        full_cron_expressions=full_crons,
        incremental_cron_expressions=incremental_crons,
    )
    return advisories


async def volume_schedule_advisories_for_volume(
    *,
    session: AsyncSession,
    project_id: str,
    volume_id: str,
) -> list[str]:
    schedules = await _volume_backup_schedules(
        session=session,
        project_id=project_id,
        volume_id=volume_id,
    )
    has_full = any(s.action_type == "volume_backup_full" for s in schedules)
    has_incremental = any(s.action_type == "volume_backup_incremental" for s in schedules)
    advisories = advisories_from_flags(has_full=has_full, has_incremental=has_incremental)
    full_crons = [
        str(s.cron_expression or "").strip()
        for s in schedules
        if s.action_type == "volume_backup_full" and str(s.cron_expression or "").strip()
    ]
    incremental_crons = [
        str(s.cron_expression or "").strip()
        for s in schedules
        if s.action_type == "volume_backup_incremental" and str(s.cron_expression or "").strip()
    ]
    _append_volume_cron_advisories(
        advisories,
        full_cron_expressions=full_crons,
        incremental_cron_expressions=incremental_crons,
    )
    return advisories
