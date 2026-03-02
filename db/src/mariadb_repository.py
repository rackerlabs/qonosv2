"""MariaDB implementation of QonoS v2 persistence."""

from __future__ import annotations

from typing import Optional

from sqlalchemy import and_, select, text, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import sessionmaker

from .errors import PersistenceError, PersistenceErrorCode
from .models import Job, JobFault, JobStatus, Schedule
from .repository import QonoSPersistenceRepository


class MariaDBQonoSRepository(QonoSPersistenceRepository):
  """Async SQLAlchemy-based repository for MariaDB."""

  def __init__(self, engine: AsyncEngine) -> None:
    self._engine = engine
    self._session_factory = sessionmaker(
      engine, class_=AsyncSession, expire_on_commit=False
    )

  async def cas_schedule_next_run_at(
    self,
    *,
    schedule_id: str,
    expected_next_run_at: Optional[str],
    next_run_at: Optional[str],
  ) -> bool:
    """CAS on schedules.next_run_at using MySQL null-safe equality (<=>)."""
    async with self._session_factory() as session:
      stmt = (
        update(Schedule)
        .where(
          and_(
            Schedule.id == schedule_id,
            text("next_run_at <=> :expected_next_run_at"),
            Schedule.deleted_at.is_(None),
          )
        )
        .values(next_run_at=next_run_at)
      )
      result = await session.execute(
        stmt,
        {"expected_next_run_at": expected_next_run_at},
      )
      await session.commit()
      return result.rowcount == 1

  async def transition_job_status_occ(
    self,
    *,
    job_id: str,
    project_id: str,
    from_status: JobStatus,
    to_status: JobStatus,
    expected_version: int,
  ) -> bool:
    """Optimistic concurrency-controlled job status transition."""
    async with self._session_factory() as session:
      stmt = (
        update(Job)
        .where(
          and_(
            Job.id == job_id,
            Job.project_id == project_id,
            Job.status == from_status,
            Job.version == expected_version,
          )
        )
        .values(status=to_status, version=Job.version + 1)
      )
      result = await session.execute(stmt)
      if result.rowcount == 1:
        await session.commit()
        return True

      job_row = await session.execute(
        select(Job).where(
          and_(Job.id == job_id, Job.project_id == project_id)
        )
      )
      job = job_row.scalar_one_or_none()
      if job is None:
        raise PersistenceError(
          "Job not found",
          code=PersistenceErrorCode.NOT_FOUND,
          meta={"job_id": job_id, "project_id": project_id},
        )
      return False

  async def append_job_fault(
    self,
    *,
    job_id: str,
    project_id: str,
    code: str,
    title: str,
    detail: str,
    extra: Optional[dict] = None,
  ) -> None:
    """Append-only insert into job_faults."""
    async with self._session_factory() as session:
      fault = JobFault(
        job_id=job_id,
        project_id=project_id,
        code=code,
        title=title,
        detail=detail,
        extra=extra,
      )
      session.add(fault)
      await session.commit()

