"""Abstract persistence interface for QonoS v2."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from .models import JobStatus


class QonoSPersistenceRepository(ABC):
  """Interface for QonoS persistence operations."""

  @abstractmethod
  async def cas_schedule_next_run_at(
    self,
    *,
    schedule_id: str,
    expected_next_run_at: Optional[str],
    next_run_at: Optional[str],
  ) -> bool:
    """Compare-and-swap `next_run_at` to prevent duplicate job creation.

    Returns True if the update succeeded, False if the expected value did not match.
    """

  @abstractmethod
  async def transition_job_status_occ(
    self,
    *,
    job_id: str,
    project_id: str,
    from_status: JobStatus,
    to_status: JobStatus,
    expected_version: int,
  ) -> bool:
    """Optimistic concurrency-controlled status transition.

    Returns True if the transition succeeded, False if version/status mismatched.
    """

  @abstractmethod
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
    """Append an entry to the job_faults audit log."""

