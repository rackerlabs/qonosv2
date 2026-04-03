"""SQLAlchemy models for QonoS v2 persistence"""

from __future__ import annotations

import enum
from datetime import datetime
from typing import Any, Optional

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
  pass


class JobStatus(str, enum.Enum):
  QUEUED = "QUEUED"
  PROCESSING = "PROCESSING"
  DONE = "DONE"
  TIMED_OUT = "TIMED_OUT"
  ERROR = "ERROR"
  CANCELLED = "CANCELLED"
  HARD_TIMED_OUT = "HARD_TIMED_OUT"
  MAX_RETRIED = "MAX_RETRIED"


class ActionType(Base):
  __tablename__ = "action_types"

  action_type: Mapped[str] = mapped_column(sa.String(100), primary_key=True)
  name: Mapped[str] = mapped_column(sa.String(255), nullable=False)
  description: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
  action_parameters_schema: Mapped[dict[str, Any]] = mapped_column(sa.JSON, nullable=False)
  enabled: Mapped[bool] = mapped_column(sa.Boolean, nullable=False, default=True)
  created_at: Mapped[datetime] = mapped_column(
    sa.DateTime(timezone=True), nullable=False, server_default=sa.func.current_timestamp()
  )
  updated_at: Mapped[datetime] = mapped_column(
    sa.DateTime(timezone=True),
    nullable=False,
    server_default=sa.func.current_timestamp(),
    onupdate=sa.func.current_timestamp(),
  )


class Schedule(Base):
  __tablename__ = "schedules"

  id: Mapped[str] = mapped_column(sa.CHAR(36), primary_key=True)
  project_id: Mapped[str] = mapped_column(sa.CHAR(36), nullable=False, index=True)
  name: Mapped[str] = mapped_column(sa.String(255), nullable=False)
  description: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
  action_type: Mapped[str] = mapped_column(
    sa.String(100), sa.ForeignKey("action_types.action_type"), nullable=False
  )
  cron_expression: Mapped[str] = mapped_column(sa.String(255), nullable=False)
  action_parameters: Mapped[dict[str, Any]] = mapped_column(sa.JSON, nullable=False)
  retention_policy: Mapped[Optional[dict[str, Any]]] = mapped_column(sa.JSON, nullable=True)
  enabled: Mapped[bool] = mapped_column(sa.Boolean, nullable=False, default=True)
  next_run_at: Mapped[Optional[datetime]] = mapped_column(sa.DateTime(timezone=True), nullable=True)
  last_run_at: Mapped[Optional[datetime]] = mapped_column(sa.DateTime(timezone=True), nullable=True)
  created_at: Mapped[datetime] = mapped_column(
    sa.DateTime(timezone=True), nullable=False, server_default=sa.func.current_timestamp()
  )
  updated_at: Mapped[datetime] = mapped_column(
    sa.DateTime(timezone=True),
    nullable=False,
    server_default=sa.func.current_timestamp(),
    onupdate=sa.func.current_timestamp(),
  )
  deleted_at: Mapped[Optional[datetime]] = mapped_column(sa.DateTime(timezone=True), nullable=True)


sa.Index(
  "schedules_project_active_name_uniq",
  Schedule.project_id,
  Schedule.name,
  unique=True,
  postgresql_where=None,
)
sa.Index("schedules_next_run_at_idx", Schedule.next_run_at)


class Job(Base):
  __tablename__ = "jobs"

  id: Mapped[str] = mapped_column(sa.CHAR(36), primary_key=True)
  schedule_id: Mapped[str] = mapped_column(
    sa.CHAR(36), sa.ForeignKey("schedules.id"), nullable=False, index=True
  )
  project_id: Mapped[str] = mapped_column(sa.CHAR(36), nullable=False, index=True)
  action_type: Mapped[str] = mapped_column(sa.String(100), nullable=False)
  action_parameters: Mapped[dict[str, Any]] = mapped_column(sa.JSON, nullable=False)
  run_at: Mapped[datetime] = mapped_column(sa.DateTime(timezone=True), nullable=False)
  status: Mapped[JobStatus] = mapped_column(
    sa.Enum(JobStatus, name="job_status"), nullable=False, default=JobStatus.QUEUED
  )
  version: Mapped[int] = mapped_column(sa.Integer, nullable=False, default=1)
  started_at: Mapped[Optional[datetime]] = mapped_column(sa.DateTime(timezone=True), nullable=True)
  ended_at: Mapped[Optional[datetime]] = mapped_column(sa.DateTime(timezone=True), nullable=True)
  retry_count: Mapped[int] = mapped_column(sa.Integer, nullable=False, default=0)
  error_message: Mapped[Optional[str]] = mapped_column(sa.Text, nullable=True)
  created_at: Mapped[datetime] = mapped_column(
    sa.DateTime(timezone=True), nullable=False, server_default=sa.func.current_timestamp()
  )
  updated_at: Mapped[datetime] = mapped_column(
    sa.DateTime(timezone=True),
    nullable=False,
    server_default=sa.func.current_timestamp(),
    onupdate=sa.func.current_timestamp(),
  )
  cancelled_at: Mapped[Optional[datetime]] = mapped_column(sa.DateTime(timezone=True), nullable=True)
  completed_at: Mapped[Optional[datetime]] = mapped_column(sa.DateTime(timezone=True), nullable=True)


sa.Index("jobs_project_id_run_at_idx", Job.project_id, Job.run_at)
sa.Index("jobs_schedule_id_run_at_idx", Job.schedule_id, Job.run_at)
sa.Index("jobs_status_idx", Job.status)


class JobFault(Base):
  __tablename__ = "job_faults"

  id: Mapped[int] = mapped_column(sa.Integer, primary_key=True, autoincrement=True)
  job_id: Mapped[str] = mapped_column(sa.CHAR(36), sa.ForeignKey("jobs.id"), nullable=False, index=True)
  project_id: Mapped[str] = mapped_column(sa.CHAR(36), nullable=False, index=True)
  occurred_at: Mapped[datetime] = mapped_column(
    sa.DateTime(timezone=True), nullable=False, server_default=sa.func.current_timestamp()
  )
  code: Mapped[str] = mapped_column(sa.String(100), nullable=False)
  title: Mapped[str] = mapped_column(sa.String(255), nullable=False)
  detail: Mapped[str] = mapped_column(sa.Text, nullable=False)
  extra: Mapped[Optional[dict[str, Any]]] = mapped_column(sa.JSON, nullable=True)
  created_at: Mapped[datetime] = mapped_column(
    sa.DateTime(timezone=True), nullable=False, server_default=sa.func.current_timestamp()
  )

sa.Index("job_faults_job_id_idx", JobFault.job_id, JobFault.occurred_at)
sa.Index("job_faults_project_id_idx", JobFault.project_id, JobFault.occurred_at)

