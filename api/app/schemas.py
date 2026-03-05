from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class RetentionPolicy(BaseModel):
    count: Optional[int] = Field(default=None, ge=1)
    age_seconds: Optional[int] = Field(default=None, ge=1)


class ScheduleBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, max_length=4096)
    action_type: str = Field(..., max_length=100)
    cron_expression: str = Field(..., max_length=255)
    action_parameters: Dict[str, Any]
    retention_policy: Optional[RetentionPolicy] = None
    enabled: bool = True


class ScheduleCreate(ScheduleBase):
    """Payload for creating a schedule."""

    pass


class ScheduleUpdate(BaseModel):
    """Partial update payload for schedules."""

    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, max_length=4096)
    cron_expression: Optional[str] = Field(default=None, max_length=255)
    action_parameters: Optional[Dict[str, Any]] = None
    retention_policy: Optional[RetentionPolicy] = None
    enabled: Optional[bool] = None


class Schedule(ScheduleBase):
    id: str
    project_id: str
    next_run_at: Optional[datetime] = None
    last_run_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ScheduleList(BaseModel):
    schedules: List[Schedule]
    links: List[Dict[str, Any]] = Field(default_factory=list)


class JobStatus(str, Enum):
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    DONE = "DONE"
    TIMED_OUT = "TIMED_OUT"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"
    HARD_TIMED_OUT = "HARD_TIMED_OUT"
    MAX_RETRIED = "MAX_RETRIED"


class Job(BaseModel):
    id: str
    schedule_id: str
    project_id: str
    action_type: str
    action_parameters: Dict[str, Any]
    status: JobStatus
    run_at: datetime
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    retry_count: int = 0
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class JobCreate(BaseModel):
    schedule_id: str = Field(..., description="ID of the schedule to create a job from.")


class JobUpdate(BaseModel):
    status: Optional[JobStatus] = None
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    retry_count: Optional[int] = Field(default=None, ge=0)
    error_message: Optional[str] = None
    cancelled_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class JobList(BaseModel):
    jobs: List[Job]
    links: List[Dict[str, Any]] = Field(default_factory=list)


class Action(BaseModel):
    action_type: str
    name: str
    description: Optional[str] = None
    action_parameters_schema: Dict[str, Any]
    enabled: bool
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ActionList(BaseModel):
    actions: List[Action]
    links: List[Dict[str, Any]] = Field(default_factory=list)

