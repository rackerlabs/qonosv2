from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RetentionPolicy(BaseModel):
    type: str = Field(..., pattern="^(count|age)$")
    value: Optional[int] = Field(default=None, ge=0)
    max_age_days: Optional[int] = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_shape(self) -> "RetentionPolicy":
        if self.type == "count":
            if self.value is None:
                raise ValueError("retention_policy.value is required when type='count'")
            if self.max_age_days is not None:
                raise ValueError("retention_policy.max_age_days is not allowed when type='count'")
        elif self.type == "age":
            if self.max_age_days is None:
                raise ValueError("retention_policy.max_age_days is required when type='age'")
            if self.value is not None:
                raise ValueError("retention_policy.value is not allowed when type='age'")
        return self


class ScheduleBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, max_length=4096)
    action_type: str = Field(..., max_length=100)
    cron_expression: str = Field(..., max_length=255)
    action_parameters: Dict[str, Any]
    webhook_url: Optional[str] = Field(default=None, max_length=2048)
    retention_policy: Optional[RetentionPolicy] = None
    execution_profile_id: Optional[str] = Field(
        ...,
        description="Execution profile UUID used for delegated execution.",
    )
    enabled: bool = True

    @model_validator(mode="after")
    def validate_execution_profile_id(self) -> "ScheduleBase":
        profile_id = (self.execution_profile_id or "").strip()
        if not profile_id:
            raise ValueError("execution_profile_id is required")
        try:
            UUID(profile_id)
        except Exception as exc:
            raise ValueError("execution_profile_id must be a valid UUID") from exc
        self.execution_profile_id = profile_id
        return self


class ScheduleCreate(ScheduleBase):
    """Payload for creating a schedule."""

    pass


class ScheduleUpdate(BaseModel):
    """Partial update payload for schedules."""

    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, max_length=4096)
    cron_expression: Optional[str] = Field(default=None, max_length=255)
    action_parameters: Optional[Dict[str, Any]] = None
    webhook_url: Optional[str] = Field(default=None, max_length=2048)
    retention_policy: Optional[RetentionPolicy] = None
    execution_profile_id: Optional[str] = Field(
        default=None,
        description="Execution profile UUID used for delegated execution.",
    )
    enabled: Optional[bool] = None

    @model_validator(mode="after")
    def validate_execution_profile_id(self) -> "ScheduleUpdate":
        if self.execution_profile_id is not None:
            profile_id = self.execution_profile_id.strip()
            if not profile_id:
                self.execution_profile_id = None
            else:
                try:
                    UUID(profile_id)
                except Exception as exc:
                    raise ValueError("execution_profile_id must be a valid UUID") from exc
                self.execution_profile_id = profile_id
        return self


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


class ExecutionProfileBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, max_length=4096)
    auth_type: str = Field(default="trust", max_length=32)
    trust_id: Optional[str] = Field(
        default=None,
        description="Keystone trust UUID used for delegated execution.",
    )
    enabled: bool = True

    @model_validator(mode="after")
    def validate_trust_shape(self) -> "ExecutionProfileBase":
        auth_type = (self.auth_type or "").strip().lower()
        if not auth_type:
            raise ValueError("auth_type is required")
        if auth_type != "trust":
            raise ValueError("auth_type must be 'trust'")
        self.auth_type = auth_type
        trust_id = (self.trust_id or "").strip()
        if not trust_id:
            raise ValueError("trust_id is required when auth_type='trust'")
        try:
            UUID(trust_id)
        except Exception as exc:
            raise ValueError("trust_id must be a valid UUID") from exc
        self.trust_id = trust_id
        return self


class ExecutionProfileCreate(ExecutionProfileBase):
    pass


class ExecutionProfileUpdate(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    description: Optional[str] = Field(default=None, max_length=4096)
    auth_type: Optional[str] = Field(default=None, max_length=32)
    trust_id: Optional[str] = Field(
        default=None,
        description="Keystone trust UUID used for delegated execution.",
    )
    enabled: Optional[bool] = None

    @model_validator(mode="after")
    def validate_optional_fields(self) -> "ExecutionProfileUpdate":
        if self.auth_type is not None:
            auth_type = self.auth_type.strip().lower()
            if not auth_type:
                raise ValueError("auth_type cannot be empty")
            if auth_type != "trust":
                raise ValueError("auth_type must be 'trust'")
            self.auth_type = auth_type
        if self.trust_id is not None:
            trust_id = self.trust_id.strip()
            if not trust_id:
                self.trust_id = None
            else:
                try:
                    UUID(trust_id)
                except Exception as exc:
                    raise ValueError("trust_id must be a valid UUID") from exc
                self.trust_id = trust_id
        return self


class ExecutionProfile(ExecutionProfileBase):
    id: str
    project_id: str
    created_by_user_id: Optional[str] = Field(
        default=None,
        description="Keystone user ID of the profile creator (set by the API on create).",
    )
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ExecutionProfileList(BaseModel):
    execution_profiles: List[ExecutionProfile]
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
    worker_id: Optional[str] = None
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    retry_count: int = 0
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class JobFault(BaseModel):
    id: int
    job_id: str
    project_id: str
    occurred_at: datetime
    worker_id: Optional[str] = None
    code: str
    title: str
    detail: str
    error_message: Optional[str] = None
    action_parameters_at_failure: Optional[Dict[str, Any]] = None
    extra: Optional[Dict[str, Any]] = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class JobDetail(Job):
    version: int
    cancelled_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    fault_history: List[JobFault] = Field(default_factory=list)


class JobCreate(BaseModel):
    schedule_id: str = Field(..., description="ID of the schedule to create a job from.")


class JobUpdate(BaseModel):
    status: Optional[JobStatus] = None
    worker_id: Optional[str] = Field(default=None, max_length=255)
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


class VolumeBackupChainItem(BaseModel):
    id: str
    name: Optional[str] = None
    status: str
    is_incremental: bool
    parent_id: Optional[str] = None
    size: Optional[int] = None
    created_at: Optional[datetime] = None
    project_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class VolumeBackupChain(BaseModel):
    volume_id: str
    project_id: str
    has_full_schedule: bool
    has_incremental_schedule: bool
    advisories: List[str] = Field(default_factory=list)
    backups: List[VolumeBackupChainItem] = Field(default_factory=list)
