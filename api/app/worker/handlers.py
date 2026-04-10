from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Mapping, Optional

from db.src.models import Job as JobModel


class ActionHandler(ABC):
    """Interface implemented by all action handlers."""

    @abstractmethod
    def validate_parameters(self, parameters: Mapping[str, Any]) -> None:
        """Raise on invalid parameters."""

    @abstractmethod
    async def execute(self, job: JobModel) -> Any:
        """Execute the job. Return value is handler-defined."""

    async def on_success(self, job: JobModel) -> None:
        return None

    async def on_failure(self, job: JobModel, error: Exception) -> None:
        return None

    async def on_timeout(self, job: JobModel) -> None:
        return None


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, type[ActionHandler]] = {}

    def register(self, action_type: str, handler_cls: type[ActionHandler]) -> None:
        self._handlers[action_type] = handler_cls

    def get(self, action_type: str) -> Optional[type[ActionHandler]]:
        return self._handlers.get(action_type)

    def supported_action_types(self) -> list[str]:
        return sorted(self._handlers.keys())
