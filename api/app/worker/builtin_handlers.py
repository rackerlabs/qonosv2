from __future__ import annotations

from typing import Any, Mapping

from db.src.models import Job as JobModel

from .handlers import ActionHandler, HandlerRegistry
from .registry import register_handler


class SnapshotActionHandler(ActionHandler):
    """Built-in placeholder handler for snapshot action type."""

    def validate_parameters(self, parameters: Mapping[str, Any]) -> None:
        if not isinstance(parameters, dict):
            raise ValueError("action_parameters must be an object")

    async def execute(self, job: JobModel) -> Any:
        return {"job_id": job.id, "action_type": job.action_type, "result": "accepted"}


def register_builtin_handlers(registry: HandlerRegistry) -> None:
    register_handler(registry, action_type="snapshot")(SnapshotActionHandler)

