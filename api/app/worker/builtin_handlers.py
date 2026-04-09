from __future__ import annotations

from .handlers import HandlerRegistry
from .registry import register_handler
from .server_snapshot_handler import ServerSnapshotActionHandler


def register_builtin_handlers(registry: HandlerRegistry) -> None:
    register_handler(registry, action_type="server_snapshot")(ServerSnapshotActionHandler)
