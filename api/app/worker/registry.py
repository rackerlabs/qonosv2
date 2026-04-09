from __future__ import annotations

from importlib import metadata
from typing import Callable, Optional

from .handlers import ActionHandler, HandlerRegistry

ENTRYPOINT_GROUP = "qonos.action_handlers"


def load_entrypoint_handlers(registry: HandlerRegistry) -> None:
    """Load ActionHandler classes from entry points."""
    try:
        eps = metadata.entry_points()
        group = eps.select(group=ENTRYPOINT_GROUP)
    except Exception:
        return

    for ep in group:
        try:
            obj = ep.load()
        except Exception:
            continue
        if isinstance(obj, type) and issubclass(obj, ActionHandler):
            registry.register(ep.name, obj)


def register_handler(
    registry: HandlerRegistry, *, action_type: str
) -> Callable[[type[ActionHandler]], type[ActionHandler]]:
    def deco(cls: type[ActionHandler]) -> type[ActionHandler]:
        registry.register(action_type, cls)
        return cls

    return deco


def build_registry(*, extra_registry: Optional[HandlerRegistry] = None) -> HandlerRegistry:
    """Create a registry and load entry-point handlers."""
    registry = extra_registry or HandlerRegistry()
    load_entrypoint_handlers(registry)
    return registry

