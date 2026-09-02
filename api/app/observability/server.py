from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Optional

from fastapi import FastAPI, Response

from .health import HealthExtraProvider, build_health_payload
from .metrics import CONTENT_TYPE_LATEST, generate_metrics

MetricsRefreshCallback = Callable[[], Awaitable[None]]


def build_observability_app(
    *,
    component: str,
    health_extra: Optional[HealthExtraProvider] = None,
    refresh_metrics: Optional[MetricsRefreshCallback] = None,
) -> FastAPI:
    app = FastAPI(title=f"QonoS {component} Observability")

    @app.get("/health")
    async def health() -> dict[str, Any]:
        extra = health_extra() if health_extra is not None else None
        return build_health_payload(component=component, extra=extra)

    @app.get("/metrics")
    async def metrics() -> Response:
        if refresh_metrics is not None:
            await refresh_metrics()
        return Response(content=generate_metrics(), media_type=CONTENT_TYPE_LATEST)

    return app
