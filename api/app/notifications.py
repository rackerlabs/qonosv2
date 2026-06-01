from __future__ import annotations

import asyncio
import json
import logging
from contextlib import suppress
from functools import lru_cache
from typing import Any

import aio_pika
import httpx
from aio_pika.abc import AbstractChannel, AbstractConnection

from .config import AppConfig, get_config
from .utils.time import utc_now

logger = logging.getLogger("qonos.notifications")
_publish_lock = asyncio.Lock()
_bus_publish_lock = asyncio.Lock()
_publish_connection: AbstractConnection | None = None
_publish_channel: AbstractChannel | None = None
_pending_delivery_tasks: set[asyncio.Task[Any]] = set()

JOB_EVENT_TYPES = {
    "job.created",
    "job.started",
    "job.completed",
    "job.failed",
    "job.retrying",
    "job.timed_out",
    "job.cancelled",
}

DEFAULT_DRAIN_TIMEOUT_SECONDS = 30.0


def build_job_event_payload(
    *,
    event_type: str,
    job_id: str,
    schedule_id: str,
    project_id: str,
    action_type: str,
    status: str,
    error_message: str | None = None,
) -> dict[str, Any]:
    if event_type not in JOB_EVENT_TYPES:
        raise ValueError(f"Unsupported job event type: {event_type}")
    return {
        "event_type": event_type,
        "job_id": job_id,
        "schedule_id": schedule_id,
        "project_id": project_id,
        "action_type": action_type,
        "status": status,
        "timestamp": utc_now().isoformat(),
        "error_message": error_message,
    }


def _register_delivery_task(task: asyncio.Task[Any]) -> None:
    _pending_delivery_tasks.add(task)

    def _done(t: asyncio.Task[Any]) -> None:
        _pending_delivery_tasks.discard(t)
        try:
            t.result()
        except asyncio.CancelledError:
            logger.warning("notification delivery cancelled decision=non_fatal")
        except Exception as exc:
            logger.warning("notification delivery failed decision=non_fatal error=%s", str(exc))

    task.add_done_callback(_done)


async def drain_pending_deliveries(
    *, timeout_seconds: float = DEFAULT_DRAIN_TIMEOUT_SECONDS
) -> None:
    pending = [task for task in list(_pending_delivery_tasks) if not task.done()]
    if not pending:
        return

    try:
        await asyncio.wait_for(
            asyncio.gather(*pending, return_exceptions=True),
            timeout=timeout_seconds,
        )
    except asyncio.TimeoutError:
        logger.warning(
            "notification drain timed out decision=shutdown timeout_seconds=%s pending=%s",
            timeout_seconds,
            len(pending),
        )
        for task in pending:
            if not task.done():
                task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await asyncio.gather(*pending, return_exceptions=True)


async def close_notification_publishers() -> None:
    global _publish_connection, _publish_channel

    async with _publish_lock:
        channel = _publish_channel
        connection = _publish_connection
        _publish_channel = None
        _publish_connection = None

    if channel is not None and not channel.is_closed:
        with suppress(Exception):
            await channel.close()
    if connection is not None and not connection.is_closed:
        with suppress(Exception):
            await connection.close()


class JobEventNotifier:
    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg

    def emit_non_blocking(
        self,
        *,
        event_type: str,
        job: Any,
        schedule_webhook_url: str | None = None,
        error_message: str | None = None,
        status: str | None = None,
    ) -> None:
        payload = build_job_event_payload(
            event_type=event_type,
            job_id=str(job.id),
            schedule_id=str(job.schedule_id),
            project_id=str(job.project_id),
            action_type=str(job.action_type),
            status=status or str(getattr(job.status, "value", job.status)),
            error_message=error_message,
        )
        task = asyncio.create_task(
            self._deliver(payload=payload, webhook_url=schedule_webhook_url),
            name=f"qonos-notification-{event_type}",
        )
        _register_delivery_task(task)

    async def _deliver(self, *, payload: dict[str, Any], webhook_url: str | None) -> None:
        coros: list[Any] = []
        if self.cfg.amqp_url:
            coros.append(self._publish_to_bus(payload))
        if webhook_url:
            coros.append(self._publish_to_webhook(payload=payload, webhook_url=webhook_url))
        if not coros:
            return
        results = await asyncio.gather(*coros, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.warning(
                    "notification delivery failed decision=non_fatal error=%s",
                    str(result),
                )

    async def _publish_to_bus(self, payload: dict[str, Any]) -> None:
        amqp_url = self.cfg.amqp_url
        if not amqp_url:
            return
        exchange_name = self.cfg.notification_exchange
        routing_key = self.cfg.notification_routing_key
        try:
            async with _bus_publish_lock:
                channel = await _get_publish_channel(amqp_url)
                exchange = await channel.declare_exchange(
                    exchange_name,
                    aio_pika.ExchangeType.TOPIC,
                    durable=True,
                )
                await exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(payload).encode("utf-8"),
                        content_type="application/json",
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key=routing_key,
                )
        except Exception as exc:
            logger.warning(
                "notification publish failed target=bus decision=non_fatal exchange=%s routing_key=%s error=%s",
                exchange_name,
                routing_key,
                str(exc),
            )
            raise

    async def _publish_to_webhook(self, *, payload: dict[str, Any], webhook_url: str) -> None:
        try:
            async with httpx.AsyncClient(
                timeout=self.cfg.notification_webhook_timeout_seconds
            ) as client:
                resp = await client.post(webhook_url, json=payload)
            if resp.status_code >= 400:
                logger.warning(
                    "notification publish failed target=webhook decision=non_fatal status=%s url=%s",
                    resp.status_code,
                    webhook_url,
                )
        except Exception as exc:
            logger.warning(
                "notification publish failed target=webhook decision=non_fatal url=%s error=%s",
                webhook_url,
                str(exc),
            )
            raise


@lru_cache()
def get_job_event_notifier() -> JobEventNotifier:
    return JobEventNotifier(get_config())


async def _get_publish_channel(amqp_url: str) -> AbstractChannel:
    global _publish_connection, _publish_channel
    async with _publish_lock:
        if _publish_connection is None or _publish_connection.is_closed:
            _publish_connection = await aio_pika.connect_robust(amqp_url)
            _publish_channel = None
        if _publish_channel is None or _publish_channel.is_closed:
            assert _publish_connection is not None
            _publish_channel = await _publish_connection.channel()
        return _publish_channel
