from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractIncomingMessage

logger = logging.getLogger("qonos.messaging.amqp_queue")

_publish_lock = asyncio.Lock()
_publish_connection: AbstractConnection | None = None
_publish_channel: AbstractChannel | None = None


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


async def publish_job_queued(
    *,
    amqp_url: str,
    exchange: str,
    routing_key: str,
    job_id: str,
    action_type: str,
    project_id: str,
) -> None:
    payload = {
        "event": "job_queued",
        "job_id": job_id,
        "action_type": action_type,
        "project_id": project_id,
    }
    channel = await _get_publish_channel(amqp_url)
    declared_exchange = await channel.declare_exchange(
        exchange,
        aio_pika.ExchangeType.TOPIC,
        durable=True,
    )
    await declared_exchange.publish(
        aio_pika.Message(
            body=json.dumps(payload).encode("utf-8"),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=routing_key,
    )


async def consume_job_events(
    *,
    amqp_url: str,
    exchange: str,
    queue_name: str,
    routing_key: str,
    on_job_queued: Callable[[str], Awaitable[None]],
    prefetch_count: int = 50,
) -> None:
    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=max(1, prefetch_count))
        declared_exchange = await channel.declare_exchange(
            exchange,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        queue = await channel.declare_queue(queue_name, durable=True)
        await queue.bind(declared_exchange, routing_key=routing_key)

        tasks: set[asyncio.Task[None]] = set()
        sem = asyncio.Semaphore(max(1, prefetch_count))

        async def _run_one(msg: AbstractIncomingMessage) -> None:
            await sem.acquire()
            try:
                await _handle_message(message=msg, on_job_queued=on_job_queued)
            finally:
                sem.release()

        async with queue.iterator() as iterator:
            async for message in iterator:
                task = asyncio.create_task(_run_one(message))
                tasks.add(task)

                def _done(t: asyncio.Task[None]) -> None:
                    tasks.discard(t)

                task.add_done_callback(_done)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


async def _handle_message(
    *,
    message: AbstractIncomingMessage,
    on_job_queued: Callable[[str], Awaitable[None]],
) -> None:
    try:
        async with message.process(requeue=False, ignore_processed=True):
            payload = json.loads(message.body.decode("utf-8"))
            if payload.get("event") != "job_queued":
                return
            job_id = payload.get("job_id")
            if isinstance(job_id, str) and job_id:
                await on_job_queued(job_id)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.warning(
            "amqp message handling failed decision=non_fatal error=%s",
            str(exc),
        )
