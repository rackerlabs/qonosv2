from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Optional


async def publish_job_queued(
    *,
    redis_url: str,
    stream: str,
    job_id: str,
    action_type: str,
    project_id: str,
    max_len: Optional[int] = None,
) -> None:
    from redis.asyncio import Redis

    redis = Redis.from_url(redis_url, decode_responses=True)
    try:
        fields = {
            "event": "job_queued",
            "job_id": job_id,
            "action_type": action_type,
            "project_id": project_id,
        }
        if max_len is not None:
            await redis.xadd(
                stream,
                fields,
                maxlen=max_len,
                approximate=True,
            )
        else:
            await redis.xadd(stream, fields)
    finally:
        await redis.aclose()


async def consume_job_events(
    *,
    redis_url: str,
    stream: str,
    consumer_group: str,
    consumer_name: str,
    on_job_queued: Callable[[str], Awaitable[None]],
    block_ms: int = 2000,
    count: int = 50,
) -> None:
    from redis.asyncio import Redis
    from redis.exceptions import ResponseError

    redis = Redis.from_url(redis_url, decode_responses=True)
    tasks: set[asyncio.Task[None]] = set()
    sem = asyncio.Semaphore(max(1, count))
    try:
        try:
            await redis.xgroup_create(stream, consumer_group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

        async def handle_message(
            *, stream_name: str, message_id: str, fields: dict[str, Any]
        ) -> None:
            await sem.acquire()
            try:
                if fields.get("event") == "job_queued":
                    job_id = fields.get("job_id")
                    if isinstance(job_id, str) and job_id:
                        await on_job_queued(job_id)
            finally:
                try:
                    await redis.xack(stream_name, consumer_group, message_id)
                finally:
                    sem.release()

        while True:
            data = await redis.xreadgroup(
                groupname=consumer_group,
                consumername=consumer_name,
                streams={stream: ">"},
                count=count,
                block=block_ms,
            )
            if not data:
                continue
            for stream_name, entries in data:
                for message_id, fields in entries:
                    t = asyncio.create_task(
                        handle_message(
                            stream_name=stream_name, message_id=message_id, fields=fields
                        )
                    )
                    tasks.add(t)

                    def _done(tt: asyncio.Task[None]) -> None:
                        tasks.discard(tt)

                    t.add_done_callback(_done)
    finally:
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await redis.aclose()
