"""Фоновый relay таблицы outbox.

Периодически выбирает неопубликованные события (FOR UPDATE SKIP LOCKED, чтобы
несколько инстансов API не публиковали одно и то же событие), публикует их в
очередь payments.new и проставляет published_at. Гарантирует доставку события
"хотя бы один раз": если публикация упала до commit, событие останется
неопубликованным и будет повторено на следующей итерации.
"""

import asyncio
import logging

from sqlalchemy import select

from app.broker import PAYMENTS_QUEUE, broker
from app.config import settings
from app.db import async_session_factory
from app.models import OutboxEvent

logger = logging.getLogger("outbox.relay")


async def _publish_batch() -> int:
    async with async_session_factory() as session:
        async with session.begin():
            result = await session.execute(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.created_at)
                .limit(settings.outbox_batch_size)
                .with_for_update(skip_locked=True)
            )
            events = list(result.scalars().all())

            for event in events:
                await broker.publish(
                    event.payload,
                    queue=PAYMENTS_QUEUE,
                    headers={"x-retry-count": 0},
                )
                event.published_at = _utcnow()

    return len(events)


def _utcnow():
    from datetime import datetime, timezone

    return datetime.now(timezone.utc)


async def relay_loop(stop_event: asyncio.Event) -> None:
    logger.info("Outbox relay запущен (интервал=%ss)", settings.outbox_poll_interval)
    while not stop_event.is_set():
        try:
            published = await _publish_batch()
            if published:
                logger.info("Опубликовано событий из outbox: %d", published)
        except Exception:  # noqa: BLE001 - relay не должен падать целиком
            logger.exception("Ошибка в outbox relay, повтор через интервал")

        try:
            await asyncio.wait_for(
                stop_event.wait(), timeout=settings.outbox_poll_interval
            )
        except asyncio.TimeoutError:
            pass

    logger.info("Outbox relay остановлен")
