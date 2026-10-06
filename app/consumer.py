"""FastStream consumer — один обработчик, делающий всё:

1. читает сообщение из payments.new;
2. эмулирует обработку платежа (2-5 сек, 90% success / 10% failed);
3. обновляет статус платежа в БД;
4. отправляет webhook-уведомление;
5. при ошибке — повторяет с экспоненциальной задержкой, после MAX_RETRIES
   попыток отправляет сообщение в Dead Letter Queue.

Обработчик идемпотентен: повторная доставка не выполняет эмуляцию второй раз
(статус уже терминальный) и не шлёт webhook повторно, если он уже доставлен.
"""

import asyncio
import logging
import random
import uuid
from datetime import datetime, timezone

from faststream import FastStream
from faststream.rabbit import RabbitMessage

from app.broker import (
    DLQ_QUEUE,
    PAYMENTS_QUEUE,
    RETRY_QUEUE,
    broker,
    declare_topology,
)
from app.config import settings
from app.db import async_session_factory
from app.enums import PaymentStatus
from app.models import Payment
from app.webhook import send_webhook

logger = logging.getLogger("consumer")

app = FastStream(broker)


@app.after_startup
async def on_startup() -> None:
    # after_startup гарантирует, что брокер уже подключён.
    # Consumer-владелец топологии: объявляет основную, retry- и DLQ-очереди.
    await declare_topology()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def process_payment(payment_id: uuid.UUID) -> None:
    """Выполняет обработку платежа. Идемпотентна по статусу и признаку webhook."""
    async with async_session_factory() as session:
        payment = await session.get(Payment, payment_id)
        if payment is None:
            logger.warning("Платёж %s не найден, пропускаем", payment_id)
            return

        # Шаг эмуляции выполняется только для платежей в статусе pending.
        if payment.status == PaymentStatus.PENDING:
            delay = random.uniform(
                settings.process_min_seconds, settings.process_max_seconds
            )
            await asyncio.sleep(delay)

            succeeded = random.random() < settings.success_rate
            payment.status = (
                PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
            )
            payment.processed_at = _utcnow()
            await session.commit()
            await session.refresh(payment)
            logger.info(
                "Платёж %s обработан за %.1fs -> %s",
                payment_id,
                delay,
                payment.status,
            )

        # Доставляем webhook, если ещё не доставлен.
        if not payment.webhook_delivered:
            await send_webhook(payment)  # бросит исключение при ошибке
            payment.webhook_delivered = True
            await session.commit()


async def _schedule_retry_or_dlq(
    payload: dict, retry: int, error: Exception
) -> None:
    attempt = retry + 1  # номер текущей попытки, 1-based
    if attempt < settings.max_retries:
        delay = settings.retry_base_delay * (2**retry)
        logger.warning(
            "Попытка %d/%d не удалась (%s). Повтор через %.0fs",
            attempt,
            settings.max_retries,
            error,
            delay,
        )
        await broker.publish(
            payload,
            queue=RETRY_QUEUE,
            headers={"x-retry-count": retry + 1},
            expiration=delay,  # per-message TTL в секундах
        )
    else:
        logger.error(
            "Попытка %d/%d не удалась (%s). Сообщение отправлено в DLQ",
            attempt,
            settings.max_retries,
            error,
        )
        await broker.publish(
            payload,
            queue=DLQ_QUEUE,
            headers={
                "x-retry-count": retry,
                "x-error": str(error)[:500],
            },
        )


@broker.subscriber(PAYMENTS_QUEUE)
async def consume(body: dict, msg: RabbitMessage) -> None:
    retry = int(msg.headers.get("x-retry-count", 0) or 0)
    payment_id = uuid.UUID(body["payment_id"])

    try:
        await process_payment(payment_id)
    except Exception as error:  # noqa: BLE001 - обрабатываем все ошибки через retry/DLQ
        await _schedule_retry_or_dlq(body, retry, error)
    # Явного ack не требуется: при нормальном возврате FastStream подтвердит
    # сообщение. Все ошибки перехвачены и переведены в retry/DLQ.
