import uuid

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.enums import PaymentStatus
from app.models import OutboxEvent, Payment
from app.schemas import PaymentCreate

EVENT_PAYMENT_CREATED = "payment.created"


async def get_payment(session: AsyncSession, payment_id: uuid.UUID) -> Payment | None:
    return await session.get(Payment, payment_id)


async def _get_by_idempotency_key(
    session: AsyncSession, idempotency_key: str
) -> Payment | None:
    result = await session.execute(
        select(Payment).where(Payment.idempotency_key == idempotency_key)
    )
    return result.scalar_one_or_none()


async def create_payment(
    session: AsyncSession, idempotency_key: str, data: PaymentCreate
) -> Payment:
    """Создаёт платёж и событие outbox в одной транзакции (Outbox pattern).

    Идемпотентность: при повторном запросе с тем же Idempotency-Key возвращается
    ранее созданный платёж, новый не создаётся.
    """
    existing = await _get_by_idempotency_key(session, idempotency_key)
    if existing is not None:
        return existing

    payment = Payment(
        amount=data.amount,
        currency=data.currency,
        description=data.description,
        payment_metadata=data.metadata,
        status=PaymentStatus.PENDING,
        idempotency_key=idempotency_key,
        webhook_url=data.webhook_url,
    )
    session.add(payment)

    try:
        # flush, чтобы получить сгенерированный id платежа для события.
        await session.flush()

        event = OutboxEvent(
            aggregate_id=payment.id,
            event_type=EVENT_PAYMENT_CREATED,
            payload={"payment_id": str(payment.id)},
        )
        session.add(event)
        await session.commit()
    except IntegrityError:
        # Гонка: параллельный запрос с тем же ключом успел создать платёж.
        await session.rollback()
        existing = await _get_by_idempotency_key(session, idempotency_key)
        if existing is not None:
            return existing
        raise

    await session.refresh(payment)
    return payment
