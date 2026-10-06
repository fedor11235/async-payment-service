import logging

import httpx

from app.models import Payment

logger = logging.getLogger("webhook")

WEBHOOK_TIMEOUT = 10.0


def build_webhook_payload(payment: Payment) -> dict:
    return {
        "event": "payment.processed",
        "payment_id": str(payment.id),
        "status": str(payment.status),
        "amount": str(payment.amount),
        "currency": str(payment.currency),
        "processed_at": payment.processed_at.isoformat()
        if payment.processed_at
        else None,
        "metadata": payment.payment_metadata,
    }


async def send_webhook(payment: Payment) -> None:
    """Отправляет webhook. Бросает исключение при ошибке/не-2xx ответе —
    тогда сообщение уйдёт на повторную обработку (retry) и, в итоге, в DLQ."""
    payload = build_webhook_payload(payment)
    async with httpx.AsyncClient(timeout=WEBHOOK_TIMEOUT) as client:
        response = await client.post(payment.webhook_url, json=payload)
        response.raise_for_status()
    logger.info(
        "Webhook доставлен: payment_id=%s status=%s", payment.id, payment.status
    )
