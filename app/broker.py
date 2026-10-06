"""Настройка RabbitMQ: брокер, очереди и топология Retry / Dead Letter Queue.

Схема прохождения сообщения:

    outbox relay ──publish──▶ [payments.new] ──▶ consumer
                                                     │
                                       ошибка, попытка < MAX_RETRIES
                                                     ▼
                                   publish(expiration=backoff) в [payments.retry]
                                                     │  TTL сообщения истёк
                                                     ▼  (x-dead-letter-exchange)
                                              обратно в [payments.new]
                                                     │
                                       ошибка, попытка == MAX_RETRIES
                                                     ▼
                                              [payments.dlq]

Экспоненциальный бэкофф достигается per-message TTL (expiration) при публикации
в retry-очередь: когда срок жизни сообщения истекает, RabbitMQ по механизму
dead-lettering возвращает его в основную очередь payments.new.
"""

from faststream.rabbit import RabbitBroker, RabbitQueue

from app.config import settings

broker = RabbitBroker(settings.rabbitmq_url)

# Основная очередь — сюда публикует outbox relay и сюда возвращаются
# сообщения из retry-очереди после истечения TTL.
PAYMENTS_QUEUE = RabbitQueue("payments.new", durable=True)

# Retry-очередь без потребителя: сообщения лежат здесь до истечения per-message
# expiration, после чего dead-letter возвращает их в payments.new.
RETRY_QUEUE = RabbitQueue(
    "payments.retry",
    durable=True,
    arguments={
        "x-dead-letter-exchange": "",  # default exchange
        "x-dead-letter-routing-key": PAYMENTS_QUEUE.name,
    },
)

# Dead Letter Queue — окончательно не обработанные сообщения.
DLQ_QUEUE = RabbitQueue("payments.dlq", durable=True)


async def declare_topology() -> None:
    """Идемпотентно объявляет все очереди. Вызывается и в API, и в consumer,
    чтобы publish не зависел от того, кто стартовал первым."""
    await broker.declare_queue(PAYMENTS_QUEUE)
    await broker.declare_queue(RETRY_QUEUE)
    await broker.declare_queue(DLQ_QUEUE)
