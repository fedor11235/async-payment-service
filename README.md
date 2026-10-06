# Асинхронный сервис процессинга платежей

Микросервис для асинхронной обработки платежей. Принимает запросы на оплату,
обрабатывает их через эмуляцию внешнего платёжного шлюза и уведомляет клиента о
результате через webhook.

Гарантированная публикация событий реализована через **Outbox pattern**,
доставка в очередь — через **RabbitMQ**, повторная обработка — через
**retry с экспоненциальной задержкой** и **Dead Letter Queue**.

## Стек

- **FastAPI** + **Pydantic v2** — HTTP API
- **SQLAlchemy 2.0** (async) + **asyncpg** — работа с БД
- **PostgreSQL** — хранилище
- **RabbitMQ** + **FastStream** — брокер сообщений
- **Alembic** — миграции
- **Docker** + **docker-compose** — запуск

## Архитектура

```
        POST /payments                            ┌───────────────┐
 client ───────────────▶ API ──┐  одна транзакция │  PostgreSQL   │
                               ├─────────────────▶│ payments +    │
                               │                  │ outbox        │
                               │                  └───────────────┘
                               │                          ▲
                     ┌─────────┴──────────┐               │ UPDATE status,
                     │  outbox relay      │ polling       │ webhook_delivered
                     │ (фоновая задача)   │───────────────┘
                     └─────────┬──────────┘
                               │ publish
                               ▼
                       [ payments.new ] ◀─────────────────┐ dead-letter
                               │                          │ по истечении TTL
                               ▼                          │
                          consumer ──ошибка, попытка<3──▶ [ payments.retry ]
                               │                            (per-message TTL,
                               │ ошибка, попытка==3          экспоненц. бэкофф)
                               ▼
                        [ payments.dlq ]
```

### Поток обработки

1. `POST /api/v1/payments` создаёт платёж в статусе `pending` **и** запись в
   таблице `outbox` — в одной транзакции (Outbox pattern).
2. Фоновый **outbox relay** (внутри процесса API) опрашивает таблицу `outbox`,
   публикует неотправленные события в очередь `payments.new` и проставляет
   `published_at`. Это гарантирует, что событие уедет в брокер, только если
   платёж реально закоммичен, и уедет «хотя бы один раз».
3. **Consumer** читает `payments.new` и делает всё:
   - эмулирует обработку платежа (2–5 сек, 90% `succeeded` / 10% `failed`);
   - обновляет статус в БД;
   - отправляет webhook на `webhook_url`;
   - при ошибке — повторяет с экспоненциальной задержкой, после 3 попыток
     отправляет сообщение в Dead Letter Queue.

### Гарантии и идемпотентность

- **Outbox pattern** — событие публикуется, только если транзакция с платежом
  закоммичена; relay использует `FOR UPDATE SKIP LOCKED`, поэтому несколько
  инстансов API не продублируют публикацию.
- **Idempotency-Key** — повторный `POST` с тем же ключом возвращает уже
  созданный платёж (уникальный индекс в БД + обработка гонки).
- **Идемпотентный consumer** — повторная доставка сообщения не выполняет
  эмуляцию дважды (проверка статуса) и не шлёт webhook повторно (флаг
  `webhook_delivered`).
- **Retry** — 3 попытки, задержка `retry_base_delay * 2**attempt` (по умолчанию
  2с, затем 4с). Экспоненциальный бэкофф достигается per-message TTL в
  retry-очереди: по истечении TTL RabbitMQ возвращает сообщение в `payments.new`
  через механизм dead-lettering.
- **Dead Letter Queue** — `payments.dlq` для сообщений, не обработанных за 3
  попытки.

## Запуск

Нужен только Docker.

```bash
cp .env.example .env        # при необходимости поменяйте API_KEY и пр.
docker compose up --build
```

Поднимутся 4 сервиса:

| Сервис     | Назначение                        | Адрес                     |
|------------|-----------------------------------|---------------------------|
| `api`      | FastAPI (+ outbox relay)          | http://localhost:8000     |
| `consumer` | FastStream обработчик платежей     | —                         |
| `postgres` | PostgreSQL 16                     | localhost:5432            |
| `rabbitmq` | RabbitMQ + management UI          | http://localhost:15672    |

- Swagger UI: http://localhost:8000/docs
- RabbitMQ UI: http://localhost:15672 (логин/пароль `guest` / `guest`)

Миграции Alembic применяются автоматически при старте контейнера `api`.

## Аутентификация

Все эндпоинты требуют заголовок `X-API-Key` со значением из переменной
`API_KEY` (по умолчанию `super-secret-api-key`). Неверный или отсутствующий
ключ — `401`.

## API

### Создание платежа

`POST /api/v1/payments`

Заголовки:
- `X-API-Key: <ключ>` (обязательный)
- `Idempotency-Key: <уникальный ключ>` (обязательный)

Тело:

```json
{
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Подписка Pro",
  "metadata": {"user_id": 42},
  "webhook_url": "https://example.com/webhooks/payments"
}
```

Ответ `202 Accepted`:

```json
{
  "payment_id": "6238b91e-d325-4d78-893f-18cb9c47ce60",
  "status": "pending",
  "created_at": "2026-10-06T08:31:40.732356Z"
}
```

Пример `curl`:

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: super-secret-api-key" \
  -H "Idempotency-Key: order-12345" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Подписка Pro",
        "metadata": {"user_id": 42},
        "webhook_url": "https://example.com/webhooks/payments"
      }'
```

### Получение платежа

`GET /api/v1/payments/{payment_id}`

```bash
curl http://localhost:8000/api/v1/payments/6238b91e-d325-4d78-893f-18cb9c47ce60 \
  -H "X-API-Key: super-secret-api-key"
```

Ответ `200 OK`:

```json
{
  "id": "6238b91e-d325-4d78-893f-18cb9c47ce60",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Подписка Pro",
  "metadata": {"user_id": 42},
  "status": "succeeded",
  "idempotency_key": "order-12345",
  "webhook_url": "https://example.com/webhooks/payments",
  "webhook_delivered": true,
  "created_at": "2026-10-06T08:31:40.732356Z",
  "processed_at": "2026-10-06T08:31:44.997764Z"
}
```

### Формат webhook

Сервис отправляет `POST` на `webhook_url` после обработки:

```json
{
  "event": "payment.processed",
  "payment_id": "6238b91e-d325-4d78-893f-18cb9c47ce60",
  "status": "succeeded",
  "amount": "1500.00",
  "currency": "RUB",
  "processed_at": "2026-10-06T08:31:44.997764+00:00",
  "metadata": {"user_id": 42}
}
```

Для теста можно указать публичный приёмник, например https://webhook.site.

## Проверка retry / DLQ

Укажите заведомо недоступный `webhook_url` — доставка упадёт, сработает retry
(2с → 4с), после 3 попыток сообщение уйдёт в `payments.dlq`:

```bash
curl -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: super-secret-api-key" \
  -H "Idempotency-Key: order-dlq-1" \
  -H "Content-Type: application/json" \
  -d '{"amount":"50.00","currency":"USD","webhook_url":"http://127.0.0.1:9999/down"}'

# логи обработчика
docker compose logs -f consumer

# глубина очередей (в т.ч. DLQ)
docker compose exec rabbitmq rabbitmqctl list_queues name messages
```

## Конфигурация

Все параметры задаются переменными окружения (см. `.env.example`):

| Переменная             | По умолчанию | Описание                                        |
|------------------------|--------------|-------------------------------------------------|
| `API_KEY`              | `super-secret-api-key` | Ключ для `X-API-Key`                  |
| `DATABASE_URL`         | —            | DSN PostgreSQL (`postgresql+asyncpg://…`)        |
| `RABBITMQ_URL`         | —            | URL RabbitMQ (`amqp://…`)                         |
| `PROCESS_MIN_SECONDS`  | `2`          | Мин. время эмуляции обработки                     |
| `PROCESS_MAX_SECONDS`  | `5`          | Макс. время эмуляции обработки                    |
| `SUCCESS_RATE`         | `0.9`        | Доля успешных платежей                            |
| `MAX_RETRIES`          | `3`          | Число попыток до отправки в DLQ                   |
| `RETRY_BASE_DELAY`     | `2`          | Базовая задержка бэкоффа (сек)                    |
| `OUTBOX_POLL_INTERVAL` | `0.5`        | Период опроса таблицы outbox (сек)               |
| `OUTBOX_BATCH_SIZE`    | `100`        | Размер пачки событий за итерацию relay           |

## Структура проекта

```
app/
  config.py        # настройки (pydantic-settings)
  db.py            # async engine, сессии, Base
  enums.py         # Currency, PaymentStatus
  models.py        # ORM-модели Payment, OutboxEvent
  schemas.py       # Pydantic-схемы запросов/ответов
  security.py      # проверка X-API-Key
  service.py       # бизнес-логика создания/получения платежа + запись в outbox
  api.py           # роуты /api/v1/payments
  broker.py        # RabbitMQ: очереди, retry/DLQ топология
  outbox_relay.py  # фоновый publisher из outbox в RabbitMQ
  main.py          # FastAPI-приложение (+ запуск relay)
  consumer.py      # FastStream consumer
  webhook.py       # отправка webhook
alembic/           # миграции
docker-compose.yml
Dockerfile
requirements.txt
```

## Локальный запуск без Docker (опционально)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# поднимите postgres и rabbitmq, пропишите их адреса в .env (host=localhost)
alembic upgrade head
uvicorn app.main:app --reload          # терминал 1: API + relay
faststream run app.consumer:app        # терминал 2: consumer
```
