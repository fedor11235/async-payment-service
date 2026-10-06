import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import router
from app.broker import broker, declare_topology
from app.outbox_relay import relay_loop

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Подключаемся к RabbitMQ и объявляем топологию очередей/DLQ.
    await broker.connect()
    await declare_topology()

    stop_event = asyncio.Event()
    relay_task = asyncio.create_task(relay_loop(stop_event))

    try:
        yield
    finally:
        stop_event.set()
        relay_task.cancel()
        try:
            await relay_task
        except asyncio.CancelledError:
            pass
        await broker.close()


app = FastAPI(
    title="Async Payments Service",
    version="1.0.0",
    description="Асинхронный сервис процессинга платежей (Outbox + RabbitMQ + DLQ)",
    lifespan=lifespan,
)

app.include_router(router)


@app.get("/health", tags=["health"])
async def health() -> dict:
    return {"status": "ok"}
