import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from app.enums import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    """Тело запроса на создание платежа."""

    amount: Decimal = Field(gt=0, description="Сумма платежа, больше нуля")
    currency: Currency
    description: str | None = Field(default=None, max_length=1024)
    metadata: dict = Field(default_factory=dict, description="Произвольные метаданные")
    webhook_url: str = Field(description="URL для уведомления о результате")


class PaymentCreateResponse(BaseModel):
    """Ответ 202 Accepted на создание платежа."""

    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentResponse(BaseModel):
    """Детальная информация о платеже."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict = Field(validation_alias="payment_metadata")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    webhook_delivered: bool
    created_at: datetime
    processed_at: datetime | None
