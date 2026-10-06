import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app import service
from app.db import get_session
from app.schemas import PaymentCreate, PaymentCreateResponse, PaymentResponse
from app.security import require_api_key

router = APIRouter(
    prefix="/api/v1",
    dependencies=[Depends(require_api_key)],
)


@router.post(
    "/payments",
    response_model=PaymentCreateResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Создать платёж",
)
async def create_payment(
    data: PaymentCreate,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    session: AsyncSession = Depends(get_session),
) -> PaymentCreateResponse:
    payment = await service.create_payment(session, idempotency_key, data)
    return PaymentCreateResponse(
        payment_id=payment.id,
        status=payment.status,
        created_at=payment.created_at,
    )


@router.get(
    "/payments/{payment_id}",
    response_model=PaymentResponse,
    summary="Получить платёж",
)
async def get_payment(
    payment_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
) -> PaymentResponse:
    payment = await service.get_payment(session, payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found"
        )
    return PaymentResponse.model_validate(payment)
