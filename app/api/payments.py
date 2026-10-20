from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status

from app.auth import require_api_key
from app.dependencies import get_payment_service
from app.schemas import PaymentAccepted, PaymentCreate, PaymentOut
from app.services.payments import IdempotencyConflictError, PaymentService

router = APIRouter(
    prefix="/api/v1/payments", tags=["payments"], dependencies=[Depends(require_api_key)]
)

IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Уникальный ключ запроса для защиты от дублей",
    ),
]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={409: {"description": "Idempotency-Key уже использован с другим телом"}},
)
async def create_payment(
    body: PaymentCreate,
    idempotency_key: IdempotencyKey,
    service: Annotated[PaymentService, Depends(get_payment_service)],
) -> PaymentAccepted:
    try:
        payment = await service.create(body, idempotency_key)
    except IdempotencyConflictError:
        raise HTTPException(
            status.HTTP_409_CONFLICT, "Idempotency-Key already used with a different payload"
        ) from None
    return PaymentAccepted.from_model(payment)


@router.get("/{payment_id}", response_model=PaymentOut)
async def get_payment(
    payment_id: UUID, service: Annotated[PaymentService, Depends(get_payment_service)]
) -> PaymentOut:
    payment = await service.get(payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Payment not found")
    return PaymentOut.from_model(payment)
