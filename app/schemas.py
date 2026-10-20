from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, UrlConstraints, field_validator

from app.models import WEBHOOK_URL_MAX_LENGTH, Currency, Payment, PaymentStatus

PAYMENT_CREATED = "payment.created"


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2, examples=["100.50"])
    currency: Currency
    description: str = Field(min_length=1, max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: Annotated[HttpUrl, UrlConstraints(max_length=WEBHOOK_URL_MAX_LENGTH)]

    @field_validator("amount", mode="after")
    @classmethod
    def normalize_amount(cls, amount: Decimal) -> Decimal:
        """100.5 и 100.50 — одна сумма: иначе повтор с другим форматом даст ложный 409."""
        return amount.quantize(Decimal("0.01"))


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_model(cls, payment: Payment) -> "PaymentAccepted":
        return cls(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


class PaymentOut(BaseModel):
    payment_id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None

    @classmethod
    def from_model(cls, payment: Payment) -> "PaymentOut":
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
            webhook_delivered_at=payment.webhook_delivered_at,
        )


class PaymentCreatedEvent(BaseModel):
    """Сообщение в очереди payments.new."""

    payment_id: UUID

    @classmethod
    def from_model(cls, payment: Payment) -> "PaymentCreatedEvent":
        return cls(payment_id=payment.id)


class WebhookPayload(BaseModel):
    event: str
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_model(cls, payment: Payment) -> "WebhookPayload":
        return cls(
            event=f"payment.{payment.status.value}",
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )
