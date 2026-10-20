import hashlib
import json
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent, Payment
from app.schemas import PAYMENT_CREATED, PaymentCreate, PaymentCreatedEvent
from app.webhook_policy import WebhookUrlPolicy


class IdempotencyConflictError(Exception):
    """Idempotency-Key уже использован с другим телом запроса."""


def request_fingerprint(data: PaymentCreate) -> str:
    canonical = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class PaymentService:
    def __init__(self, session: AsyncSession, webhook_policy: WebhookUrlPolicy) -> None:
        self._session = session
        self._webhook_policy = webhook_policy

    async def create(self, data: PaymentCreate, idempotency_key: str) -> Payment:
        """Создаёт платёж и событие outbox одной транзакцией.

        Повторный запрос с тем же ключом возвращает уже созданный платёж;
        тот же ключ с другим телом — ошибка. Webhook во внутреннюю сеть — ошибка.
        """
        await self._webhook_policy.validate(str(data.webhook_url))
        fingerprint = request_fingerprint(data)
        async with self._session.begin():
            payment = await self._insert_if_absent(data, idempotency_key, fingerprint)
            if payment is not None:
                self._session.add(
                    OutboxEvent(
                        event_type=PAYMENT_CREATED,
                        payload=PaymentCreatedEvent.from_model(payment).model_dump(mode="json"),
                    )
                )
                return payment

        existing = await self._get_by_idempotency_key(idempotency_key)
        if existing.request_hash != fingerprint:
            raise IdempotencyConflictError(idempotency_key)
        return existing

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def _insert_if_absent(
        self, data: PaymentCreate, idempotency_key: str, fingerprint: str
    ) -> Payment | None:
        stmt = (
            insert(Payment)
            .values(
                amount=data.amount,
                currency=data.currency,
                description=data.description,
                metadata_=data.metadata,
                idempotency_key=idempotency_key,
                request_hash=fingerprint,
                webhook_url=str(data.webhook_url),
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        return await self._session.scalar(stmt)

    async def _get_by_idempotency_key(self, idempotency_key: str) -> Payment:
        stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
        return (await self._session.scalars(stmt)).one()
