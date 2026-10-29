import hashlib
import json
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment
from app.repositories.outbox import OutboxRepository
from app.repositories.payments import PaymentRepository
from app.schemas import PaymentCreate
from app.webhook_policy import WebhookUrlPolicy


class IdempotencyConflictError(Exception):
    """Idempotency-Key уже использован с другим телом запроса."""


def request_fingerprint(data: PaymentCreate) -> str:
    canonical = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class PaymentService:
    def __init__(self, session: AsyncSession, webhook_policy: WebhookUrlPolicy) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)
        self._webhook_policy = webhook_policy

    async def create(self, data: PaymentCreate, idempotency_key: str) -> Payment:
        """Создаёт платёж и событие outbox одной транзакцией.

        Повторный запрос с тем же ключом возвращает уже созданный платёж;
        тот же ключ с другим телом — ошибка. Webhook во внутреннюю сеть — ошибка
        (проверяется только для нового платежа: повтор не должен зависеть от DNS).
        Гонку двух первых запросов разрешает ON CONFLICT DO NOTHING.
        """
        fingerprint = request_fingerprint(data)
        async with self._session.begin():
            is_new = await self._payments.get_by_idempotency_key(idempotency_key) is None
        if is_new:
            await self._webhook_policy.validate(str(data.webhook_url))
        async with self._session.begin():
            payment = await self._payments.add_if_absent(data, idempotency_key, fingerprint)
            if payment is not None:
                self._outbox.add_payment_created(payment)
                return payment

        existing = await self._payments.get_by_idempotency_key(idempotency_key)
        if existing is None or existing.request_hash != fingerprint:
            raise IdempotencyConflictError(idempotency_key)
        return existing

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self._payments.get(payment_id)
