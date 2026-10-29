from datetime import datetime
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment, PaymentStatus
from app.schemas import PaymentCreate


class PaymentRepository:
    """Запросы к БД по Платежу.

    Транзакцию открывает и закрывает вызывающий сервис: репозиторий её не коммитит,
    поэтому Платёж и Событие outbox пишутся одной транзакцией.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add_if_absent(
        self, data: PaymentCreate, idempotency_key: str, fingerprint: str
    ) -> Payment | None:
        """Создаёт Платёж; None, если Ключ идемпотентности уже занят (ON CONFLICT DO NOTHING)."""
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

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        """Находит Платёж по Ключу идемпотентности."""
        stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
        return await self._session.scalar(stmt)

    async def get(self, payment_id: UUID) -> Payment | None:
        """Находит Платёж по id."""
        return await self._session.get(Payment, payment_id)

    async def save_charge_if_pending(
        self, payment_id: UUID, status: PaymentStatus, processed_at: datetime
    ) -> Payment | None:
        """Записывает результат Списания; None, если Платёж уже не ожидает."""
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=processed_at)
            .returning(Payment)
        )
        return await self._session.scalar(stmt)

    async def mark_delivered_if_undelivered(self, payment_id: UUID, delivered_at: datetime) -> bool:
        """Отмечает доставку Уведомления; False, если отметка уже стоит."""
        stmt = (
            update(Payment)
            .where(Payment.id == payment_id, Payment.webhook_delivered_at.is_(None))
            .values(webhook_delivered_at=delivered_at)
            .returning(Payment.id)
        )
        return await self._session.scalar(stmt) is not None
