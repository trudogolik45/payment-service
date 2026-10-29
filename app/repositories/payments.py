from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment
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
        stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
        return await self._session.scalar(stmt)

    async def get(self, payment_id: UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_for_update(self, payment_id: UUID) -> Payment | None:
        """Читает Платёж под блокировкой строки, перечитывая его из БД."""
        return await self._session.get(
            Payment, payment_id, with_for_update=True, populate_existing=True
        )
