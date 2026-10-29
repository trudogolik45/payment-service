from datetime import datetime
from typing import Any, cast

from sqlalchemy import CursorResult, delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import OutboxEvent, Payment
from app.schemas import PAYMENT_CREATED, PaymentCreatedEvent


class OutboxRepository:
    """Запросы к БД по Событию outbox.

    Транзакцию открывает и закрывает вызывающий сервис: репозиторий её не коммитит.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add_payment_created(self, payment: Payment) -> None:
        """Записывает Событие outbox о создании Платежа в текущую транзакцию."""
        self._session.add(
            OutboxEvent(
                event_type=PAYMENT_CREATED,
                payload=PaymentCreatedEvent.from_model(payment).model_dump(mode="json"),
            )
        )

    async def lock_pending_batch(self, batch_size: int, max_attempts: int) -> list[OutboxEvent]:
        """Берёт пачку неопубликованных событий под FOR UPDATE SKIP LOCKED, без карантинных."""
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None), OutboxEvent.attempts < max_attempts)
            .order_by(OutboxEvent.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        return list(await self._session.scalars(stmt))

    async def delete_published_before(self, threshold: datetime) -> int:
        """Удаляет опубликованные события старше порога. Возвращает число удалённых."""
        stmt = delete(OutboxEvent).where(OutboxEvent.published_at < threshold)
        result = cast(CursorResult[Any], await self._session.execute(stmt))
        return result.rowcount
