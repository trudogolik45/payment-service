import logging
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.gateway import PaymentGateway
from app.models import Payment, PaymentStatus
from app.webhook import Notifier

log = logging.getLogger(__name__)


class PaymentNotFoundError(Exception):
    """Платёж из сообщения отсутствует в БД — повторять обработку бессмысленно."""


class PaymentProcessor:
    """Один обработчик платежа: шлюз → статус в БД → webhook.

    Идемпотентен: при повторной доставке уже обработанный платёж не проводится
    через шлюз заново, а уже доставленный webhook не отправляется повторно.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        notifier: Notifier,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._notifier = notifier

    async def process(self, payment_id: UUID) -> None:
        async with self._session_factory() as session:
            payment = await session.get(Payment, payment_id)
            if payment is None:
                raise PaymentNotFoundError(payment_id)
            if payment.status is PaymentStatus.PENDING:
                await self._charge(session, payment)
            if payment.webhook_delivered_at is None:
                await self._notify(session, payment)

    async def _charge(self, session: AsyncSession, payment: Payment) -> None:
        payment.status = await self._gateway.charge()
        payment.processed_at = datetime.now(UTC)
        await session.commit()
        log.info("payment %s processed: %s", payment.id, payment.status.value)

    async def _notify(self, session: AsyncSession, payment: Payment) -> None:
        await self._notifier.notify(payment)
        payment.webhook_delivered_at = datetime.now(UTC)
        await session.commit()
        log.info("payment %s webhook delivered to %s", payment.id, payment.webhook_url)
