import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.gateway import PaymentGateway
from app.models import Payment, PaymentStatus
from app.repositories.payments import PaymentRepository
from app.webhook import Notifier

log = logging.getLogger(__name__)

Step = Callable[[Payment], Awaitable[None]]


class PaymentNotFoundError(Exception):
    """Платёж из сообщения отсутствует в БД — повторять обработку бессмысленно."""


class PaymentProcessor:
    """Один обработчик платежа: шлюз → статус в БД → webhook.

    Идемпотентен: при повторной доставке уже обработанный платёж не проводится
    через шлюз заново, а уже доставленный webhook не отправляется повторно.

    Каждый шаг выполняется под блокировкой строки (SELECT ... FOR UPDATE), поэтому
    две одновременные доставки одного события выполняются последовательно, и вторая
    видит результат первой. Объект перечитывается из БД на каждом шаге
    (populate_existing): между шагами его могла изменить параллельная доставка.
    Блокировка снимается при падении процесса — в отличие от флага «в обработке»
    в БД. Цена: транзакция живёт, пока отвечает шлюз или webhook, поэтому пул
    соединений consumer'а не меньше prefetch.
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
            await self._locked_step(session, payment_id, self._charge_if_pending)
            await self._locked_step(session, payment_id, self._notify_if_not_delivered)

    async def _locked_step(self, session: AsyncSession, payment_id: UUID, step: Step) -> None:
        async with session.begin():
            payment = await PaymentRepository(session).get_for_update(payment_id)
            if payment is None:
                raise PaymentNotFoundError(payment_id)
            await step(payment)

    async def _charge_if_pending(self, payment: Payment) -> None:
        if payment.status is not PaymentStatus.PENDING:
            return
        payment.status = await self._gateway.charge(payment.id)
        payment.processed_at = datetime.now(UTC)
        log.info("payment %s processed: %s", payment.id, payment.status.value)

    async def _notify_if_not_delivered(self, payment: Payment) -> None:
        if payment.webhook_delivered_at is not None:
            return
        await self._notifier.notify(payment)
        payment.webhook_delivered_at = datetime.now(UTC)
        log.info("payment %s webhook delivered to %s", payment.id, payment.webhook_url)
