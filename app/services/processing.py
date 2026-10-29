import logging
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.gateway import PaymentGateway
from app.models import Payment, PaymentStatus
from app.repositories.payments import PaymentRepository
from app.webhook import Notifier

log = logging.getLogger(__name__)


class PaymentNotFoundError(Exception):
    """Платёж из сообщения отсутствует в БД — повторять обработку бессмысленно."""


class PaymentProcessor:
    """Обработка Платежа: шлюз → статус в БД → webhook.

    Идемпотентна: уже обработанный Платёж не проводится через шлюз заново, а уже
    доставленное Уведомление не отправляется повторно.

    Пока отвечают шлюз и webhook, ни транзакция, ни соединение с БД не удерживаются,
    строка Платежа не заблокирована. Результат Списания пишется по условию «Платёж
    ещё ожидает», отметка о доставке — по условию «ещё не доставлено». Если
    параллельная доставка успела раньше, Обработка продолжает с тем, что в БД.
    Поэтому Уведомление — «как минимум один раз»: при двух одновременных доставках
    оно может уйти дважды (см. docs/adr/0004-charge-without-row-lock.md).
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
        payment = await self._load(payment_id)
        if payment.status is PaymentStatus.PENDING:
            payment = await self._charge(payment)
        if payment.webhook_delivered_at is None:
            await self._notify(payment)

    async def _load(self, payment_id: UUID) -> Payment:
        async with self._session_factory() as session:
            payment = await PaymentRepository(session).get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment

    async def _charge(self, payment: Payment) -> Payment:
        status = await self._gateway.charge(payment.id)
        async with self._session_factory() as session, session.begin():
            saved = await PaymentRepository(session).save_charge_if_pending(
                payment.id, status, datetime.now(UTC)
            )
        if saved is None:
            log.info("payment %s already processed by a concurrent delivery", payment.id)
            return await self._load(payment.id)
        log.info("payment %s processed: %s", saved.id, saved.status.value)
        return saved

    async def _notify(self, payment: Payment) -> None:
        await self._notifier.notify(payment)
        async with self._session_factory() as session, session.begin():
            marked = await PaymentRepository(session).mark_delivered_if_undelivered(
                payment.id, datetime.now(UTC)
            )
        if not marked:
            log.info("payment %s webhook already marked delivered", payment.id)
            return
        log.info("payment %s webhook delivered to %s", payment.id, payment.webhook_url)
