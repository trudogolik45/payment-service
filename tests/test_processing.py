import asyncio
from uuid import UUID, uuid4

import pytest
from app.models import Payment, PaymentStatus
from app.services.processing import PaymentNotFoundError, PaymentProcessor
from app.webhook import WebhookDeliveryError
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from tests.conftest import fetch_payment, insert_payment


class StubGateway:
    def __init__(self, result: PaymentStatus, delay: float = 0) -> None:
        self.result = result
        self.delay = delay
        self.calls = 0
        self.keys: list[UUID] = []

    async def charge(self, idempotency_key: UUID) -> PaymentStatus:
        self.calls += 1
        self.keys.append(idempotency_key)
        await asyncio.sleep(self.delay)
        return self.result


class StubNotifier:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.notified: list[Payment] = []

    async def notify(self, payment: Payment) -> None:
        if self.fail:
            raise WebhookDeliveryError("down")
        self.notified.append(payment)


class RowLockProbe:
    """Пробует взять строку Платежа через FOR UPDATE NOWAIT из другой сессии."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self.errors: list[str] = []
        self.idle_connections: list[int] = []

    async def __call__(self, payment_id: UUID) -> None:
        engine = self._session_factory.kw["bind"]
        assert isinstance(engine, AsyncEngine)
        self.idle_connections.append(engine.pool.checkedout())  # type: ignore[attr-defined]
        stmt = select(Payment).where(Payment.id == payment_id).with_for_update(nowait=True)
        try:
            async with self._session_factory() as session, session.begin():
                await session.scalar(stmt)
        except DBAPIError as exc:
            self.errors.append(str(exc.orig))


class ProbingGateway(StubGateway):
    def __init__(self, probe: RowLockProbe) -> None:
        super().__init__(PaymentStatus.SUCCEEDED)
        self._probe = probe

    async def charge(self, idempotency_key: UUID) -> PaymentStatus:
        await self._probe(idempotency_key)
        return await super().charge(idempotency_key)


class ProbingNotifier(StubNotifier):
    def __init__(self, probe: RowLockProbe) -> None:
        super().__init__()
        self._probe = probe

    async def notify(self, payment: Payment) -> None:
        await self._probe(payment.id)
        await super().notify(payment)


async def test_payment_row_stays_unlocked_during_charge_and_notification(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await insert_payment(session_factory)
    probe = RowLockProbe(session_factory)
    processor = PaymentProcessor(session_factory, ProbingGateway(probe), ProbingNotifier(probe))

    await processor.process(payment.id)

    assert probe.errors == []
    assert probe.idle_connections == [0, 0]


async def test_processes_pending_payment_and_notifies(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await insert_payment(session_factory)
    gateway, notifier = StubGateway(PaymentStatus.SUCCEEDED), StubNotifier()

    await PaymentProcessor(session_factory, gateway, notifier).process(payment.id)

    stored = await fetch_payment(session_factory, payment)
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.processed_at is not None
    assert stored.webhook_delivered_at is not None
    assert [p.id for p in notifier.notified] == [payment.id]
    assert gateway.keys == [payment.id]


async def test_stores_failed_status_from_gateway(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await insert_payment(session_factory)

    await PaymentProcessor(
        session_factory, StubGateway(PaymentStatus.FAILED), StubNotifier()
    ).process(payment.id)

    stored = await fetch_payment(session_factory, payment)
    assert stored.status is PaymentStatus.FAILED
    assert stored.processed_at is not None


async def test_webhook_failure_keeps_status_but_raises(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await insert_payment(session_factory)
    processor = PaymentProcessor(
        session_factory, StubGateway(PaymentStatus.SUCCEEDED), StubNotifier(fail=True)
    )

    with pytest.raises(WebhookDeliveryError):
        await processor.process(payment.id)

    stored = await fetch_payment(session_factory, payment)
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.webhook_delivered_at is None


async def test_redelivery_skips_gateway_and_only_resends_webhook(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await insert_payment(session_factory)
    gateway = StubGateway(PaymentStatus.SUCCEEDED)
    failing = PaymentProcessor(session_factory, gateway, StubNotifier(fail=True))
    with pytest.raises(WebhookDeliveryError):
        await failing.process(payment.id)
    notifier = StubNotifier()

    await PaymentProcessor(session_factory, gateway, notifier).process(payment.id)

    assert gateway.calls == 1
    assert len(notifier.notified) == 1
    assert (await fetch_payment(session_factory, payment)).webhook_delivered_at is not None


async def test_already_delivered_payment_is_noop(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await insert_payment(session_factory)
    gateway, notifier = StubGateway(PaymentStatus.SUCCEEDED), StubNotifier()
    processor = PaymentProcessor(session_factory, gateway, notifier)
    await processor.process(payment.id)

    await processor.process(payment.id)

    assert gateway.calls == 1
    assert len(notifier.notified) == 1


async def test_unknown_payment_raises_not_found(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    processor = PaymentProcessor(
        session_factory, StubGateway(PaymentStatus.SUCCEEDED), StubNotifier()
    )

    with pytest.raises(PaymentNotFoundError):
        await processor.process(uuid4())


class SequenceGateway(StubGateway):
    """Отвечает по очереди заданными результатами с заданными задержками."""

    def __init__(self, *responses: tuple[PaymentStatus, float]) -> None:
        super().__init__(PaymentStatus.SUCCEEDED)
        self._responses = list(responses)

    async def charge(self, idempotency_key: UUID) -> PaymentStatus:
        self.calls += 1
        self.keys.append(idempotency_key)
        result, delay = self._responses.pop(0)
        await asyncio.sleep(delay)
        return result


async def test_concurrent_deliveries_keep_first_charge_result(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await insert_payment(session_factory)
    gateway = SequenceGateway((PaymentStatus.SUCCEEDED, 0.1), (PaymentStatus.FAILED, 0.3))
    notifier = StubNotifier()
    processor = PaymentProcessor(session_factory, gateway, notifier)

    await asyncio.gather(processor.process(payment.id), processor.process(payment.id))

    stored = await fetch_payment(session_factory, payment)
    assert gateway.keys == [payment.id, payment.id]
    assert stored.status is PaymentStatus.SUCCEEDED
    assert notifier.notified
    assert {(p.status, p.processed_at) for p in notifier.notified} == {
        (PaymentStatus.SUCCEEDED, stored.processed_at)
    }
