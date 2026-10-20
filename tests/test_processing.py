import asyncio
from uuid import UUID, uuid4

import pytest
from app.models import Payment, PaymentStatus
from app.services.processing import PaymentNotFoundError, PaymentProcessor
from app.webhook import WebhookDeliveryError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

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


async def test_concurrent_duplicate_deliveries_process_payment_once(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    payment = await insert_payment(session_factory)
    gateway, notifier = StubGateway(PaymentStatus.SUCCEEDED, delay=0.2), StubNotifier()
    processor = PaymentProcessor(session_factory, gateway, notifier)

    await asyncio.gather(processor.process(payment.id), processor.process(payment.id))

    assert gateway.calls == 1
    assert len(notifier.notified) == 1
