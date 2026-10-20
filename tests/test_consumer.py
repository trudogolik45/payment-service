from collections.abc import AsyncIterator
from typing import Any, cast
from uuid import UUID, uuid4

import pytest
from app.broker import NEW_PAYMENTS_QUEUE, PAYMENTS_EXCHANGE, retry_delay_seconds, retry_queue
from app.config import Settings
from app.consumer import build_consumer, schedule_retry
from app.schemas import PaymentCreatedEvent
from app.services.processing import PaymentNotFoundError
from faststream.exceptions import NackMessage
from faststream.rabbit import RabbitBroker, RabbitMessage, RabbitQueue, TestRabbitBroker


class StubProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.processed: list[UUID] = []

    async def process(self, payment_id: UUID) -> None:
        self.processed.append(payment_id)
        if self.error is not None:
            raise self.error


class Harness:
    def __init__(self, broker: RabbitBroker, processor: StubProcessor) -> None:
        self.broker = broker
        self.processor = processor
        self.retried: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

    async def publish_new_payment(self, payment_id: UUID, attempt: int | None = None) -> None:
        headers = {} if attempt is None else {"x-attempt": attempt}
        await self.broker.publish(
            {"payment_id": str(payment_id)},
            exchange=PAYMENTS_EXCHANGE,
            routing_key=NEW_PAYMENTS_QUEUE.routing_key,
            headers=headers,
        )


def _harness_factory(settings: Settings, processor: StubProcessor) -> Harness:
    app = build_consumer(settings)
    app.context.set_global("processor", processor)
    harness = Harness(app.broker, processor)

    for attempt in range(1, settings.max_attempts):
        _capture_retries(app.broker, retry_queue(attempt, settings), harness)

    return harness


def _capture_retries(broker: RabbitBroker, queue: RabbitQueue, harness: Harness) -> None:
    @broker.subscriber(queue)
    async def capture(body: dict[str, Any], msg: RabbitMessage) -> None:
        harness.retried.append((queue.name, body, dict(msg.headers)))


@pytest.fixture
def make_harness(settings: Settings) -> Any:
    async def _make(processor: StubProcessor) -> AsyncIterator[Harness]:
        harness = _harness_factory(settings, processor)
        async with TestRabbitBroker(harness.broker):
            yield harness

    return _make


def test_retry_delays_grow_exponentially(settings: Settings) -> None:
    assert [retry_delay_seconds(n, settings) for n in (1, 2)] == [2, 4]


def test_retry_queue_has_ttl_and_dead_letters_back_to_main_queue(settings: Settings) -> None:
    queue = retry_queue(2, settings)

    assert queue.name == "payments.new.retry.2"
    assert queue.arguments is not None
    assert queue.arguments["x-message-ttl"] == 4000
    assert queue.arguments["x-dead-letter-exchange"] == PAYMENTS_EXCHANGE.name
    assert queue.arguments["x-dead-letter-routing-key"] == NEW_PAYMENTS_QUEUE.routing_key


async def test_successful_processing_does_not_retry(make_harness: Any) -> None:
    processor = StubProcessor()
    async for harness in make_harness(processor):
        payment_id = uuid4()

        await harness.publish_new_payment(payment_id)

        assert processor.processed == [payment_id]
        assert harness.retried == []


async def test_first_failure_schedules_retry_with_incremented_attempt(make_harness: Any) -> None:
    processor = StubProcessor(error=RuntimeError("webhook down"))
    async for harness in make_harness(processor):
        payment_id = uuid4()

        await harness.publish_new_payment(payment_id)

        assert harness.retried == [
            ("payments.new.retry.1", {"payment_id": str(payment_id)}, {"x-attempt": 2})
        ]


async def test_second_failure_goes_to_next_retry_queue(make_harness: Any) -> None:
    processor = StubProcessor(error=RuntimeError("webhook down"))
    async for harness in make_harness(processor):
        await harness.publish_new_payment(uuid4(), attempt=2)

        assert [name for name, _, _ in harness.retried] == ["payments.new.retry.2"]
        assert harness.retried[0][2] == {"x-attempt": 3}


async def test_last_attempt_failure_rejects_message_for_dlq(make_harness: Any) -> None:
    processor = StubProcessor(error=RuntimeError("webhook down"))
    async for harness in make_harness(processor):
        with pytest.raises(RuntimeError):
            await harness.publish_new_payment(uuid4(), attempt=3)

        assert harness.retried == []


async def test_unknown_payment_is_rejected_without_retry(make_harness: Any) -> None:
    processor = StubProcessor(error=PaymentNotFoundError(uuid4()))
    async for harness in make_harness(processor):
        with pytest.raises(PaymentNotFoundError):
            await harness.publish_new_payment(uuid4())

        assert harness.retried == []


async def test_retry_publish_failure_requeues_instead_of_dead_lettering(
    settings: Settings,
) -> None:
    class BrokenBroker:
        async def publish(self, *args: Any, **kwargs: Any) -> None:
            raise ConnectionError("broker down")

    broker = cast(RabbitBroker, BrokenBroker())
    event = PaymentCreatedEvent(payment_id=uuid4())

    with pytest.raises(NackMessage):
        await schedule_retry(broker, event, attempt=1, settings=settings)
