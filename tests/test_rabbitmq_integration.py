"""Интеграционные тесты с настоящим RabbitMQ: TTL retry-очередей и DLX эмулятор не умеет.

Каждый прогон работает в отдельном vhost, который создаётся и удаляется через
Management API, поэтому аргументы очередей не конфликтуют с рабочими.
"""

import asyncio
import os
import time
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from app.broker import (
    ATTEMPT_HEADER,
    DEAD_LETTER_QUEUE,
    NEW_PAYMENTS_QUEUE,
    PAYMENTS_EXCHANGE,
    declare_topology,
)
from app.config import Settings
from app.consumer import build_consumer
from faststream.rabbit import TestRabbitBroker

from tests.conftest import API_KEY

MANAGEMENT_URL = os.environ.get("TEST_RABBITMQ_MANAGEMENT_URL", "http://localhost:15672")
RETRY_BASE_DELAY = 0.2


class CountingProcessor:
    def __init__(self, fail: bool) -> None:
        self.fail = fail
        self.calls: list[float] = []

    async def process(self, payment_id: UUID) -> None:
        self.calls.append(time.monotonic())
        if self.fail:
            raise RuntimeError("webhook down")


async def _wait_until(condition: Any) -> None:
    deadline = time.monotonic() + 5
    while not condition():
        if time.monotonic() > deadline:
            pytest.fail("condition not met in time")
        await asyncio.sleep(0.05)


async def _wait_for_message(queue: Any) -> Any:
    """Reject и dead-lettering асинхронны: сообщение появляется в DLQ не мгновенно."""
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        message = await queue.get(no_ack=True, fail=False, timeout=1)
        if message is not None:
            return message
        await asyncio.sleep(0.05)
    pytest.fail("message did not arrive in time")


@pytest.fixture
async def integration_settings() -> AsyncIterator[Settings]:
    base = Settings(api_key=API_KEY)
    vhost = f"test-{uuid4().hex[:8]}"
    async with httpx.AsyncClient(base_url=MANAGEMENT_URL, auth=("guest", "guest")) as api:
        try:
            (await api.put(f"/api/vhosts/{vhost}")).raise_for_status()
        except httpx.HTTPError as exc:
            pytest.fail(
                f"RabbitMQ Management недоступен по {MANAGEMENT_URL}: {exc}. "
                "Поднимите брокер: `docker compose up -d rabbitmq`."
            )
        permissions = {"configure": ".*", "write": ".*", "read": ".*"}
        (await api.put(f"/api/permissions/{vhost}/guest", json=permissions)).raise_for_status()
        yield base.model_copy(
            update={
                "rabbitmq_url": base.rabbitmq_url.rstrip("/") + f"/{vhost}",
                "retry_base_delay_seconds": RETRY_BASE_DELAY,
                "max_attempts": 3,
            }
        )
        (await api.delete(f"/api/vhosts/{vhost}")).raise_for_status()


async def _publish_new_payment(app: Any, payment_id: UUID) -> None:
    await app.broker.publish(
        {"payment_id": str(payment_id)},
        exchange=PAYMENTS_EXCHANGE,
        routing_key=NEW_PAYMENTS_QUEUE.routing_key,
    )


async def test_message_lands_in_dlq_after_three_attempts_with_growing_delays(
    integration_settings: Settings,
) -> None:
    processor = CountingProcessor(fail=True)
    app = build_consumer(integration_settings)
    app.context.set_global("processor", processor)

    async with TestRabbitBroker(app.broker, with_real=True):
        await declare_topology(app.broker, integration_settings)
        payment_id = uuid4()

        await _publish_new_payment(app, payment_id)

        await _wait_until(lambda: len(processor.calls) == 3)
        dead = await _wait_for_message(await app.broker.declare_queue(DEAD_LETTER_QUEUE))

    assert dead.body == b'{"payment_id":"' + str(payment_id).encode() + b'"}'
    assert dead.headers[ATTEMPT_HEADER] == 3
    assert [(d["queue"], d["reason"]) for d in dead.headers["x-death"]] == [
        ("payments.new", "rejected"),
        ("payments.new.retry.2", "expired"),
    ]
    first, second, third = processor.calls
    assert second - first >= RETRY_BASE_DELAY
    assert third - second >= RETRY_BASE_DELAY * 2


async def test_successful_message_is_acked_and_dlq_stays_empty(
    integration_settings: Settings,
) -> None:
    processor = CountingProcessor(fail=False)
    app = build_consumer(integration_settings)
    app.context.set_global("processor", processor)

    async with TestRabbitBroker(app.broker, with_real=True):
        await declare_topology(app.broker, integration_settings)

        await _publish_new_payment(app, uuid4())

        await _wait_until(lambda: len(processor.calls) == 1)
        await asyncio.sleep(RETRY_BASE_DELAY)
        dlq = await app.broker.declare_queue(DEAD_LETTER_QUEUE)
        main = await app.broker.declare_queue(NEW_PAYMENTS_QUEUE)
        assert await dlq.get(no_ack=True, fail=False, timeout=1) is None
        assert await main.get(no_ack=True, fail=False, timeout=1) is None
    assert len(processor.calls) == 1
