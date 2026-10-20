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

import aio_pika
import httpx
import pytest
import uvicorn
from app.broker import (
    ATTEMPT_HEADER,
    DEAD_LETTER_QUEUE,
    DELIVERY_LIMIT,
    NEW_PAYMENTS_QUEUE,
    PAYMENTS_EXCHANGE,
    declare_topology,
)
from app.config import Settings
from app.consumer import build_consumer
from app.models import PaymentStatus
from faststream import TestApp
from faststream.rabbit import RabbitBroker, TestRabbitBroker
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from tools import webhook_sink

from tests.conftest import TEST_DATABASE_URL, base_settings, fetch_payment, insert_payment

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
    base = base_settings()
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
                "database_url": TEST_DATABASE_URL,
                "retry_base_delay_seconds": RETRY_BASE_DELAY,
                "gateway_success_rate": 1,
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


@pytest.fixture
async def webhook_sink_url(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[str]:
    """Демо-приёмник webhook'ов, поднятый в процессе тестов на свободном порту."""
    monkeypatch.setenv("WEBHOOK_SECRET", base_settings().webhook_secret)
    config = uvicorn.Config(webhook_sink.app, host="127.0.0.1", port=0, log_level="warning")
    server = uvicorn.Server(config)
    serving = asyncio.create_task(server.serve())
    await _wait_until(lambda: server.started)
    port = server.servers[0].sockets[0].getsockname()[1]
    webhook_sink.received.clear()
    yield f"http://127.0.0.1:{port}/webhooks"
    server.should_exit = True
    await serving


async def test_consumer_starts_and_processes_payment_end_to_end(
    integration_settings: Settings,
    session_factory: async_sessionmaker[AsyncSession],
    webhook_sink_url: str,
) -> None:
    """Хуки старта consumer'а, БД, шлюз и webhook — без подмен."""
    payment = await insert_payment(session_factory, webhook_url=webhook_sink_url)
    app = build_consumer(integration_settings)

    async with TestRabbitBroker(app.broker, with_real=True), TestApp(app):
        await _publish_new_payment(app, payment.id)

        await _wait_until(lambda: len(webhook_sink.received) == 1)

    stored = await fetch_payment(session_factory, payment)
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.processed_at is not None
    assert stored.webhook_delivered_at is not None
    assert webhook_sink.received[0]["payment_id"] == str(payment.id)
    assert webhook_sink.received[0]["event"] == "payment.succeeded"


async def test_webhook_sink_rejects_bad_signature(webhook_sink_url: str) -> None:
    headers = {"X-Webhook-Timestamp": str(int(time.time())), "X-Webhook-Signature": "sha256=bad"}
    async with httpx.AsyncClient() as client:
        response = await client.post(webhook_sink_url, content=b"{}", headers=headers)

    assert response.status_code == 401
    assert webhook_sink.received == []


async def test_message_is_dead_lettered_after_consumer_crashes_too_many_times(
    integration_settings: Settings,
) -> None:
    """Consumer умирает посреди обработки (обрыв соединения без ack): quorum-очередь
    считает такие доставки и после DELIVERY_LIMIT переносит сообщение в DLQ."""
    broker = RabbitBroker(integration_settings.rabbitmq_url)
    async with broker:
        await declare_topology(broker, integration_settings)
        await broker.publish(
            {"payment_id": str(uuid4())},
            exchange=PAYMENTS_EXCHANGE,
            routing_key=NEW_PAYMENTS_QUEUE.routing_key,
        )
        delivery_counts = [
            await _acquire_and_crash(integration_settings.rabbitmq_url)
            for _ in range(DELIVERY_LIMIT + 1)
        ]
        dead = await _wait_for_message(await broker.declare_queue(DEAD_LETTER_QUEUE))

    assert delivery_counts == [0, *range(1, DELIVERY_LIMIT + 1)]
    assert dead.headers["x-death"][0]["reason"] == "delivery_limit"


async def _acquire_and_crash(url: str) -> int:
    """Берёт сообщение без ack и рвёт соединение. Возвращает x-delivery-count доставки."""
    connection = await aio_pika.connect(url)
    channel = await connection.channel()
    queue = await channel.get_queue(NEW_PAYMENTS_QUEUE.name)
    message = await queue.get(no_ack=False, fail=False, timeout=5)
    assert message is not None
    await connection.close()
    return int(message.headers.get("x-delivery-count", 0))
