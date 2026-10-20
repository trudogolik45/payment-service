import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from app.models import Currency, Payment, PaymentStatus
from app.webhook import WebhookDeliveryError, WebhookNotifier


def _payment() -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.EUR,
        description="test",
        metadata_={"order_id": 7},
        status=PaymentStatus.SUCCEEDED,
        idempotency_key="k",
        request_hash="h",
        webhook_url="https://client.example/hook",
        created_at=datetime.now(UTC),
        processed_at=datetime.now(UTC),
    )


def _notifier(handler: object) -> WebhookNotifier:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))  # type: ignore[arg-type]
    return WebhookNotifier(client, timeout_seconds=1)


async def test_notifier_posts_payment_result_to_webhook_url() -> None:
    payment = _payment()
    received: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received.append(request)
        return httpx.Response(200)

    await _notifier(handler).notify(payment)

    assert received[0].url == payment.webhook_url
    body = json.loads(received[0].content)
    assert body["event"] == "payment.succeeded"
    assert body["payment_id"] == str(payment.id)
    assert body["status"] == "succeeded"
    assert body["amount"] == "10.00"
    assert body["currency"] == "EUR"
    assert body["metadata"] == {"order_id": 7}


async def test_notifier_raises_on_non_2xx() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    with pytest.raises(WebhookDeliveryError):
        await _notifier(handler).notify(_payment())


async def test_notifier_raises_on_connection_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    with pytest.raises(WebhookDeliveryError):
        await _notifier(handler).notify(_payment())
