import hashlib
import hmac
import json
import time
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from app.models import Currency, Payment, PaymentStatus
from app.webhook import (
    WebhookDeliveryError,
    WebhookNotifier,
    create_http_client,
    pin_to_address,
    verify,
)
from app.webhook_policy import WebhookUrlPolicy


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


SECRET = "webhook-secret-for-tests"
PUBLIC_IP = "93.184.216.34"


async def _resolver(host: str) -> list[str]:
    return {"client.example": [PUBLIC_IP], "private.example": ["10.0.0.1"]}.get(host, [])


def _notifier(handler: object, allowed_hosts: str = "client.example") -> WebhookNotifier:
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))  # type: ignore[arg-type]
    policy = WebhookUrlPolicy(allowed_hosts, resolver=_resolver)
    return WebhookNotifier(client, secret=SECRET, timeout_seconds=1, policy=policy)


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


async def test_notifier_signs_timestamp_and_body_with_hmac_sha256() -> None:
    received: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received.append(request)
        return httpx.Response(200)

    await _notifier(handler).notify(_payment())

    request = received[0]
    timestamp = request.headers["X-Webhook-Timestamp"]
    signed = f"{timestamp}.".encode() + request.content
    expected = hmac.new(SECRET.encode(), signed, hashlib.sha256).hexdigest()
    assert request.headers["X-Webhook-Signature"] == f"sha256={expected}"
    assert verify(SECRET, timestamp, request.headers["X-Webhook-Signature"], request.content)


def test_verify_rejects_stale_timestamp() -> None:
    body = b"{}"
    old = str(int(time.time()) - 600)
    signature = (
        "sha256=" + hmac.new(SECRET.encode(), f"{old}.".encode() + body, hashlib.sha256).hexdigest()
    )

    assert not verify(SECRET, old, signature, body)


@pytest.mark.parametrize("timestamp", ["not-a-number", "²", "1" * 5000, "", "-1"])
def test_verify_rejects_garbage_timestamp(timestamp: str) -> None:
    assert not verify(SECRET, timestamp, "sha256=00", b"{}")


def test_verify_rejects_non_ascii_signature() -> None:
    assert not verify(SECRET, str(int(time.time())), "sha256=é", b"{}")


def test_pin_keeps_port_in_host_header_and_ipv6_in_brackets() -> None:
    url, headers, extensions = pin_to_address("https://client.example:8443/hook?x=1", "2001:db8::1")

    assert url == "https://[2001:db8::1]:8443/hook?x=1"
    assert headers == {"Host": "client.example:8443"}
    assert extensions == {"sni_hostname": "client.example"}


def test_pin_over_plain_http_has_no_sni() -> None:
    url, headers, extensions = pin_to_address("http://client.example/hook", "93.184.216.34")

    assert url == "http://93.184.216.34/hook"
    assert headers == {"Host": "client.example"}
    assert extensions == {}


async def test_notifier_pins_connection_to_validated_ip() -> None:
    """Против DNS rebinding: соединение идёт на проверенный IP, Host и SNI — исходные."""
    received: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received.append(request)
        return httpx.Response(200)

    await _notifier(handler, allowed_hosts="").notify(_payment())

    request = received[0]
    assert request.url.host == PUBLIC_IP
    assert request.url.path == "/hook"
    assert request.headers["Host"] == "client.example"
    assert request.extensions["sni_hostname"] == "client.example"


async def test_notifier_treats_private_target_as_delivery_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200)

    payment = _payment()
    payment.webhook_url = "https://private.example/hook"

    with pytest.raises(WebhookDeliveryError):
        await _notifier(handler, allowed_hosts="").notify(payment)


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


def test_pin_strips_credentials_from_host_header() -> None:
    _, headers, _ = pin_to_address("https://u:p@client.example/hook", "93.184.216.34")

    assert headers == {"Host": "client.example"}


async def test_http_client_has_no_keepalive_and_ignores_proxy_env() -> None:
    async with create_http_client() as client:
        assert client.trust_env is False
        assert client._transport._pool._max_keepalive_connections == 0  # type: ignore[attr-defined]
