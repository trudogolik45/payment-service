import asyncio
import uuid
from typing import Any

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.conftest import fetch_outbox

URL = "/api/v1/payments"


async def test_create_payment_returns_202_and_pending(
    client: AsyncClient, auth_headers: dict[str, str], payment_body: dict[str, Any]
) -> None:
    response = await client.post(
        URL, json=payment_body, headers={**auth_headers, "Idempotency-Key": "k1"}
    )

    assert response.status_code == 202
    data = response.json()
    assert uuid.UUID(data["payment_id"])
    assert data["status"] == "pending"
    assert data["created_at"]


async def test_create_payment_writes_outbox_event_in_same_transaction(
    client: AsyncClient,
    auth_headers: dict[str, str],
    payment_body: dict[str, Any],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    response = await client.post(
        URL, json=payment_body, headers={**auth_headers, "Idempotency-Key": "k1"}
    )

    events = await fetch_outbox(session_factory)
    assert len(events) == 1
    assert events[0].event_type == "payment.created"
    assert events[0].payload["payment_id"] == response.json()["payment_id"]
    assert events[0].published_at is None


async def test_get_payment_returns_details(
    client: AsyncClient, auth_headers: dict[str, str], payment_body: dict[str, Any]
) -> None:
    created = await client.post(
        URL, json=payment_body, headers={**auth_headers, "Idempotency-Key": "k1"}
    )
    payment_id = created.json()["payment_id"]

    response = await client.get(f"{URL}/{payment_id}", headers=auth_headers)

    assert response.status_code == 200
    data = response.json()
    assert data["payment_id"] == payment_id
    assert data["amount"] == "100.50"
    assert data["currency"] == "RUB"
    assert data["description"] == "Заказ #1"
    assert data["metadata"] == {"order_id": 1}
    assert data["status"] == "pending"
    assert data["webhook_url"] == "https://example.com/hook"
    assert data["processed_at"] is None


async def test_get_unknown_payment_returns_404(
    client: AsyncClient, auth_headers: dict[str, str]
) -> None:
    response = await client.get(f"{URL}/{uuid.uuid4()}", headers=auth_headers)

    assert response.status_code == 404


async def test_same_idempotency_key_returns_same_payment_without_duplicate(
    client: AsyncClient,
    auth_headers: dict[str, str],
    payment_body: dict[str, Any],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    headers = {**auth_headers, "Idempotency-Key": "k1"}

    first = await client.post(URL, json=payment_body, headers=headers)
    second = await client.post(URL, json=payment_body, headers=headers)

    assert second.status_code == 202
    assert second.json()["payment_id"] == first.json()["payment_id"]
    assert len(await fetch_outbox(session_factory)) == 1


async def test_same_idempotency_key_with_different_body_returns_409(
    client: AsyncClient, auth_headers: dict[str, str], payment_body: dict[str, Any]
) -> None:
    headers = {**auth_headers, "Idempotency-Key": "k1"}
    await client.post(URL, json=payment_body, headers=headers)

    response = await client.post(URL, json={**payment_body, "amount": "1.00"}, headers=headers)

    assert response.status_code == 409


async def test_missing_idempotency_key_returns_422(
    client: AsyncClient, auth_headers: dict[str, str], payment_body: dict[str, Any]
) -> None:
    response = await client.post(URL, json=payment_body, headers=auth_headers)

    assert response.status_code == 422


async def test_invalid_body_returns_422(
    client: AsyncClient, auth_headers: dict[str, str], payment_body: dict[str, Any]
) -> None:
    response = await client.post(
        URL,
        json={**payment_body, "currency": "GBP"},
        headers={**auth_headers, "Idempotency-Key": "k1"},
    )

    assert response.status_code == 422


async def test_missing_api_key_returns_401(
    client: AsyncClient, payment_body: dict[str, Any]
) -> None:
    response = await client.post(URL, json=payment_body, headers={"Idempotency-Key": "k1"})

    assert response.status_code == 401


async def test_wrong_api_key_returns_401(client: AsyncClient) -> None:
    response = await client.get(f"{URL}/{uuid.uuid4()}", headers={"X-API-Key": "wrong"})

    assert response.status_code == 401


async def test_health_is_public(client: AsyncClient) -> None:
    response = await client.get("/health")

    assert response.status_code == 200


async def test_concurrent_requests_with_same_key_create_one_payment(
    client: AsyncClient,
    auth_headers: dict[str, str],
    payment_body: dict[str, Any],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    headers = {**auth_headers, "Idempotency-Key": "k1"}

    responses = await asyncio.gather(
        *(client.post(URL, json=payment_body, headers=headers) for _ in range(5))
    )

    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    assert len(await fetch_outbox(session_factory)) == 1


async def test_equivalent_amount_formatting_is_not_a_conflict(
    client: AsyncClient, auth_headers: dict[str, str], payment_body: dict[str, Any]
) -> None:
    headers = {**auth_headers, "Idempotency-Key": "k1"}
    await client.post(URL, json={**payment_body, "amount": "100.5"}, headers=headers)

    response = await client.post(URL, json={**payment_body, "amount": "100.50"}, headers=headers)

    assert response.status_code == 202


async def test_non_ascii_api_key_returns_401(client: AsyncClient) -> None:
    response = await client.get(
        f"{URL}/{uuid.uuid4()}", headers={b"X-API-Key": "clé".encode("latin-1")}
    )

    assert response.status_code == 401


async def test_private_webhook_url_is_rejected(
    client: AsyncClient, auth_headers: dict[str, str], payment_body: dict[str, Any]
) -> None:
    response = await client.post(
        URL,
        json={**payment_body, "webhook_url": "http://10.0.0.1:9/hook"},
        headers={**auth_headers, "Idempotency-Key": "k1"},
    )

    assert response.status_code == 422
    assert "webhook_url" in response.json()["detail"]


async def test_allowlisted_private_host_is_accepted(
    client: AsyncClient, auth_headers: dict[str, str], payment_body: dict[str, Any]
) -> None:
    response = await client.post(
        URL,
        json={**payment_body, "webhook_url": "http://webhook-sink:8080/webhooks"},
        headers={**auth_headers, "Idempotency-Key": "k1"},
    )

    assert response.status_code == 202
