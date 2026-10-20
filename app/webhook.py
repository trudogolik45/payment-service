import hashlib
import hmac
from typing import Protocol

import httpx

from app.models import Payment
from app.schemas import WebhookPayload


class WebhookDeliveryError(Exception):
    """Webhook не доставлен: сетевая ошибка или ответ не 2xx."""


class Notifier(Protocol):
    async def notify(self, payment: Payment) -> None: ...


SIGNATURE_HEADER = "X-Webhook-Signature"


def sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


class WebhookNotifier:
    """POST с JSON-телом и HMAC-SHA256 подписью тела в заголовке X-Webhook-Signature."""

    def __init__(self, client: httpx.AsyncClient, *, secret: str, timeout_seconds: float) -> None:
        self._client = client
        self._secret = secret
        self._timeout = timeout_seconds

    async def notify(self, payment: Payment) -> None:
        body = WebhookPayload.from_model(payment).model_dump_json().encode()
        headers = {"Content-Type": "application/json", SIGNATURE_HEADER: sign(self._secret, body)}
        try:
            response = await self._client.post(
                payment.webhook_url, content=body, headers=headers, timeout=self._timeout
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"{payment.webhook_url}: {exc}") from exc
