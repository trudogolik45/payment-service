from typing import Protocol

import httpx

from app.models import Payment
from app.schemas import WebhookPayload


class WebhookDeliveryError(Exception):
    """Webhook не доставлен: сетевая ошибка или ответ не 2xx."""


class Notifier(Protocol):
    async def notify(self, payment: Payment) -> None: ...


class WebhookNotifier:
    def __init__(self, client: httpx.AsyncClient, *, timeout_seconds: float) -> None:
        self._client = client
        self._timeout = timeout_seconds

    async def notify(self, payment: Payment) -> None:
        payload = WebhookPayload.from_model(payment).model_dump(mode="json")
        try:
            response = await self._client.post(
                payment.webhook_url, json=payload, timeout=self._timeout
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"{payment.webhook_url}: {exc}") from exc
