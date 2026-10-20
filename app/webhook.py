import hashlib
import hmac
import time
from typing import Any, Protocol
from urllib.parse import urlsplit, urlunsplit

import httpx

from app.models import Payment
from app.schemas import WebhookPayload
from app.webhook_policy import WebhookUrlNotAllowedError, WebhookUrlPolicy

SIGNATURE_HEADER = "X-Webhook-Signature"
TIMESTAMP_HEADER = "X-Webhook-Timestamp"
SIGNATURE_MAX_AGE_SECONDS = 300


class WebhookDeliveryError(Exception):
    """Webhook не доставлен: адрес не прошёл проверку, сетевая ошибка или ответ не 2xx."""


class Notifier(Protocol):
    async def notify(self, payment: Payment) -> None: ...


def sign(secret: str, timestamp: str, body: bytes) -> str:
    """HMAC-SHA256 от «timestamp.body»: timestamp защищает от повтора перехваченного запроса."""
    digest = hmac.new(secret.encode(), f"{timestamp}.".encode() + body, hashlib.sha256)
    return "sha256=" + digest.hexdigest()


def verify(
    secret: str,
    timestamp: str,
    signature: str,
    body: bytes,
    *,
    max_age_seconds: int = SIGNATURE_MAX_AGE_SECONDS,
) -> bool:
    if not timestamp.isdigit() or abs(time.time() - int(timestamp)) > max_age_seconds:
        return False
    return hmac.compare_digest(signature, sign(secret, timestamp, body))


def pin_to_address(url: str, address: str) -> tuple[str, dict[str, str], dict[str, Any]]:
    """URL на проверенный IP; Host и SNI остаются исходными, чтобы TLS и роутинг не сломались."""
    parts = urlsplit(url)
    host = parts.hostname or ""
    netloc = f"[{address}]" if ":" in address else address
    if parts.port is not None:
        netloc = f"{netloc}:{parts.port}"
    pinned = urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    extensions = {"sni_hostname": host} if parts.scheme == "https" else {}
    return pinned, {"Host": parts.netloc}, extensions


class WebhookNotifier:
    """POST с JSON-телом и HMAC-подписью. Адрес перепроверяется перед каждой отправкой."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        secret: str,
        timeout_seconds: float,
        policy: WebhookUrlPolicy,
    ) -> None:
        self._client = client
        self._secret = secret
        self._timeout = timeout_seconds
        self._policy = policy

    async def notify(self, payment: Payment) -> None:
        body = WebhookPayload.from_model(payment).model_dump_json().encode()
        timestamp = str(int(time.time()))
        headers = {
            "Content-Type": "application/json",
            TIMESTAMP_HEADER: timestamp,
            SIGNATURE_HEADER: sign(self._secret, timestamp, body),
        }
        try:
            url, extra_headers, extensions = await self._target(payment.webhook_url)
            response = await self._client.post(
                url,
                content=body,
                headers={**headers, **extra_headers},
                extensions=extensions,
                timeout=self._timeout,
            )
            response.raise_for_status()
        except (httpx.HTTPError, WebhookUrlNotAllowedError) as exc:
            raise WebhookDeliveryError(f"{payment.webhook_url}: {exc}") from exc

    async def _target(self, url: str) -> tuple[str, dict[str, str], dict[str, Any]]:
        addresses = await self._policy.validate(url)
        if not addresses:
            return url, {}, {}
        return pin_to_address(url, addresses[0])
