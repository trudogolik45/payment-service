"""Защита от SSRF: webhook нельзя направить во внутреннюю сеть.

Хост из allowlist пропускается без проверки — так демо-приёмник внутри
docker-сети остаётся доступным. Проверка выполняется при создании платежа;
от DNS rebinding между созданием и доставкой она не защищает.
"""

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable
from urllib.parse import urlsplit

Resolver = Callable[[str], Awaitable[list[str]]]


class WebhookUrlNotAllowedError(Exception):
    """Адрес webhook указывает на внутреннюю сеть или не резолвится."""


async def resolve_host(host: str) -> list[str]:
    try:
        records = await asyncio.get_running_loop().getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return []
    return [str(record[4][0]) for record in records]


def _is_internal(address: str) -> bool:
    ip = ipaddress.ip_address(address)
    return not ip.is_global


def _normalize_host(host: str) -> str:
    return host.strip().lower().rstrip(".")


class WebhookUrlPolicy:
    def __init__(
        self,
        allowed_hosts: str,
        *,
        resolver: Resolver = resolve_host,
        resolve_timeout: float = 3,
    ) -> None:
        self._allowed_hosts = {_normalize_host(h) for h in allowed_hosts.split(",") if h.strip()}
        self._resolve = resolver
        self._resolve_timeout = resolve_timeout

    async def validate(self, url: str) -> None:
        host = _normalize_host(urlsplit(url).hostname or "")
        if host in self._allowed_hosts:
            return
        try:
            addresses = await asyncio.wait_for(self._resolve(host), self._resolve_timeout)
        except TimeoutError:
            raise WebhookUrlNotAllowedError(
                f"webhook_url host {host!r} resolve timed out"
            ) from None
        if not addresses:
            raise WebhookUrlNotAllowedError(f"webhook_url host {host!r} does not resolve")
        if any(_is_internal(address) for address in addresses):
            raise WebhookUrlNotAllowedError(f"webhook_url host {host!r} is not a public address")
