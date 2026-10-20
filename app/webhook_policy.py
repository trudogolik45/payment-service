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


class WebhookUrlPolicy:
    def __init__(self, allowed_hosts: str, *, resolver: Resolver = resolve_host) -> None:
        self._allowed_hosts = {h.strip() for h in allowed_hosts.split(",") if h.strip()}
        self._resolve = resolver

    async def validate(self, url: str) -> None:
        host = urlsplit(url).hostname or ""
        if host in self._allowed_hosts:
            return
        addresses = await self._resolve(host)
        if not addresses:
            raise WebhookUrlNotAllowedError(f"webhook_url host {host!r} does not resolve")
        if any(_is_internal(address) for address in addresses):
            raise WebhookUrlNotAllowedError(f"webhook_url host {host!r} is not a public address")
