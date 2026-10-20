"""Защита от SSRF: webhook нельзя направить во внутреннюю сеть.

Хост из allowlist пропускается без проверки — так демо-приёмник внутри
docker-сети остаётся доступным. Для остальных хост резолвится, все адреса
должны быть публичными; IPv6-обёртки над IPv4 (mapped, 6to4, NAT64)
разворачиваются до IPv4. Проверка выполняется при создании платежа и перед
каждой отправкой, а соединение идёт на проверенный адрес (см. webhook.py) —
это закрывает DNS rebinding.
"""

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable
from urllib.parse import urlsplit

Resolver = Callable[[str], Awaitable[list[str]]]

# IPv6-префиксы, последние 32 бита которых — IPv4-адрес
EMBEDDED_IPV4_PREFIXES = (
    ipaddress.IPv6Network("64:ff9b::/96"),  # NAT64
    ipaddress.IPv6Network("::ffff:0:0:0/96"),  # IPv4-translated (SIIT)
    ipaddress.IPv6Network("::/96"),  # IPv4-compatible (устаревший)
)


class WebhookUrlNotAllowedError(Exception):
    """Адрес webhook указывает на внутреннюю сеть или не резолвится."""


async def resolve_host(host: str) -> list[str]:
    try:
        records = await asyncio.get_running_loop().getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except socket.gaierror:
        return []
    return [str(record[4][0]) for record in records]


def _unwrap_ipv4(
    ip: ipaddress.IPv4Address | ipaddress.IPv6Address,
) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    if isinstance(ip, ipaddress.IPv4Address):
        return ip
    if ip.ipv4_mapped is not None:
        return ip.ipv4_mapped
    if ip.sixtofour is not None:
        return ip.sixtofour
    if any(ip in prefix for prefix in EMBEDDED_IPV4_PREFIXES):
        return ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF)
    return ip


def _is_internal(address: str) -> bool:
    return not _unwrap_ipv4(ipaddress.ip_address(address)).is_global


def _normalize_host(host: str) -> str:
    return host.strip().lower().rstrip(".")


def _is_ip_literal(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return False
    return True


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

    async def validate(self, url: str) -> list[str]:
        """Возвращает проверенные адреса хоста; пустой список — хост из allowlist."""
        host = _normalize_host(urlsplit(url).hostname or "")
        if host in self._allowed_hosts:
            return []
        addresses = [host] if _is_ip_literal(host) else await self._resolve_with_timeout(host)
        if not addresses:
            raise WebhookUrlNotAllowedError(f"webhook_url host {host!r} does not resolve")
        if any(_is_internal(address) for address in addresses):
            raise WebhookUrlNotAllowedError(f"webhook_url host {host!r} is not a public address")
        return addresses

    async def _resolve_with_timeout(self, host: str) -> list[str]:
        try:
            return await asyncio.wait_for(self._resolve(host), self._resolve_timeout)
        except TimeoutError:
            raise WebhookUrlNotAllowedError(f"webhook_url host {host!r}: DNS timeout") from None
