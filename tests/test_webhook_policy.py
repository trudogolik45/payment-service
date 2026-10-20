import asyncio

import pytest
from app.webhook_policy import WebhookUrlNotAllowedError, WebhookUrlPolicy

RESOLVED = {
    "shop.example.com": ["93.184.216.34"],
    "internal.example.com": ["10.0.0.5"],
    "dual.example.com": ["93.184.216.34", "192.168.1.1"],
}


async def fake_resolver(host: str) -> list[str]:
    return RESOLVED.get(host, [])


def policy(allowed_hosts: str = "") -> WebhookUrlPolicy:
    return WebhookUrlPolicy(allowed_hosts, resolver=fake_resolver)


async def test_public_host_is_allowed_and_returns_addresses() -> None:
    assert await policy().validate("https://shop.example.com/hook") == ["93.184.216.34"]


async def test_allowlisted_host_returns_no_addresses_to_pin() -> None:
    assert await policy("webhook-sink").validate("http://webhook-sink/hook") == []


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:8080/hook",
        "http://localhost/hook",
        "http://10.1.2.3/hook",
        "http://192.168.0.10/hook",
        "http://169.254.169.254/latest/meta-data",
        "http://[::1]/hook",
        "http://[::ffff:10.0.0.1]/hook",
        "http://[::ffff:0:a00:1]/hook",
        "http://[::a00:1]/hook",
        "http://[64:ff9b::a00:1]/hook",
        "http://[2002:a00:1::1]/hook",
        "http://internal.example.com/hook",
        "http://dual.example.com/hook",
    ],
)
async def test_private_and_loopback_targets_are_rejected(url: str) -> None:
    with pytest.raises(WebhookUrlNotAllowedError):
        await policy().validate(url)


async def test_unresolvable_host_is_rejected() -> None:
    with pytest.raises(WebhookUrlNotAllowedError):
        await policy().validate("http://nope.example.com/hook")


async def test_allowlisted_host_bypasses_network_check() -> None:
    await policy("webhook-sink, internal.example.com").validate("http://webhook-sink:8080/hook")
    await policy("webhook-sink, internal.example.com").validate("http://internal.example.com/x")


async def test_allowlist_is_case_insensitive() -> None:
    await policy("Webhook-Sink.").validate("http://WEBHOOK-SINK:8080/hook")


async def test_slow_dns_is_rejected() -> None:
    async def slow_resolver(host: str) -> list[str]:
        await asyncio.sleep(10)
        return ["93.184.216.34"]

    with pytest.raises(WebhookUrlNotAllowedError):
        await WebhookUrlPolicy("", resolver=slow_resolver, resolve_timeout=0.01).validate(
            "https://slow.example.com/hook"
        )


@pytest.mark.parametrize(
    "url",
    [
        "http://8.8.8.8/hook",
        "http://[2606:4700::1111]/hook",
        "http://[::ffff:8.8.8.8]/hook",
        "http://[64:ff9b::808:808]/hook",
        "http://[2002:808:808::1]/hook",
    ],
)
async def test_public_literal_addresses_are_allowed(url: str) -> None:
    await policy().validate(url)


async def test_ip_literal_is_checked_without_dns() -> None:
    async def exploding_resolver(host: str) -> list[str]:
        raise AssertionError("resolver must not be called for IP literals")

    await WebhookUrlPolicy("", resolver=exploding_resolver).validate("http://8.8.8.8/hook")
