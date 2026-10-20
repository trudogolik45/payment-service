from typing import Any

import pytest
from app.models import OutboxEvent
from app.outbox_relay import OutboxRelay
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tests.conftest import fetch_outbox


async def _add_events(session_factory: async_sessionmaker[AsyncSession], count: int) -> None:
    async with session_factory() as session:
        for i in range(count):
            session.add(OutboxEvent(event_type="payment.created", payload={"payment_id": str(i)}))
        await session.commit()


async def test_relay_publishes_pending_events_and_marks_them(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _add_events(session_factory, 2)
    published: list[dict[str, Any]] = []

    async def publisher(event: OutboxEvent) -> None:
        published.append(event.payload)

    relay = OutboxRelay(session_factory, publisher, batch_size=10, poll_interval_seconds=0)

    count = await relay.relay_once()

    assert count == 2
    assert [p["payment_id"] for p in published] == ["0", "1"]
    assert all(e.published_at is not None for e in await fetch_outbox(session_factory))


async def test_relay_does_not_republish_already_published(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _add_events(session_factory, 1)
    calls = 0

    async def publisher(event: OutboxEvent) -> None:
        nonlocal calls
        calls += 1

    relay = OutboxRelay(session_factory, publisher, batch_size=10, poll_interval_seconds=0)
    await relay.relay_once()

    count = await relay.relay_once()

    assert count == 0
    assert calls == 1


async def test_relay_keeps_event_unpublished_when_publish_fails(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _add_events(session_factory, 1)

    async def publisher(event: OutboxEvent) -> None:
        raise ConnectionError("broker down")

    relay = OutboxRelay(session_factory, publisher, batch_size=10, poll_interval_seconds=0)

    with pytest.raises(ConnectionError):
        await relay.relay_once()

    events = await fetch_outbox(session_factory)
    assert events[0].published_at is None


async def test_relay_respects_batch_size(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _add_events(session_factory, 3)

    async def publisher(event: OutboxEvent) -> None:
        pass

    relay = OutboxRelay(session_factory, publisher, batch_size=2, poll_interval_seconds=0)

    assert await relay.relay_once() == 2
    assert await relay.relay_once() == 1
