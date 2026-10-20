"""Outbox relay: переносит события из таблицы outbox в RabbitMQ.

Строки берутся под FOR UPDATE SKIP LOCKED, поэтому можно запускать
несколько экземпляров. Гарантия — at-least-once: если публикация прошла,
а commit нет, событие будет опубликовано повторно; consumer идемпотентен.
"""

import asyncio
import contextlib
import logging
import signal
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from faststream.rabbit import RabbitBroker
from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.broker import NEW_PAYMENTS_QUEUE, PAYMENTS_EXCHANGE, create_broker, declare_topology
from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory
from app.models import OutboxEvent
from app.schemas import PAYMENT_CREATED

log = logging.getLogger(__name__)

Publisher = Callable[[OutboxEvent], Awaitable[None]]

ROUTING_KEYS = {PAYMENT_CREATED: NEW_PAYMENTS_QUEUE.routing_key}


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: Publisher,
        *,
        batch_size: int,
        poll_interval_seconds: float,
    ) -> None:
        self._session_factory = session_factory
        self._publish = publisher
        self._batch_size = batch_size
        self._poll_interval = poll_interval_seconds

    async def relay_once(self) -> int:
        """Публикует одну пачку неопубликованных событий. Возвращает их количество."""
        async with self._session_factory() as session, session.begin():
            events: list[OutboxEvent] = list(await session.scalars(self._pending_batch()))
            for event in events:
                await self._publish(event)
                event.published_at = datetime.now(UTC)
            return len(events)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            published = await self._relay_safely()
            if published < self._batch_size:
                await self._sleep(stop)

    async def _relay_safely(self) -> int:
        try:
            return await self.relay_once()
        except Exception:
            log.exception("outbox relay iteration failed")
            return 0

    async def _sleep(self, stop: asyncio.Event) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=self._poll_interval)

    def _pending_batch(self) -> Select[OutboxEvent]:
        return (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.id)
            .limit(self._batch_size)
            .with_for_update(skip_locked=True)
        )


def rabbit_publisher(broker: RabbitBroker) -> Publisher:
    async def publish(event: OutboxEvent) -> None:
        await broker.publish(
            event.payload,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=ROUTING_KEYS[event.event_type],
            message_id=str(event.id),
            persist=True,
        )

    return publish


async def main(settings: Settings) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    engine = create_engine(settings.database_url)
    broker = create_broker(settings)
    try:
        await broker.connect()
        await declare_topology(broker, settings)
        relay = OutboxRelay(
            create_session_factory(engine),
            rabbit_publisher(broker),
            batch_size=settings.outbox_batch_size,
            poll_interval_seconds=settings.outbox_poll_interval_seconds,
        )
        log.info("outbox relay started")
        await relay.run(stop)
    finally:
        await broker.stop()
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    asyncio.run(main(get_settings()))
