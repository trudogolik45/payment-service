"""Outbox relay: переносит события из таблицы outbox в RabbitMQ.

Строки берутся под FOR UPDATE SKIP LOCKED, поэтому можно запускать
несколько экземпляров. Гарантия — at-least-once: если публикация прошла,
а commit нет, событие будет опубликовано повторно; consumer идемпотентен.
Сбой публикации одного события не блокирует остальные: строка остаётся
неопубликованной и будет взята снова на следующей итерации. После
max_attempts неудач событие уходит в карантин: остаётся в таблице с
attempts и last_error, но больше не выбирается — ядовитое событие не
занимает relay вечно. Опубликованные события старше retention удаляются.
"""

import asyncio
import contextlib
import logging
import signal
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any, cast

from faststream.rabbit import RabbitBroker
from sqlalchemy import CursorResult, Select, delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.broker import NEW_PAYMENTS_QUEUE, PAYMENTS_EXCHANGE, create_broker, declare_topology
from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory
from app.log_config import LOG_FORMAT
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
        retention: timedelta = timedelta(hours=24),
        cleanup_interval_seconds: float = 60,
        max_attempts: int = 10,
    ) -> None:
        self._session_factory = session_factory
        self._publish = publisher
        self._batch_size = batch_size
        self._poll_interval = poll_interval_seconds
        self._retention = retention
        self._cleanup_interval = cleanup_interval_seconds
        self._max_attempts = max_attempts

    async def relay_once(self) -> int:
        """Публикует одну пачку неопубликованных событий. Возвращает число опубликованных."""
        async with self._session_factory() as session, session.begin():
            events: list[OutboxEvent] = list(await session.scalars(self._pending_batch()))
            return sum([await self._publish_one(event) for event in events])

    async def cleanup_once(self) -> int:
        """Удаляет опубликованные события старше retention. Возвращает число удалённых."""
        threshold = datetime.now(UTC) - self._retention
        stmt = delete(OutboxEvent).where(OutboxEvent.published_at < threshold)
        async with self._session_factory() as session, session.begin():
            result = cast(CursorResult[Any], await session.execute(stmt))
            return result.rowcount

    async def run(self, stop: asyncio.Event) -> None:
        next_cleanup = 0.0
        while not stop.is_set():
            if asyncio.get_running_loop().time() >= next_cleanup:
                await self._cleanup_safely()
                next_cleanup = asyncio.get_running_loop().time() + self._cleanup_interval
            published = await self._relay_safely()
            if published < self._batch_size:
                await self._sleep(stop)

    async def _publish_one(self, event: OutboxEvent) -> bool:
        try:
            await self._publish(event)
        except Exception as exc:
            self._record_failure(event, exc)
            return False
        event.published_at = datetime.now(UTC)
        return True

    def _record_failure(self, event: OutboxEvent, exc: Exception) -> None:
        event.attempts += 1
        event.last_error = repr(exc)[:1000]
        if event.attempts >= self._max_attempts:
            log.error(
                "outbox event %d quarantined after %d attempts: %r", event.id, event.attempts, exc
            )
            return
        log.exception(
            "outbox event %d publish failed (attempt %d), will retry", event.id, event.attempts
        )

    async def _relay_safely(self) -> int:
        try:
            return await self.relay_once()
        except Exception:
            log.exception("outbox relay iteration failed")
            return 0

    async def _cleanup_safely(self) -> None:
        try:
            deleted = await self.cleanup_once()
        except Exception:
            log.exception("outbox cleanup failed")
            return
        if deleted:
            log.info("outbox cleanup: %d published events removed", deleted)

    async def _sleep(self, stop: asyncio.Event) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=self._poll_interval)

    def _pending_batch(self) -> Select[OutboxEvent]:
        return (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None), OutboxEvent.attempts < self._max_attempts)
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
            retention=timedelta(hours=settings.outbox_retention_hours),
            cleanup_interval_seconds=settings.outbox_cleanup_interval_seconds,
            max_attempts=settings.outbox_max_attempts,
        )
        log.info("outbox relay started")
        await relay.run(stop)
        log.info("outbox relay stopped")
    finally:
        await broker.stop()
        await engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    asyncio.run(main(get_settings()))
