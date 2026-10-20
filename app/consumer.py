import asyncio
import logging
from typing import Protocol
from uuid import UUID

import httpx
from faststream import Context, FastStream
from faststream.exceptions import NackMessage
from faststream.rabbit import Channel, RabbitBroker, RabbitMessage

from app.broker import (
    ATTEMPT_HEADER,
    NEW_PAYMENTS_QUEUE,
    PAYMENTS_EXCHANGE,
    create_broker,
    declare_topology,
    retry_delay_seconds,
    retry_queue,
)
from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory
from app.gateway import SimulatedGateway
from app.log_config import LOG_FORMAT
from app.schemas import PaymentCreatedEvent
from app.services.processing import PaymentNotFoundError, PaymentProcessor
from app.webhook import WebhookNotifier

log = logging.getLogger(__name__)


class Processor(Protocol):
    async def process(self, payment_id: UUID) -> None: ...


def read_attempt(msg: RabbitMessage) -> int:
    return int(msg.headers.get(ATTEMPT_HEADER, 1))


async def schedule_retry(
    broker: RabbitBroker, event: PaymentCreatedEvent, attempt: int, settings: Settings
) -> None:
    """Кладёт событие в retry-очередь; по TTL оно вернётся в payments.new.

    Если положить не удалось, сообщение после паузы возвращается в очередь как
    есть — иначе сбой брокера отправил бы его в DLQ раньше времени. Пауза
    удерживает слот prefetch, зато не даёт сообщению крутиться без задержки.
    """
    try:
        await broker.publish(
            event,
            queue=retry_queue(attempt, settings).name,
            headers={ATTEMPT_HEADER: attempt + 1},
            persist=True,
        )
    except Exception as exc:
        log.exception("payment %s: retry publish failed, requeueing", event.payment_id)
        await asyncio.sleep(retry_delay_seconds(attempt, settings))
        raise NackMessage(requeue=True) from exc


def register_handlers(broker: RabbitBroker, settings: Settings) -> None:
    @broker.subscriber(
        NEW_PAYMENTS_QUEUE,
        PAYMENTS_EXCHANGE,
        channel=Channel(prefetch_count=settings.consumer_prefetch_count),
    )
    async def handle_payment_created(
        event: PaymentCreatedEvent,
        msg: RabbitMessage,
        processor: Processor = Context(),  # noqa: B008
    ) -> None:
        attempt = read_attempt(msg)
        try:
            await processor.process(event.payment_id)
        except PaymentNotFoundError:
            log.error("payment %s not found, message goes to DLQ", event.payment_id)
            raise
        except Exception:
            if attempt >= settings.max_attempts:
                log.exception(
                    "payment %s failed after %d attempts, message goes to DLQ",
                    event.payment_id,
                    attempt,
                )
                raise
            log.warning("payment %s attempt %d failed, scheduling retry", event.payment_id, attempt)
            await schedule_retry(broker, event, attempt, settings)


def build_consumer(settings: Settings) -> FastStream:
    broker = create_broker(settings)
    register_handlers(broker, settings)
    app = FastStream(broker)

    @app.on_startup
    async def startup() -> None:
        await broker.connect()
        await declare_topology(broker, settings)

        engine = create_engine(settings.database_url, pool_size=settings.consumer_prefetch_count)
        http_client = httpx.AsyncClient()
        processor = PaymentProcessor(
            create_session_factory(engine),
            SimulatedGateway(
                min_delay_seconds=settings.gateway_min_delay_seconds,
                max_delay_seconds=settings.gateway_max_delay_seconds,
                success_rate=settings.gateway_success_rate,
            ),
            WebhookNotifier(http_client, timeout_seconds=settings.webhook_timeout_seconds),
        )
        app.context.set_global("processor", processor)
        app.context.set_global("engine", engine)
        app.context.set_global("http_client", http_client)

    @app.on_shutdown
    async def shutdown() -> None:
        await app.context.get("http_client").aclose()
        await app.context.get("engine").dispose()

    return app


def app_factory() -> FastStream:
    """Точка входа: faststream run app.consumer:app_factory --factory."""
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    return build_consumer(get_settings())
