"""Топология RabbitMQ.

payments (direct) --payments.new--> [payments.new] --reject--> payments.dlx (fanout) --> [payments.dlq]
                                          ^
          [payments.new.retry.N] --TTL----+   (повтор с задержкой base * 2^(N-1))
"""  # noqa: E501

from faststream.rabbit import Channel, ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import Settings

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.FANOUT, durable=True)

NEW_PAYMENTS_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key="payments.new",
    arguments={"x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name},
)
DEAD_LETTER_QUEUE = RabbitQueue("payments.dlq", durable=True)

ATTEMPT_HEADER = "x-attempt"


def retry_delay_seconds(attempt: int, settings: Settings) -> float:
    """Задержка перед попыткой attempt + 1 (экспоненциальная)."""
    return float(settings.retry_base_delay_seconds * 2 ** (attempt - 1))


def retry_queue(attempt: int, settings: Settings) -> RabbitQueue:
    """Очередь ожидания после неудачной попытки attempt.

    По истечении TTL сообщение возвращается в payments.new.
    """
    return RabbitQueue(
        f"{NEW_PAYMENTS_QUEUE.name}.retry.{attempt}",
        durable=True,
        arguments={
            "x-message-ttl": int(retry_delay_seconds(attempt, settings) * 1000),
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": NEW_PAYMENTS_QUEUE.routing_key,
        },
    )


def create_broker(settings: Settings) -> RabbitBroker:
    """Брокер с publisher confirms; сообщение без маршрута — ошибка, а не тихая потеря."""
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
    )


async def declare_topology(broker: RabbitBroker, settings: Settings) -> None:
    """Объявляет обменники и очереди; вызов идемпотентен."""
    exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    dlx = await broker.declare_exchange(DEAD_LETTER_EXCHANGE)

    main_queue = await broker.declare_queue(NEW_PAYMENTS_QUEUE)
    await main_queue.bind(exchange, routing_key=NEW_PAYMENTS_QUEUE.routing_key)

    dlq = await broker.declare_queue(DEAD_LETTER_QUEUE)
    await dlq.bind(dlx)

    for attempt in range(1, settings.max_attempts):
        await broker.declare_queue(retry_queue(attempt, settings))
