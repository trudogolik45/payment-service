# Payment Service

Асинхронный сервис процессинга платежей. Принимает платёж по HTTP, обрабатывает его
через эмуляцию платёжного шлюза в фоновом consumer'е и уведомляет клиента webhook'ом.

**Стек:** FastAPI, Pydantic v2, SQLAlchemy 2.0 (async), PostgreSQL, RabbitMQ (FastStream),
Alembic, Docker Compose.

## Архитектура

```
клиент ──POST /api/v1/payments──▶ api ──┐ одна транзакция
                                        ├─▶ payments  (status = pending)
                                        └─▶ outbox    (payment.created)
                                                  │
                                      outbox-relay│ FOR UPDATE SKIP LOCKED
                                                  ▼
                 exchange payments ──payments.new──▶ queue payments.new
                        ▲                                   │
                        │ TTL истёк                         ▼ consumer
         queue payments.new.retry.N ◀── сбой ───── шлюз (2–5 с, 90 % успех)
                                     попытка < 3   │  → status в БД → webhook
                                                   │
                                    сбой на 3-й попытке (reject)
                                                   ▼
                 exchange payments.dlx (fanout) ──▶ queue payments.dlq

«Сбой» — исключение в обработке (webhook не доставлен, БД недоступна).
Отказ шлюза (10 %) — это бизнес-результат: платёж получает статус failed,
webhook уходит, retry не нужен.
```

| Сервис | Что делает |
|---|---|
| `api` | HTTP API. Создание платежа и запись события в `outbox` — одна транзакция |
| `outbox-relay` | Читает `outbox`, публикует в RabbitMQ, помечает `published_at` |
| `consumer` | Один обработчик `payments.new`: шлюз → статус → webhook. Ошибка → retry → DLQ |
| `migrate` | Одноразовый `alembic upgrade head` перед стартом остальных |
| `webhook-sink` | Приёмник webhook'ов для демонстрации (`POST /webhooks`, `GET /webhooks`) |

### Гарантии

- **Outbox pattern.** Событие пишется в таблицу `outbox` в той же транзакции, что и платёж.
  Relay публикует его с `persistent` и `publisher confirms` и только потом помечает
  опубликованным. Если publish прошёл, а commit нет — событие уйдёт повторно (at-least-once),
  consumer к этому готов.
- **Идемпотентность API.** `Idempotency-Key` уникален в БД (`INSERT … ON CONFLICT DO NOTHING`).
  Повтор с тем же ключом и телом возвращает тот же платёж без нового события. Тот же ключ
  с другим телом — `409 Conflict`.
- **Идемпотентность consumer'а.** Платёж со статусом не `pending` не отправляется в шлюз
  повторно, уже доставленный webhook (`webhook_delivered_at`) не шлётся второй раз.
  Каждый шаг идёт под `SELECT … FOR UPDATE`, поэтому две одновременные доставки одного
  события выполняются последовательно, и вторая видит результат первой.
- **Retry.** Всего `MAX_ATTEMPTS` = 3 попытки обработки (первая + два повтора). После
  неудачной попытки `n` сообщение уходит в `payments.new.retry.n` с TTL =
  `RETRY_BASE_DELAY_SECONDS × 2^(n−1)` (2 с, 4 с) и по истечении TTL возвращается
  в `payments.new` с заголовком `x-attempt`. Отдельная очередь на каждую задержку —
  чтобы не было head-of-line blocking. Если положить сообщение в retry-очередь не
  удалось, оно возвращается в `payments.new` как есть (nack + requeue), а не в DLQ.
- **Dead Letter Queue.** После последней неудачной попытки (или если платёж из сообщения
  не найден в БД) сообщение отклоняется, и RabbitMQ через `x-dead-letter-exchange`
  переносит его в `payments.dlq`. В заголовке `x-death` — история.
- **Аутентификация.** Статический ключ в заголовке `X-API-Key` для всех эндпоинтов API.
  `GET /health` — liveness-проба без ключа.

## Запуск

```bash
cp .env.example .env        # задать API_KEY, при необходимости поменять порты
docker compose up -d --build
```

- API: http://localhost:8000, Swagger: http://localhost:8000/docs
- RabbitMQ Management: http://localhost:15672 (guest / guest)
- Postgres, RabbitMQ и webhook-sink опубликованы только на `127.0.0.1`
- Логи: `docker compose logs -f api outbox-relay consumer webhook-sink`
- Остановить вместе с данными: `docker compose down -v`

## Примеры

Ключ из `.env.example`: `dev-api-key`.

### Создать платёж

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-api-key" \
  -H "Idempotency-Key: order-1001" \
  -H "Content-Type: application/json" \
  -d '{
    "amount": "1500.00",
    "currency": "RUB",
    "description": "Заказ #1001",
    "metadata": {"order_id": 1001, "customer": "ivan"},
    "webhook_url": "http://webhook-sink:8080/webhooks"
  }'
```

```json
{"payment_id":"6f1c6d1e-…","status":"pending","created_at":"2026-10-20T12:00:00.123456Z"}
```

Повторный запрос с тем же `Idempotency-Key` вернёт тот же `payment_id`.
Тот же ключ с другим телом — `409`. Без заголовка — `422`. Без `X-API-Key` — `401`.

### Получить платёж

```bash
curl -s http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: dev-api-key"
```

```json
{
  "payment_id": "6f1c6d1e-…",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Заказ #1001",
  "metadata": {"order_id": 1001, "customer": "ivan"},
  "status": "succeeded",
  "idempotency_key": "order-1001",
  "webhook_url": "http://webhook-sink:8080/webhooks",
  "created_at": "2026-10-20T12:00:00.123456Z",
  "processed_at": "2026-10-20T12:00:03.456789Z",
  "webhook_delivered_at": "2026-10-20T12:00:03.470000Z"
}
```

### Webhook

Через 2–5 секунд на `webhook_url` придёт `POST`:

```json
{
  "event": "payment.succeeded",
  "payment_id": "6f1c6d1e-…",
  "status": "succeeded",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Заказ #1001",
  "metadata": {"order_id": 1001, "customer": "ivan"},
  "created_at": "2026-10-20T12:00:00.123456Z",
  "processed_at": "2026-10-20T12:00:03.456789Z"
}
```

Любой ответ не 2xx или сетевая ошибка считаются неудачной доставкой и запускают retry.
Посмотреть, что получил демо-приёмник: `curl -s http://localhost:8080/webhooks`.

### Проверить retry и DLQ

Укажите недоступный `webhook_url`, например `http://webhook-sink:8080/nope` (404):

```bash
curl -s -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-api-key" -H "Idempotency-Key: dlq-demo" \
  -H "Content-Type: application/json" \
  -d '{"amount":"10.00","currency":"USD","description":"DLQ demo","webhook_url":"http://webhook-sink:8080/nope"}'
docker compose logs -f consumer
```

В логах consumer'а будут три попытки с паузами 2 и 4 секунды, затем
`message goes to DLQ`. Сообщение появится в очереди `payments.dlq`
(RabbitMQ Management → Queues). Статус платежа при этом сохранён (`succeeded`/`failed`),
не доставлен только webhook.

## Конфигурация

Все параметры — в `.env` (см. `.env.example`):

| Переменная | По умолчанию | Описание |
|---|---|---|
| `API_KEY` | обязателен (`dev-api-key` в `.env.example`) | Ключ для `X-API-Key` |
| `DATABASE_URL` | `postgresql+asyncpg://…@localhost:5432/payments` | Подключение к PostgreSQL |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | Подключение к RabbitMQ |
| `GATEWAY_MIN_DELAY_SECONDS` / `GATEWAY_MAX_DELAY_SECONDS` | `2` / `5` | Задержка эмуляции шлюза |
| `GATEWAY_SUCCESS_RATE` | `0.9` | Доля успешных платежей |
| `MAX_ATTEMPTS` | `3` | Всего попыток обработки сообщения |
| `RETRY_BASE_DELAY_SECONDS` | `2` | Базовая задержка retry (× 2 на каждую попытку) |
| `WEBHOOK_TIMEOUT_SECONDS` | `5` | Таймаут запроса на webhook |
| `OUTBOX_POLL_INTERVAL_SECONDS` / `OUTBOX_BATCH_SIZE` | `0.5` / `100` | Параметры relay |
| `CONSUMER_PREFETCH_COUNT` | `10` | Prefetch consumer'а |
| `*_PORT` | `5432`, `5672`, `15672`, `8000`, `8080` | Порты на хосте |

Аргументы очередей (TTL retry-очередей) фиксируются при первом объявлении: после смены
`MAX_ATTEMPTS` или `RETRY_BASE_DELAY_SECONDS` удалите старые очереди `payments.new.retry.*`.

## Что осознанно не сделано

- **Фильтр SSRF для `webhook_url`.** Принимается любой http(s)-адрес, в том числе внутри
  docker-сети — демо этим и пользуется (`webhook-sink`). В проде нужен allowlist хостов
  или запрет private/loopback-адресов после резолва.
- **Подпись webhook.** Получатель не может проверить подлинность. Решение — HMAC-заголовок
  с общим секретом.
- **Очистка и карантин `outbox`.** Опубликованные строки не удаляются; частичный индекс
  по неопубликованным держит выборку быстрой, но таблица растёт. Нужен периодический
  `DELETE … WHERE published_at < now() - interval '7 days'`. Событие, которое не удаётся
  опубликовать в принципе, остаётся в таблице и логируется на каждой итерации; в проде
  нужен счётчик попыток и карантин.
- **Лимит доставок при падении consumer'а.** Если процесс умирает посреди обработки,
  сообщение возвращается в очередь без увеличения `x-attempt`. Для classic queue лимита
  нет; защита — quorum queue с `x-delivery-limit`.

## Разработка

```bash
uv sync                                  # зависимости (включая dev)
docker compose up -d postgres rabbitmq   # тестам нужны PostgreSQL и RabbitMQ
uv run pytest                            # БД payments_test и vhost в RabbitMQ создаются сами
uv run ruff check . && uv run ruff format --check .
uv run mypy
```

Или одной командой: `make check`. `.env` для тестов не обязателен: адреса берутся из него,
если он есть, иначе — `localhost:5432` и `localhost:5672`.

Тесты: unit (схемы, шлюз, webhook, retry-логика через `TestRabbitBroker`), с PostgreSQL
(API, идемпотентность с конкурентными запросами, outbox relay, процессор с конкурентной
доставкой) и с настоящим RabbitMQ (retry по TTL → DLQ с `x-death`; сквозной запуск
consumer'а через его хуки старта: БД → шлюз → webhook на локальный HTTP-сервер).

Локальный запуск сервисов без Docker:

```bash
uv run alembic upgrade head
uv run uvicorn app.main:app_factory --factory --reload
uv run python -m app.outbox_relay
uv run faststream run app.consumer:app_factory --factory
```

Миграции: `uv run alembic upgrade head`, новая миграция —
`uv run alembic revision --autogenerate -m "..."`.

## Структура

```
app/
  main.py              FastAPI-приложение
  api/payments.py      эндпоинты
  auth.py              X-API-Key
  schemas.py           Pydantic-схемы запросов, ответов, событий, webhook
  models.py            SQLAlchemy: payments, outbox
  services/payments.py создание платежа + outbox, идемпотентность
  services/processing.py обработчик платежа (шлюз → БД → webhook)
  gateway.py           эмуляция платёжного шлюза
  webhook.py           отправка webhook
  broker.py            топология RabbitMQ: exchange, очереди, retry, DLQ
  consumer.py          FastStream-приложение, retry/DLQ-логика
  outbox_relay.py      outbox → RabbitMQ
alembic/               миграции
tests/                 pytest
tools/webhook_sink.py  демо-приёмник webhook'ов
```
