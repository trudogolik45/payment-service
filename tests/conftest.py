import os
import uuid
from collections.abc import AsyncIterator
from decimal import Decimal
from typing import Any

import pytest
from alembic import command
from alembic.config import Config
from app.config import Settings
from app.dependencies import get_session_factory
from app.main import create_app
from app.models import Currency, OutboxEvent, Payment, PaymentStatus
from httpx import ASGITransport, AsyncClient
from sqlalchemy import Connection, select, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

API_KEY = "test-api-key"


def base_settings() -> Settings:
    """Адреса инфраструктуры из .env, если он есть; остальное — тестовые значения."""
    return Settings(
        api_key=API_KEY,
        webhook_secret="webhook-secret-for-tests",
        webhook_allowed_hosts="webhook-sink, 127.0.0.1",
        gateway_min_delay_seconds=0,
        gateway_max_delay_seconds=0,
        retry_base_delay_seconds=2,
        max_attempts=3,
    )


# Своя БД на каждый запуск pytest: параллельные прогоны (CI, ревью, IDE) не мешают друг другу.
TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL",
    base_settings().database_url.rsplit("/", 1)[0] + f"/payments_test_{uuid.uuid4().hex[:8]}",
)


def _alembic_config() -> Config:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", TEST_DATABASE_URL)
    return config


def _upgrade(connection: Connection) -> None:
    config = _alembic_config()
    config.attributes["connection"] = connection
    command.upgrade(config, "head")


def _downgrade(connection: Connection) -> None:
    config = _alembic_config()
    config.attributes["connection"] = connection
    command.downgrade(config, "base")


_ADMIN_URL = TEST_DATABASE_URL.rsplit("/", 1)[0] + "/postgres"
_DB_NAME = TEST_DATABASE_URL.rsplit("/", 1)[1]


async def _run_admin(statement: str) -> None:
    engine = create_async_engine(_ADMIN_URL, isolation_level="AUTOCOMMIT")
    try:
        async with engine.connect() as conn:
            await conn.execute(text(statement))
    except OSError as exc:
        pytest.fail(
            f"PostgreSQL недоступен по {_ADMIN_URL}: {exc}. "
            "Поднимите БД: `docker compose up -d postgres`."
        )
    finally:
        await engine.dispose()


async def _create_database() -> None:
    await _run_admin(f'DROP DATABASE IF EXISTS "{_DB_NAME}"')
    await _run_admin(f'CREATE DATABASE "{_DB_NAME}"')


async def _drop_database() -> None:
    await _run_admin(f'DROP DATABASE IF EXISTS "{_DB_NAME}"')


@pytest.fixture(scope="session")
def settings() -> Settings:
    return base_settings().model_copy(update={"database_url": TEST_DATABASE_URL})


@pytest.fixture(scope="session")
async def engine() -> AsyncIterator[AsyncEngine]:
    await _create_database()
    engine = create_async_engine(TEST_DATABASE_URL)
    async with engine.begin() as conn:
        await conn.run_sync(_upgrade)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(_downgrade)
    await engine.dispose()
    await _drop_database()


@pytest.fixture
async def session_factory(engine: AsyncEngine) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    yield async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE outbox, payments"))


@pytest.fixture
async def client(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> AsyncIterator[AsyncClient]:
    app = create_app(settings)
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
def auth_headers() -> dict[str, str]:
    return {"X-API-Key": API_KEY}


@pytest.fixture
def payment_body() -> dict[str, Any]:
    return {
        "amount": "100.50",
        "currency": "RUB",
        "description": "Заказ #1",
        "metadata": {"order_id": 1},
        "webhook_url": "https://example.com/hook",
    }


async def insert_payment(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    status: PaymentStatus = PaymentStatus.PENDING,
    webhook_url: str = "https://example.com/hook",
    idempotency_key: str = "key-1",
) -> Payment:
    payment = Payment(
        amount=Decimal("10.00"),
        currency=Currency.USD,
        description="test",
        metadata_={"k": "v"},
        status=status,
        idempotency_key=idempotency_key,
        request_hash="hash",
        webhook_url=webhook_url,
    )
    async with session_factory() as session:
        session.add(payment)
        await session.commit()
    return payment


async def fetch_payment(
    session_factory: async_sessionmaker[AsyncSession], payment: Payment
) -> Payment:
    async with session_factory() as session:
        refreshed = await session.get(Payment, payment.id)
        assert refreshed is not None
        return refreshed


async def fetch_outbox(session_factory: async_sessionmaker[AsyncSession]) -> list[OutboxEvent]:
    async with session_factory() as session:
        return list(await session.scalars(select(OutboxEvent).order_by(OutboxEvent.id)))
