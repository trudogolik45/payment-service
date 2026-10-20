"""`alembic check` на одноразовой БД: ловит дрейф моделей и миграций без риска для рабочей."""

import asyncio
import uuid

from alembic import command
from alembic.config import Config
from app.config import Settings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

API_KEY = "migrations-check"


async def main() -> None:
    base_url = Settings(api_key=API_KEY).database_url.rsplit("/", 1)[0]
    db_name = f"payments_migcheck_{uuid.uuid4().hex[:8]}"
    admin = create_async_engine(f"{base_url}/postgres", isolation_level="AUTOCOMMIT")
    try:
        async with admin.connect() as conn:
            await conn.execute(text(f'CREATE DATABASE "{db_name}"'))
        config = Config("alembic.ini")
        config.set_main_option("sqlalchemy.url", f"{base_url}/{db_name}")
        await asyncio.to_thread(command.upgrade, config, "head")
        await asyncio.to_thread(command.check, config)
    finally:
        async with admin.connect() as conn:
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'))
        await admin.dispose()


if __name__ == "__main__":
    asyncio.run(main())
