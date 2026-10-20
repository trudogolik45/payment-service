from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.payments import router
from app.config import Settings, get_settings
from app.db import create_engine, create_session_factory


def create_app(settings: Settings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings.database_url)
        app.state.session_factory = create_session_factory(engine)
        yield
        await engine.dispose()

    app = FastAPI(title="Payment Service", version="0.1.0", lifespan=lifespan)
    app.state.settings = settings
    app.include_router(router)

    @app.get("/health", tags=["service"], include_in_schema=False)
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


def app_factory() -> FastAPI:
    """Точка входа: uvicorn app.main:app_factory --factory."""
    return create_app(get_settings())
