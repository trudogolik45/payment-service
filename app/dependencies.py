from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.services.payments import PaymentService
from app.webhook_policy import WebhookUrlPolicy


def get_session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
    factory: async_sessionmaker[AsyncSession] = request.app.state.session_factory
    return factory


async def get_session(
    session_factory: Annotated[async_sessionmaker[AsyncSession], Depends(get_session_factory)],
) -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session


def get_webhook_policy(request: Request) -> WebhookUrlPolicy:
    policy: WebhookUrlPolicy = request.app.state.webhook_policy
    return policy


def get_payment_service(
    session: Annotated[AsyncSession, Depends(get_session)],
    webhook_policy: Annotated[WebhookUrlPolicy, Depends(get_webhook_policy)],
) -> PaymentService:
    return PaymentService(session, webhook_policy)
