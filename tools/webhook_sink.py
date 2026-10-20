"""Приёмник webhook'ов для демонстрации: проверяет подпись (с timestamp) и логирует."""

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from app.webhook import SIGNATURE_HEADER, TIMESTAMP_HEADER, verify
from fastapi import FastAPI, HTTPException, Request

log = logging.getLogger("webhook-sink")
received: list[dict[str, Any]] = []


def _secret() -> str:
    secret = os.environ.get("WEBHOOK_SECRET")
    if not secret:
        raise RuntimeError("WEBHOOK_SECRET is required to verify webhook signatures")
    return secret


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    _secret()
    yield


app = FastAPI(title="Webhook sink", lifespan=lifespan)


@app.post("/webhooks", status_code=204)
async def receive(request: Request) -> None:
    body = await request.body()
    timestamp = request.headers.get(TIMESTAMP_HEADER, "")
    signature = request.headers.get(SIGNATURE_HEADER, "")
    if not verify(_secret(), timestamp, signature, body):
        raise HTTPException(401, "bad webhook signature")
    payload = await request.json()
    received.append(payload)
    log.info("webhook received: %s", payload)


@app.get("/webhooks")
async def list_received() -> list[dict[str, Any]]:
    return received
