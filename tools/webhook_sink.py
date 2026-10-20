"""Приёмник webhook'ов для демонстрации: проверяет подпись и логирует всё, что прилетает."""

import hmac
import logging
import os
from typing import Any

from app.webhook import SIGNATURE_HEADER, sign
from fastapi import FastAPI, HTTPException, Request

log = logging.getLogger("webhook-sink")
app = FastAPI(title="Webhook sink")
received: list[dict[str, Any]] = []


@app.post("/webhooks", status_code=204)
async def receive(request: Request) -> None:
    body = await request.body()
    secret = os.environ.get("WEBHOOK_SECRET")
    if secret and not hmac.compare_digest(
        request.headers.get(SIGNATURE_HEADER, ""), sign(secret, body)
    ):
        raise HTTPException(401, "bad webhook signature")
    payload = await request.json()
    received.append(payload)
    log.info("webhook received: %s", payload)


@app.get("/webhooks")
async def list_received() -> list[dict[str, Any]]:
    return received
