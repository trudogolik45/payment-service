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


def _secret() -> str:
    secret = os.environ.get("WEBHOOK_SECRET")
    if not secret:
        raise RuntimeError("WEBHOOK_SECRET is required to verify webhook signatures")
    return secret


@app.post("/webhooks", status_code=204)
async def receive(request: Request) -> None:
    body = await request.body()
    if not hmac.compare_digest(request.headers.get(SIGNATURE_HEADER, ""), sign(_secret(), body)):
        raise HTTPException(401, "bad webhook signature")
    payload = await request.json()
    received.append(payload)
    log.info("webhook received: %s", payload)


@app.get("/webhooks")
async def list_received() -> list[dict[str, Any]]:
    return received
