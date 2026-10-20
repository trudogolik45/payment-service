"""Приёмник webhook'ов для демонстрации: логирует всё, что прилетает."""

import logging
from typing import Any

from fastapi import FastAPI

log = logging.getLogger("webhook-sink")
app = FastAPI(title="Webhook sink")
received: list[dict[str, Any]] = []


@app.post("/webhooks", status_code=204)
async def receive(payload: dict[str, Any]) -> None:
    received.append(payload)
    log.info("webhook received: %s", payload)


@app.get("/webhooks")
async def list_received() -> list[dict[str, Any]]:
    return received
