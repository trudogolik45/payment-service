import asyncio
import random
from typing import Protocol

from app.models import PaymentStatus


class PaymentGateway(Protocol):
    async def charge(self) -> PaymentStatus: ...


class SimulatedGateway:
    """Эмуляция внешнего платёжного шлюза: задержка 2–5 с, 90% успех."""

    def __init__(
        self,
        *,
        min_delay_seconds: float,
        max_delay_seconds: float,
        success_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        self._min_delay = min_delay_seconds
        self._max_delay = max_delay_seconds
        self._success_rate = success_rate
        self._rng = rng or random.Random()

    async def charge(self) -> PaymentStatus:
        await asyncio.sleep(self._rng.uniform(self._min_delay, self._max_delay))
        if self._rng.random() < self._success_rate:
            return PaymentStatus.SUCCEEDED
        return PaymentStatus.FAILED
