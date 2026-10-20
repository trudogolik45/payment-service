import asyncio
import random
from typing import Protocol
from uuid import UUID

from app.models import PaymentStatus


class PaymentGateway(Protocol):
    async def charge(self, idempotency_key: UUID) -> PaymentStatus: ...


class SimulatedGateway:
    """Эмуляция внешнего платёжного шлюза: задержка 2–5 с, 90% успех.

    Идемпотентна по ключу, как настоящий шлюз: повторное списание с тем же
    ключом (например, после обрыва БД до commit) возвращает прежний результат,
    а не списывает деньги второй раз. Память результатов — на процесс.
    """

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
        self._results: dict[UUID, PaymentStatus] = {}

    async def charge(self, idempotency_key: UUID) -> PaymentStatus:
        if idempotency_key in self._results:
            return self._results[idempotency_key]
        await asyncio.sleep(self._rng.uniform(self._min_delay, self._max_delay))
        result = (
            PaymentStatus.SUCCEEDED
            if self._rng.random() < self._success_rate
            else PaymentStatus.FAILED
        )
        return self._results.setdefault(idempotency_key, result)
