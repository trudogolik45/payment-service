import random
from unittest.mock import AsyncMock, patch

from app.gateway import SimulatedGateway
from app.models import PaymentStatus


class FixedRandom(random.Random):
    def __init__(self, value: float) -> None:
        super().__init__()
        self._value = value

    def random(self) -> float:
        return self._value

    def uniform(self, a: float, b: float) -> float:
        return a


async def test_gateway_succeeds_when_roll_is_below_success_rate() -> None:
    gateway = SimulatedGateway(
        min_delay_seconds=0, max_delay_seconds=0, success_rate=0.9, rng=FixedRandom(0.5)
    )

    assert await gateway.charge() is PaymentStatus.SUCCEEDED


async def test_gateway_fails_when_roll_is_above_success_rate() -> None:
    gateway = SimulatedGateway(
        min_delay_seconds=0, max_delay_seconds=0, success_rate=0.9, rng=FixedRandom(0.95)
    )

    assert await gateway.charge() is PaymentStatus.FAILED


async def test_gateway_waits_between_min_and_max_delay() -> None:
    gateway = SimulatedGateway(
        min_delay_seconds=2, max_delay_seconds=5, success_rate=1, rng=FixedRandom(0.0)
    )

    with patch("app.gateway.asyncio.sleep", new=AsyncMock()) as sleep:
        await gateway.charge()

    delay = sleep.call_args.args[0]
    assert 2 <= delay <= 5
