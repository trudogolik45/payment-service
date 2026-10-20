from decimal import Decimal

import pytest
from app.schemas import PaymentCreate
from pydantic import ValidationError

VALID = {
    "amount": "100.50",
    "currency": "RUB",
    "description": "Заказ",
    "webhook_url": "https://example.com/hook",
}


def test_valid_body_parses_amount_as_decimal() -> None:
    payment = PaymentCreate.model_validate(VALID)

    assert payment.amount == Decimal("100.50")
    assert payment.metadata == {}


def test_amount_is_normalized_to_two_decimal_places() -> None:
    payment = PaymentCreate.model_validate({**VALID, "amount": "100.5"})

    assert str(payment.amount) == "100.50"


@pytest.mark.parametrize("amount", ["0", "-1", "1.234", "abc"])
def test_rejects_invalid_amount(amount: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**VALID, "amount": amount})


def test_rejects_unknown_currency() -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**VALID, "currency": "GBP"})


@pytest.mark.parametrize("url", ["ftp://example.com", "not-a-url", ""])
def test_rejects_non_http_webhook_url(url: str) -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**VALID, "webhook_url": url})


def test_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**VALID, "status": "succeeded"})


def test_rejects_webhook_url_longer_than_column() -> None:
    url = "https://example.com/" + "a" * 2060

    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**VALID, "webhook_url": url})
