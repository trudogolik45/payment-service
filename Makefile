.PHONY: up down logs test lint typecheck check migrate

up:            ## Поднять всё окружение
	docker compose up -d --build

down:          ## Остановить и удалить контейнеры и данные
	docker compose down -v

logs:
	docker compose logs -f api outbox-relay consumer webhook-sink

test:          ## Тесты (нужны Postgres и RabbitMQ)
	docker compose up -d postgres rabbitmq
	uv run pytest

lint:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run mypy

check: lint typecheck test
