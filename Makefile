KAFKA_BROKER ?= localhost:9092
KAFKA_TOPIC ?= alpaca-bars
DOCKER_COMPOSE ?= docker compose

.PHONY: up down wait-kafka local-real-up local-real-down local-real-status local-real-logs test test-integration e2e lint smoke smoke-kafka smoke-iceberg smoke-frontend

up:
	$(DOCKER_COMPOSE) up -d tansu

down:
	$(DOCKER_COMPOSE) down

wait-kafka:
	uv run --package load python scripts/wait_kafka.py --broker $(KAFKA_BROKER)

local-real-up:
	bash scripts/local_real_pipeline.sh up

local-real-down:
	bash scripts/local_real_pipeline.sh down

local-real-status:
	bash scripts/local_real_pipeline.sh status

local-real-logs:
	bash scripts/local_real_pipeline.sh logs

test:
	uv run pytest
	cd wsr && cargo test

test-integration: up wait-kafka
	KAFKA_BROKER=$(KAFKA_BROKER) uv run pytest -m integration

e2e: up wait-kafka
	KAFKA_BROKER=$(KAFKA_BROKER) uv run pytest -m e2e

lint:
	uv run ruff check conftest.py load frontend extract/helpers scripts tests
	cd wsr && cargo fmt --check
	cd wsr && cargo clippy --all-targets --all-features -- -D warnings

smoke: smoke-kafka smoke-iceberg smoke-frontend

smoke-kafka:
	uv run --package load python scripts/peek_kafka.py --broker $(KAFKA_BROKER) --topic $(KAFKA_TOPIC) --from-beginning --max 5

smoke-iceberg:
	uv run --with duckdb --package load python scripts/query_iceberg.py

smoke-frontend:
	uv run --with requests python scripts/inspect_frontend_api.py
