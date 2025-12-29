.PHONY: help dev down test test-unit test-integration clean lint

# Default values for local development
export PYTHONPATH := .
export SOURCE_URL ?= postgresql://postgres:password@localhost:5433/production_db
export SINK_URL ?= postgresql://postgres:password@localhost:5434/search_replica_db

help:
	@echo "Available commands:"
	@echo "  make dev              - Start development containers"
	@echo "  make down             - Stop development containers"
	@echo "  make test             - Run all tests"
	@echo "  make test-unit        - Run only unit tests"
	@echo "  make test-integration - Run only integration tests"
	@echo "  make lint             - Run linting checks"
	@echo "  make clean            - Remove volumes and temporary files"

dev:
	docker-compose -f dev/docker-compose.yml up --build -d

down:
	docker-compose -f dev/docker-compose.yml down

test: test-unit test-integration

test-unit:
	uv run pytest tests/test_transformer.py

test-integration:
	uv run pytest tests/test_integration.py

lint:
	uv run ruff check .

clean:
	docker-compose -f dev/docker-compose.yml down -v
	rm -rf .pytest_cache .venv

