.PHONY: help dev down test test-unit test-integration clean lint

# Default values for local development
export PYTHONPATH := src
export SOURCE_URL ?= postgresql://postgres:password@localhost:5433/production_db
export SINK_URL ?= local

help:
	@echo "Available commands:"
	@echo "  make dev              - Start development containers"
	@echo "  make down             - Stop development containers"
	@echo "  make test             - Run all tests"
	@echo "  make test-unit        - Run only unit tests"
	@echo "  make test-integration - Run only integration tests"
	@echo "  make lint             - Run ruff linter and formatter check"
	@echo "  make type-check       - Run ty type checker"
	@echo "  make clean            - Remove volumes and temporary files"

dev:
	docker compose -f dev/docker-compose.yml up --build -d

down:
	docker compose -f dev/docker-compose.yml down

test: test-unit test-integration

test-unit:
	@echo "Unit tests for custom transformers are deprecated after pgai migration."

test-integration:
	PYTHONPATH=src uv run pytest -v -s --log-cli-level=INFO tests/

lint:
	uv run ruff check src tests

type-check:
	PYTHONPATH=src uv run ty check src

clean:
	docker compose -f dev/docker-compose.yml down -v
	rm -rf .pytest_cache .venv

