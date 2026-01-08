.PHONY: help dev down test test-unit test-integration clean lint type-check wait-for-infra test-dev

# Default values for local development
export PYTHONPATH := src
export SOURCE_URL ?= postgresql://postgres:password@localhost:5433/production_db
export SINK_URL ?= postgresql://postgres@localhost:5434/postgres

help:
	@echo "Available commands:"
	@echo "  make dev              - Start development containers"
	@echo "  make down             - Stop development containers"
	@echo "  make wait-for-infra   - Wait for all services to be ready"
	@echo "  make test             - Run all tests (unit + integration)"
	@echo "  make test-unit        - Run only unit tests"
	@echo "  make test-integration - Run only integration tests. Pass ARGS=\"...\" for scoped tests."
	@echo "  make test-dev         - Start infra, wait, and run tests"
	@echo "  make lint             - Run ruff linter and formatter check"
	@echo "  make type-check       - Run ty type checker"
	@echo "  make clean            - Remove volumes and temporary files"

dev: clean
	docker compose -f dev/docker-compose.yml up --build -d

down:
	docker compose -f dev/docker-compose.yml down

wait-for-infra:
	@echo "Waiting for services to start..."
	@until [ "$$(docker compose -f dev/docker-compose.yml ps -q source | head -n 1)" ]; do sleep 1; done
	@until [ "$$(docker compose -f dev/docker-compose.yml ps -q sink | head -n 1)" ]; do sleep 1; done
	@until [ "$$(docker compose -f dev/docker-compose.yml ps -q ollama | head -n 1)" ]; do sleep 1; done
	@echo "Waiting for Postgres (Source) to be ready..."
	@until [ "$$(docker compose -f dev/docker-compose.yml ps source --format json | grep -o '\"Health\":\"healthy\"')" ] || \
	       docker compose -f dev/docker-compose.yml exec -T source pg_isready -U postgres > /dev/null 2>&1; do \
		echo "Source DB starting..."; \
		sleep 2; \
	done
	@echo "Waiting for Postgres (Sink) to be ready..."
	@until [ "$$(docker compose -f dev/docker-compose.yml ps sink --format json | grep -o '\"Health\":\"healthy\"')" ] || \
	       docker compose -f dev/docker-compose.yml exec -T sink pg_isready -U postgres -h localhost -p 54322 > /dev/null 2>&1; do \
		echo "Sink DB starting..."; \
		sleep 2; \
	done
	@echo "Waiting for Ollama model to be pulled (274MB)..."
	@until [ "$$(docker compose -f dev/docker-compose.yml ps ollama --format json | grep -o '\"Health\":\"healthy\"')" ] || \
	       (docker compose -f dev/docker-compose.yml exec -T ollama ollama list | grep -q 'nomic-embed-text'); do \
		echo "Ollama status: $$(docker compose -f dev/docker-compose.yml exec -T ollama ollama list | grep 'nomic-embed-text' || echo 'Model not found yet, pulling...')"; \
		echo "Last 3 lines of Ollama log:"; \
		docker compose -f dev/docker-compose.yml logs --tail 3 ollama; \
		sleep 5; \
	done
	@echo "Waiting for Qdrant to be ready..."
	@until curl -s http://localhost:6333/healthz > /dev/null; do \
		echo "Qdrant not ready..."; \
		sleep 2; \
	done
	@echo "Infrastructure is ready!"

test: test-unit test-integration

test-unit:
	@echo "Unit tests for custom transformers are deprecated after pgai migration."

test-integration:
	uv sync --extra test
	@if [ -z "$(ARGS)" ]; then \
		PYTHONPATH=src uv run pytest -v -s --log-cli-level=INFO tests/; \
	else \
		PYTHONPATH=src uv run pytest -v -s --log-cli-level=INFO $(ARGS); \
	fi

test-dev: dev wait-for-infra test

lint:
	uv run ruff check src tests

type-check:
	PYTHONPATH=src uv run ty check src

clean:
	docker compose -f dev/docker-compose.yml down -v
	rm -rf .pytest_cache .venv

