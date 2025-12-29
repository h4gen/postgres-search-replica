# Search Replica Daemon

A professional-grade PostgreSQL read replica with real-time Polars transformation and pgvector support.

## Architecture

- **Native Bridge**: Uses PostgreSQL Native Logical Replication for data movement.
- **Async Python**: A robust daemon using `psycopg3` async notifications.
- **Polars**: High-performance, type-safe data transformations.
- **pgvector**: Integrated vector storage for search embeddings.
- **PG 15 Row Filtering**: Selective replication to minimize network traffic and processing load.

## Key Features (Enterprise Ready)

- **Source Protection (Self-Cleaning)**: The replicator automatically drops its subscription and slot upon graceful shutdown (`SIGTERM`/`SIGINT`), ensuring the Source DB never accumulates WAL logs or runs out of disk space.
- **Smart Reconciliation**: Only updates embeddings and timestamps if the source data has actually changed, significantly reducing Sink DB load.
- **Zero-Touch Config**: Automatically synchronizes publication columns and filters from Python settings to the database on startup.

## Development

We use a `Makefile` to encapsulate best practices and common tasks.

### Setup
1. Install [uv](https://github.com/astral-sh/uv).
2. Sync dependencies: `uv sync --extra test`.

### Common Commands
- `make dev` - Spin up the local development environment (Source + Sink + Daemon).
- `make test` - Run both unit and integration tests.
- `make test-unit` - Run fast unit tests for transformation logic.
- `make test-integration` - Run full end-to-end replication tests (requires `make dev`).
- `make lint` - Run `ruff` linter and formatter checks.
- `make type-check` - Run `ty` type checker for Python type safety.
- `make down` - Stop the dev environment.
- `make clean` - Full cleanup including database volumes and temporary files.

## Configuration

Settings are managed via Pydantic and can be overridden by environment variables, a `.env` file, or a `.env.development` file. Key options:
- `SOURCE_URL`: URL of the source PostgreSQL database.
- `SINK_URL`: URL of the sink PostgreSQL database.
- `PUBLICATION_NAME`: PostgreSQL publication name (default: `pub_users`).
- `PUBLICATION_COLUMNS`: List of columns to replicate (default: None).
- `PUBLICATION_WHERE`: Optional PG 15 row filter clause (e.g., `id > 100`).
- `SUBSCRIPTION_NAME`: PostgreSQL subscription name (default: `sub_users`).
- `SUBSCRIPTION_OPTIONS`: Dict of subscription parameters (e.g., `{"streaming": "'on'"}`).
- `BATCH_SIZE`: Number of rows to process in one transformation cycle (default: `50`).
- `MAX_SLOT_WAL_KEEP_SIZE_MB`: Safety limit for WAL retention on the Source (default: `1024`).

See `src/config.py` for all available options and defaults.

## CI/CD

The project includes a GitHub Actions workflow (`.github/workflows/ci.yml`) that:
- Starts the full infrastructure using `make dev`.
- Waits for databases to be ready using `pg_isready`.
- Runs the complete test suite (`unit` and `integration`).
- Performs automated cleanup.

