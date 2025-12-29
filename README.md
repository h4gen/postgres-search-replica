# Search Replica Daemon

A professional-grade PostgreSQL read replica with real-time Polars transformation and pgvector support.

## Architecture

- **Native Bridge**: Uses PostgreSQL Native Logical Replication for data movement.
- **Async Python**: A robust daemon using `psycopg3` async notifications.
- **Polars**: High-performance, type-safe data transformations.
- **pgvector**: Integrated vector storage for search embeddings.

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
- `SUBSCRIPTION_NAME`: PostgreSQL subscription name (default: `sub_users`).
- `BATCH_SIZE`: Number of rows to process in one transformation cycle (default: `50`).

See `src/config.py` for all available options and defaults.

## CI/CD

The project includes a GitHub Actions workflow (`.github/workflows/ci.yml`) that:
- Starts the full infrastructure using `make dev`.
- Waits for databases to be ready using `pg_isready`.
- Runs the complete test suite (`unit` and `integration`).
- Performs automated cleanup.

