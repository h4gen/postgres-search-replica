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
- `make test-unit` - Run fast unit tests for transformation logic.
- `make test-integration` - Run full end-to-end replication tests (requires `make dev`).
- `make down` - Stop the dev environment.
- `make clean` - Full cleanup including database volumes.

## Configuration

Settings are managed via Pydantic and can be overridden by environment variables or a `.env` file. See `src/config.py` for all available options.

## Testing

The project uses `pytest` with a clear separation between:
- **Unit Tests**: Test the Polars logic in isolation.
- **Integration Tests**: Verify the actual WAL replication flow between two database instances.

