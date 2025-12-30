# Search Replica Daemon

A professional-grade PostgreSQL read replica with real-time vectorization using pgai, source database protection, and pgvector support.

## Architecture

- **Native Bridge**: Uses PostgreSQL Native Logical Replication for data movement.
- **Async Python Control Plane**: A robust daemon that orchestrates the replication lifecycle and monitors system health.
- **pgai**: Leverages the `pgai` extension for declarative, database-native vectorization and background worker orchestration.
- **pgvector**: Integrated vector storage for search embeddings.
- **PG 15 Row Filtering**: Selective replication to minimize network traffic and processing load.

## Key Features (Enterprise Ready)

- **Source Protection (Watchdog)**: The replicator actively monitors its own replication lag. If the lag exceeds `MAX_SLOT_WAL_KEEP_SIZE_MB`, it triggers a self-destruct by dropping its subscription and slot. This ensures the Source DB never runs out of disk space, even if the replicator falls behind.
- **Graceful Cleanup**: Automatically drops the replication slot upon normal shutdown (`SIGTERM`/`SIGINT`) to prevent WAL accumulation while offline.
- **Smart Reconciliation**: Leverages pgai's native state tracking to only update embeddings when source data changes.
- **pgai Orchestration**: Uses database-native background workers for efficient vectorization.
- **Zero-Touch & Low Privilege**: No `SUPERUSER` rights or global server configuration changes required on the Source DB. All protection logic is handled by the replicator.
- **Zero-Touch Config**: Automatically synchronizes publication columns and filters from Python settings to the database on startup.
- **Fully Configurable Schema**: Table and column names are entirely configurable via environment variables.

## Development

We use a `Makefile` to encapsulate best practices and common tasks.

### Setup
1. Install [uv](https://github.com/astral-sh/uv).
2. Sync dependencies: `uv sync --extra test`.

### Common Commands
- `make dev` - Spin up the local development environment (Source + Sink + Daemon).
- `make test` - Run integration tests.
- `make test-unit` - Inform that unit tests are deprecated.
- `make test-integration` - Run full end-to-end replication tests (requires `make dev`).
- `make lint` - Run `ruff` linter and formatter checks.
- `make type-check` - Run `ty` type checker for Python type safety.
- `make down` - Stop the dev environment.
- `make clean` - Full cleanup including database volumes and temporary files.

## Configuration

Settings are managed via Pydantic and can be overridden by environment variables, a `.env` file, or a `.env.development` file.

### Schema Settings
- `SOURCE_TABLE`: Name of the table on the source database (default: `users`).
- `SINK_RAW_TABLE`: Name of the raw landing table on the sink (default: `users`).
- `SINK_REPLICA_TABLE`: Name of the final search replica table (default: `users_replica`).
- `ID_COLUMN`: Name of the primary key column (default: `id`).
- `CONTENT_COLUMN`: Column used for generating embeddings (default: `email`).
- `TARGET_CONTENT_COLUMN`: Transformed text column in the replica (default: `transformed_email`).
- `EMBEDDING_COLUMN`: Name of the vector column (default: `embedding`).
- `EMBEDDING_DIMENSION`: Dimension of the vector (default: `3`).

### Replication Settings
- `SOURCE_URL`: URL of the source PostgreSQL database.
- `SINK_URL`: URL of the sink PostgreSQL database.
- `PUBLICATION_NAME`: PostgreSQL publication name (default: `pub_users`).
- `PUBLICATION_COLUMNS`: List of columns to replicate (default: `["id", "email"]`).
- `PUBLICATION_WHERE`: Optional PG 15 row filter clause (e.g., `id > 100`).
- `SUBSCRIPTION_NAME`: PostgreSQL subscription name (default: `sub_users`).
- `SUBSCRIPTION_OPTIONS`: Dict of subscription parameters (e.g., `{"streaming": "'on'"}`).
- `MAX_SLOT_WAL_KEEP_SIZE_MB`: Safety threshold for the Watchdog (default: `1024`).

### Vectorizer Settings (pgai)
- `EMBEDDING_PROVIDER`: The embedding provider to use (default: `ollama`).
- `EMBEDDING_MODEL`: The model name (default: `nomic-embed-text`).
- `EMBEDDING_DIMENSION`: Dimension of the vector (default: `768`).
- `CHUNKING_STRATEGY`: Text splitting strategy (default: `recursive_character_text_splitter`).
- `FORMATTING_TEMPLATE`: SQL template for the content before embedding (default: `$chunk`).

See `src/config.py` for all available options and defaults.

## CI/CD

The project includes a GitHub Actions workflow (`.github/workflows/ci.yml`) that performs automated building, infra setup, and testing.
