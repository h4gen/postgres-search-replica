# Postgres Search Replica Client & Daemon

A professional-grade PostgreSQL search replica library with real-time vectorization using `pgai`, source database protection, and `pgvector` support.

## Architecture & Declarative Design

The project is built with a strong separation of concerns and follows a **Declarative "Chunk Decoration" Principle**. Instead of manually managing vectors, you define the desired state, and the library orchestrates the underlying PostgreSQL `pgai` and `pgvector` extensions.

### The "Chunk Decoration" Principle

Traditional vector search often loses context when long documents are split into smaller chunks. This library uses a declarative template system to ensure every vector remains semantically linked to its source.

1.  **The Work Column (`CONTENT_COLUMN`)**: This is your primary text data (e.g., `description`). It is automatically processed and split into pieces according to your `CHUNKING_STRATEGY`.
2.  **Decoration Columns**: You can replicate additional metadata columns (e.g., `name`, `category`) that are not chunked themselves.
3.  **The Template (`FORMATTING_TEMPLATE`)**: These pieces are combined. For every chunk generated from the description, the template "decorates" it with metadata.

**Example Logic:**
*   **Row**: `name="Smart Watch"`, `description="A water-resistant... [long text] ...with heart rate monitor."`
*   **Template**: `"Product: $name Description: $chunk"`
*   **Resulting Vektor 1**: `"Product: Smart Watch Description: A water-resistant..."`
*   **Resulting Vektor 2**: `"Product: Smart Watch Description: ...with heart rate monitor."`

This ensures that even a small chunk from the middle of a description carries the identity of the product, significantly improving search relevance.

## Components

- **Client Interface (`PGSearchReplica`)**: The primary entry point for applications. Handles querying, status monitoring, and lifecycle management.
- **Orchestrator**: Manages the infrastructure layer, including local PostgreSQL instances (in local mode), `pgai` background workers, and the replication watchdog.
- **Native Bridge**: Uses PostgreSQL Native Logical Replication for efficient data movement from Source to Sink.
- **pgai & pgvector**: Database-native vectorization and storage, ensuring embeddings are always in sync with your source data.

## Quick Start (Local Mode)

The easiest way to get started is using the **Local Mode**, where the library manages its own dedicated PostgreSQL instance for you.

```python
import asyncio
from pg_replica import connect

async def main():
    # 'local' sink is the default. sync=True starts the private PG instance 
    # and handles all setup automatically.
    async with connect(
        source_url="postgresql://user:pass@production-db:5432/dbname",
        publication_columns=["id", "description", "name"],
        sync=True
    ) as replica:
        # Wait for initial replication and vectorization...
        print("Replica is ready!")
        
        # Perform semantic search
        results = await replica.search("autonomous productivity tools")
        for res in results:
            print(f"ID: {res['id']}, Content: {res['content']}, Score: {res['distance']}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Production Deployment (Docker)

In production, the search replica daemon should run as a managed service using the provided `Dockerfile`. This ensures that background replication, `pgai` workers, and the safety watchdog are always active.

### Using Docker Compose

The most robust way to deploy is using Docker Compose. The daemon container manages its own internal PostgreSQL (in local mode) or connects to an existing one.

```yaml
services:
  search-replica:
    image: pg-search-replica:latest  # Build from Dockerfile
    environment:
      - SOURCE_URL=postgresql://user:pass@prod-db:5432/dbname
      - SINK_URL=local  # Uses internal managed Postgres
      - PUBLICATION_COLUMNS=id,description,name
      - OLLAMA_HOST=http://ollama:11434
    ports:
      - "54322:54322"  # Exposed for search queries
```

### Self-Hosted Replica (External Postgres)

If you already have a PostgreSQL instance with `pgai` and `pgvector` and want the daemon to use it as the sink:

```bash
docker run -e SOURCE_URL="postgresql://..." \
           -e SINK_URL="postgresql://external-replica:5432/dbname" \
           pg-search-replica:latest
```

## Client Library Usage

Once the daemon is running (via Docker), your application can use the `PGSearchReplica` client in **Query Only** mode to perform searches. By default, `connect` starts in query-only mode (`sync=False`), making it safe to use with existing databases.

```python
from pg_replica import connect

async def search_example():
    # Safe-by-default: sync=False prevents starting redundant workers or local PG
    async with connect(sink_url="postgresql://localhost:54322/postgres") as replica:
        results = await replica.search("AI research")
        for res in results:
            print(f"Content: {res['content']}, Distance: {res['distance']}")
```

## Key Features

- **Declarative Chunk Decoration**: Automatically combine metadata (e.g., `name`) with chunked text (e.g., `description`) using Python-style templates to preserve context across all vectors.
- **Source Protection (Watchdog)**: Actively monitors replication lag. If the lag exceeds `MAX_SLOT_WAL_KEEP_SIZE_MB`, it triggers a self-destruct to ensure the Source DB never runs out of disk space.
- **Connection Pooling**: Uses `psycopg-pool` for robust management of database connections, preventing exhaustion under high load.
- **Observability Hub**: Built-in FastAPI server providing health checks and real-time Prometheus metrics.
- **Structured JSON Logging**: Native support for single-line JSON logging, ready for ingestion by Datadog, ELK, or Grafana Loki.
- **Managed Lifecycle**: Automatically handles replication slot creation/cleanup and `pgai` worker management.
- **Smart Reconciliation**: Only updates embeddings when source data actually changes, leveraging `pgai` native state tracking.
- **Zero-Touch Config**: Automatically synchronizes publication columns and filters from Python settings to the database on startup.

## Development

We use a `Makefile` to encapsulate best practices and common tasks.

### Setup
1. Install [uv](https://github.com/astral-sh/uv).
2. Sync dependencies: `uv sync --extra test`.

### Common Commands
- `make dev` - Spin up the local development environment (Source + Sink + Daemon).
- `make test` - Run integration tests.
- `make lint` - Run `ruff` linter and formatter checks.
- `make type-check` - Run `mypy` type checker.
- `make clean` - Full cleanup including database volumes and temporary files.

## Configuration

Settings are managed via Pydantic and can be overridden by environment variables or a `.env` file.

### Primary Settings
- `SOURCE_URL`: URL of the source PostgreSQL database.
- `SINK_URL`: URL of the sink database (default: `local`).
- `LOCAL_PORT`: Port for the internal managed Postgres (default: `54322`).
- `PUBLICATION_COLUMNS`: List of columns to replicate (default: `["id", "name", "description"]`).
- `MAX_SLOT_WAL_KEEP_SIZE_MB`: Safety threshold for the Watchdog (default: `1024`).
- `OBSERVABILITY_HOST`: Host for the health/metrics server (default: `0.0.0.0`).
- `OBSERVABILITY_PORT`: Port for the health/metrics server (default: `8000`).

### Vectorizer Settings
- `EMBEDDING_MODEL`: The model name (default: `nomic-embed-text`).
- `EMBEDDING_DIMENSION`: Dimension of the vector (default: `768`).

See `src/pg_replica/config.py` for all available options.

## Observability

The library includes a built-in observability server (FastAPI) that starts automatically with the daemon.

### Endpoints
- **GET `/health`**: Returns `{"status": "ok"}`. Used for liveness and readiness probes.
- **GET `/metrics`**: Exports Prometheus-formatted metrics including:
    - `replication_lag_mb`: Current WAL distance from the source database.
    - `pgai_pending_items`: Number of items currently queued for vectorization (labeled by table).

### Structured Logging
All logs are output as single-line JSON objects by default. This ensures seamless integration with modern logging infrastructure (Datadog, Grafana Loki, ELK):
```json
{"asctime": "2025-12-30 19:30:17,123", "levelname": "INFO", "name": "pg_replica.main", "message": "Daemon started."}
```
