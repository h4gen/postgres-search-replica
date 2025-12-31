# Postgres Search Replica Client & Daemon

A professional-grade PostgreSQL search replica library with **real-time CDC vectorization** using native WAL streaming, `pgai`, and `pgvector`.

## High-Performance Architecture

The project is built for enterprise-scale data movement, leveraging PostgreSQL's native Change Data Capture (CDC) capabilities to ensure sub-second search synchronization with zero impact on the source database.

- **Native WAL Streaming (CDC)**: Uses PostgreSQL Logical Replication to stream changes directly from the write-ahead log. No polling, no triggers on the source, and minimal overhead.
- **PG 15 Row & Column Filtering**: Minimizes network traffic and sink load by replicating only the specific rows and columns you need for search.
- **Postgres-Native Vectorization**: Unlike external sync tools, this uses the `pgai` extension to handle vectorization *inside* the database. This ensures your embeddings are governed by the same ACID guarantees as your data.
- **Hybrid Recovery Model**: A self-healing state machine that automatically bridges data gaps using SQL keyset pagination before handing off to real-time streaming.

## Declarative Context-Aware Design

The system follows a **Declarative Design Principle**. Instead of manually managing vectors, you define the desired state, and the library orchestrates the underlying PostgreSQL extensions.

### Hybrid Recovery Model
Enterprise-grade data movement requires more than just binary streaming. This library implements a state-machine for self-healing:
1.  **LSN Anchoring**: Automatically creates replication slots on the source to bookmark the exact binary position.
2.  **SQL Catch-up**: Uses idempotent Keyset Pagination (`WHERE id > last_id`) to bridge data gaps without holding the Source WAL open.
3.  **Anti-Entropy (Ghost Cleaner)**: Performs checksum-based verification of ID chunks to find and delete "Ghost Records" (rows deleted on Source while the replica was offline).
4.  **Zero-Loss Handover**: Seamlessly transitions from SQL catch-up to real-time binary streaming.

### Context-Aware Embedding
Traditional vector search often loses context when long documents are split into smaller chunks. This library uses a declarative template system to ensure every vector remains semantically linked to its source.

1.  **The Work Column (`CONTENT_COLUMN`)**: This is your primary text data (e.g., `description`). It is automatically processed and split into pieces according to your `CHUNKING_STRATEGY`.
2.  **Metadata Columns**: You can replicate additional metadata columns (e.g., `name`, `category`) that are not chunked themselves.
3.  **The Template (`FORMATTING_TEMPLATE`)**: These pieces are combined. For every chunk generated from the description, the template enriches it with metadata.

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
    volumes:
      # CRITICAL: Persist the managed Postgres data to avoid re-embedding on restart
      - replica_data:/var/lib/postgresql/.local/share/pg-search-replica
    ports:
      - "54322:54322"  # Exposed for search queries

volumes:
  replica_data:
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

- **Native CDC Streaming**: High-performance binary replication from Source WAL to Sink, ensuring sub-second latency for search results.
- **Hybrid Recovery & Self-Healing**: Automatically detect missing replication slots and bridge the gap using SQL catch-up followed by an LSN-anchored handover to native replication.
- **Anti-Entropy (Ghost Cleaner)**: Checksum-based sweep to identify and prune records that were hard-deleted from the source while the daemon was offline.
- **Source Protection (Watchdog)**: Actively monitors replication lag. If the lag exceeds `MAX_SLOT_WAL_KEEP_SIZE_MB`, it triggers a self-destruct to ensure the Source DB never runs out of disk space.
- **Zero-Touch & Low Privilege**: No `SUPERUSER` rights required on the source. All protection and reconciliation logic is handled by the replica sidecar.
- **Dynamic Type Detection**: Automatically detect primary key types (including **UUID**, **BIGINT**, **TEXT**) and schema from the source database at runtime.
- **Context-Aware Embedding**: Automatically combine metadata (e.g., `name`) with chunked text (e.g., `description`) using Python-style templates to preserve context across all vectors.
- **Connection Pooling**: Uses `psycopg-pool` for robust management of database connections, preventing exhaustion under high load.
- **Observability Hub**: Built-in FastAPI server providing health checks and real-time Prometheus metrics.
- **Structured JSON Logging**: Native support for single-line JSON logging, ready for ingestion by Datadog, ELK, or Grafana Loki.
- **Managed Lifecycle**: Automatically handles replication slot creation/cleanup and `pgai` worker management.
- **Smart Reconciliation**: Only updates embeddings when source data actually changes, leveraging `pgai` native state tracking.
- **Zero-Touch Config**: Automatically synchronizes publication columns and filters from Python settings to the database on startup.

## Roadmap

See our [Enterprise Readiness Roadmap](roadmap.md) for planned features, including:
- **Fault Tolerance**: Poison Pill handling and Circuit Breakers.
- **Observability**: Prometheus metrics and Structured JSON Logging.
- **Enterprise Source Integration**: Pre-provisioned infra and Multi-Engine Polling (Oracle, MySQL, etc.).
- **Search UX**: Hybrid Search with RRF (Reciprocal Rank Fusion).

## Configuration & API Reference

All settings can be configured via environment variables (e.g., `SOURCE_URL`) or a `.env` file. The library uses `pydantic-settings` for robust validation.

### 1. Core Infrastructure
| Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `SOURCE_URL` | `str` | **Required** | Connection string for the source PostgreSQL database. |
| `SINK_URL` | `str` | `local` | Connection string for the sink (search) database. Set to `local` to use the managed sidecar Postgres. |
| `LOCAL_PORT` | `int` | `54322` | Port for the managed Postgres instance when `SINK_URL=local`. |
| `PG_REPLICA_DIR` | `Path` | `~/.local/share/...` | Base directory for storage, WAL data, and logs in local mode. |
| `SUBSCRIPTION_SOURCE_URL` | `str` | `SOURCE_URL` | The URL used *by the Sink DB* to reach the Source. Useful for Docker networking (e.g. `postgresql://host.docker.internal...`). |

### 2. Schema & Table Mapping
| Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `SOURCE_TABLE` | `str` | `products` | The table name on the source database to replicate. |
| `SINK_RAW_TABLE` | `str` | `products` | The name of the raw landing table in the sink database. |
| `SINK_REPLICA_TABLE` | `str` | `products_replica` | The name of the search-ready View that joins raw data with embeddings. |
| `ID_COLUMN` | `str` | `id` | The Primary Key column (must be numeric, UUID, or TEXT). |
| `CONTENT_COLUMN` | `str` | `description` | The source column that will be chunked and vectorized. |
| `TARGET_CONTENT_COLUMN` | `str` | `transformed_description`| The name of the text column in the final search View. |

### 3. Replication & CDC Settings
| Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `PUBLICATION_NAME` | `str` | `pub_products` | Name of the PostgreSQL publication on the source. |
| `PUBLICATION_COLUMNS`| `list` | `["id", "name", "description"]` | Columns to include in the replication stream. |
| `PUBLICATION_WHERE` | `str` | `None` | Optional SQL filter clause for row-level replication (PG 15+). |
| `SUBSCRIPTION_NAME` | `str` | `sub_products` | Name of the PostgreSQL subscription on the sink. |
| `SUBSCRIPTION_OPTIONS`| `dict` | `{"streaming": "'on'"}` | Additional options passed to `CREATE SUBSCRIPTION`. |
| `MAX_SLOT_WAL_KEEP_SIZE_MB` | `int` | `1024` | Safety limit. If replication lag exceeds this, the sidecar self-destructs to protect source disk. |
| `BATCH_SIZE` | `int` | `50` | Batch size for initial SQL catch-up and anti-entropy sweeps. |

### 4. Vectorization & Context-Aware Embedding (pgai)
| Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `EMBEDDING_PROVIDER` | `str` | `ollama` | The `pgai` provider (`ollama`, `openai`, `anthropic`, etc.). |
| `EMBEDDING_MODEL` | `str` | `nomic-embed-text` | The embedding model name. |
| `EMBEDDING_DIMENSION`| `int` | `768` | Dimension of the vectors generated by the model. |
| `EMBEDDING_COLUMN` | `str` | `embedding` | Name of the vector column in the embedding table. |
| `CHUNKING_STRATEGY` | `str` | `recursive_character_text_splitter` | `pgai` strategy for splitting the `CONTENT_COLUMN`. |
| `FORMATTING_TEMPLATE`| `str` | *See below* | Python template for metadata enrichment (e.g., `Product: $name Description: $chunk`). |

### 5. Observability & System
| Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `OBSERVABILITY_HOST` | `str` | `0.0.0.0` | Binding host for the built-in FastAPI metrics/health server. |
| `OBSERVABILITY_PORT` | `int` | `8000` | Port for the observability server. |
| `NOTIFY_CHANNEL` | `str` | `new_raw_data` | Internal PG channel for real-time signaling. |

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

## Development

We use a `Makefile` to encapsulate best practices and common tasks.

### Setup
1. Install [uv](https://github.com/astral-sh/uv).
2. Sync dependencies: `uv sync --extra test`.

### Common Commands
- `make dev` - Spin up the local development environment (Source + Sink + Daemon).
- `make test` - Run integration tests.
- `make lint` - Run `ruff` linter and formatter checks.
- `make type-check` - Run `ty` type checker.
- `make clean` - Full cleanup including database volumes and temporary files.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on our development workflow and how to submit pull requests.

## License

This project is licensed under **AGPL v3**.

## Enterprise & Commercial Licensing

Building a closed-source SaaS or need a commercial license? Please **[Contact Me / Sponsor via GitHub](https://github.com/sponsors/h4gen)** to discuss enterprise support and alternative licensing options.
