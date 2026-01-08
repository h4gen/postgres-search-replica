# Postgres Search Replica Client & Daemon

A professional-grade PostgreSQL search replica library with **real-time CDC vectorization** using native WAL streaming, `pgai`, and `pgvector`.

## High-Performance Architecture

The project is built for enterprise-scale data movement, leveraging PostgreSQL's native Change Data Capture (CDC) capabilities to ensure sub-second search synchronization with zero impact on the source database.

- **Fault-Tolerant Reconciler**: A production-grade state enforcement engine. It reconciles your desired state (configs) with the actual database infrastructure, handling per-target failures gracefully so one broken table doesn't block your entire fleet.
- **Isolated Control Plane**: Uses `pg_advisory_xact_lock` for distributed concurrency control and `copy.deepcopy` for settings isolation, ensuring multiple daemon instances or test runs never collide.
- **Native WAL Streaming (CDC)**: Uses PostgreSQL Logical Replication to stream changes directly from the write-ahead log. No polling, no triggers on the source, and minimal overhead.
- **PG 15 Row & Column Filtering**: Minimizes network traffic and sink load by replicating only the specific rows and columns you need for search.
- **Postgres-Native Vectorization**: Unlike external sync tools, this uses the `pgai` extension to handle vectorization *inside* the database. This ensures your embeddings are governed by the same ACID guarantees as your data.
- **Universal Outbox (Multicast Sync)**: Acts as a high-reliability bridge to external search engines (Qdrant, Pinecone, etc.). Changes are captured in a transactional outbox and synced downstream with at-least-once delivery guarantees.
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
- **Declarative Blue-Green Swaps**: Zero-downtime search index updates. The system automatically builds new vector versions in the background and only swaps the public view once synchronization is complete.
- **Multicast Sync Engine**: A plugin-based system to mirror your vectorized data to external engines like Qdrant or Pinecone.

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
        # Default engine (configured or postgres)
        results = await replica.search("AI research")
        
        # Explicitly target Qdrant
        results_q = await replica.search("AI research", engine="qdrant")
        
        for res in results:
            print(f"Content: {res['content']}, Distance: {res['distance']}")
```

### Unified Search API
The client supports different search backends via the **Strategy Pattern**. This allows you to leverage specialized vector engines like Qdrant while maintaining a consistent application interface.

```python
from pg_replica import connect
from pg_replica.config import SearchPipeline, IngestConfig, PipelineConfig, EmbeddingConfig, StorageConfig, MirrorConfig

# Configure a table to use Qdrant by default
replica = connect(
    pipelines={
        "products": SearchPipeline(
            ingest=IngestConfig(table="products", columns=["id", "name", "description"]),
            pipeline=PipelineConfig(
                template="Product: $name Description: $chunk",
                embedding=EmbeddingConfig(provider="ollama", model="nomic-embed-text", dimension=768)
            ),
            storage=StorageConfig(
                mirrors=[MirrorConfig(id="m1", type="qdrant", config={"url": "http://localhost:6333"})]
            )
        )
    }
)

# This query executes directly against Qdrant!
res = await replica.search("high-tech accessories")
```

## Key Features

- **Unified Configuration**: A single, declarative `config.py` using Pydantic for validation. No more split between settings and table schemas.
- **Fault-Tolerant Reconciler**: Centralized state management that catches and reports errors per-target, preventing cascading failures.
- **Transactional Control Plane**: Distributed locking and settings isolation for reliable multi-node operations.
- **Production-Grade Teardown**: Robust logical replication cleanup with worker termination and retry logic to prevent zombie slots.
- **Native CDC Streaming**: High-performance binary replication from Source WAL to Sink, ensuring sub-second latency for search results.
- **Hybrid Recovery & Self-Healing**: Automatically detect missing replication slots and bridge the gap using SQL catch-up followed by an LSN-anchored handover to native replication.
- **Anti-Entropy (Ghost Cleaner)**: Checksum-based sweep to identify and prune records that were hard-deleted from the source while the daemon was offline.
- **Source Protection (Watchdog)**: Actively monitors replication lag. If the lag exceeds `MAX_SLOT_WAL_KEEP_SIZE_MB`, it triggers a self-destruct to ensure the Source DB never runs out of disk space.
- **Dynamic Type Detection**: Automatically detect primary key types (including **UUID**, **BIGINT**, **TEXT**) and schema from the source database at runtime.
- **Context-Aware Embedding**: Automatically combine metadata (e.g., `name`) with chunked text (e.g., `description`) using Python-style templates to preserve context across all vectors.
- **Blue-Green Data Migration (Swap Pattern)**: Zero-downtime search index updates. The system automatically builds new vector versions in the background and only swaps the public view once synchronization is complete.
- **Multicast CDC Bridge**: Transactionally capture embeddings and sync them to external vector databases (Qdrant, etc.) with persistent state tracking and retry logic.
- **Unified Search Interface (Strategy Pattern)**: Switch between `postgres` and `qdrant` search engines dynamically with a single line of code.

## Configuration & API Reference

All settings can be configured via environment variables (e.g., `SOURCE_URL`) or a `.env` file. Complex pipeline configurations are best defined in Python or via JSON/YAML environment variables.

### 1. Core Infrastructure
| Environment Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `SOURCE_URL` | `str` | **Required** | Connection string for the source PostgreSQL database. |
| `SINK_URL` | `str` | `local` | Connection string for the sink database. `local` uses the managed sidecar. |
| `LOCAL_PORT` | `int` | `54322` | Port for the managed Postgres instance when `SINK_URL=local`. |
| `SUBSCRIPTION_SOURCE_URL` | `str` | `SOURCE_URL` | The URL used *by the Sink DB* to reach the Source (internal Docker networking). |
| `MAX_SLOT_WAL_KEEP_SIZE_MB` | `int` | `1024` | Safety limit for Source WAL retention before self-destruct. |

### 2. Multi-Pipeline Configuration
Pipeline schemas are defined in `src/pg_replica/config.py`. You can configure multiple tables simultaneously using the `pipelines` dictionary.

#### Full Python Example
This example shows a complex setup with a custom primary key (`p_key`), row filtering, and a Qdrant mirror.

```python
from pg_replica.config import (
    SearchPipeline, IngestConfig, PipelineConfig, 
    EmbeddingConfig, StorageConfig, MirrorConfig
)

config = {
    "pipelines": {
        "legacy_products": SearchPipeline(
            # 1. Ingest: How to pull data from Source
            ingest=IngestConfig(
                table="products_v1",      # Source table name
                p_key="sku_uuid",         # REQUIRED if not 'id'. Used for catch-up & sync.
                columns=["sku_uuid", "name", "category", "description"],
                filter="active = true"    # SQL where clause for CDC and Catch-up
            ),
            # 2. Pipeline: How to transform text to vectors
            pipeline=PipelineConfig(
                template="Name: $name\nCategory: $category\nContext: $chunk",
                content_column="description",
                embedding=EmbeddingConfig(
                    provider="openai",        # Using OpenAI instead of Ollama
                    model="text-embedding-3-small",
                    dimension=1536,
                    api_key_name="OPENAI_API_KEY" # Name of environment variable in Sink DB
                )
            ),
            # 3. Storage: Where to put the results
            storage=StorageConfig(
                postgres={"profile": "hybrid"},
                mirrors=[
                    MirrorConfig(
                        id="qdrant_main",
                        type="qdrant",
                        config={
                            "url": "http://qdrant:6333", 
                            "prefix": "prod_",
                            "api_key": "your-qdrant-secret-key" # Passed to Qdrant adapter
                        }
                    )
                ]
            )
        )
    }
}
```

### 3. Secret Management
The library handles secrets in two ways depending on the target:

1.  **Internal Vectorizers (OpenAI, VoyageAI, etc.)**: These are managed by `pgai` inside the database. You provide the **`api_key_name`**, which is the name of an environment variable (or a Postgres session variable) that the Sink DB can access.
2.  **External Mirrors (Qdrant, Pinecone)**: These are managed by the `MirrorWorker` daemon. You can pass the **`api_key`** directly in the mirror's `config` dictionary.

> [!TIP]
> For production safety, use environment variable expansion in your configuration loader rather than hardcoding keys in Python files.

### 3. Schema Reference (`IngestConfig`)
| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `table` | `str` | **Required** | The table name on the source database. |
| `p_key` | `str` | `id` | **CRITICAL**: The Primary Key. Used for Keyset Pagination and CDC tracking. |
| `columns`| `list` | `[]` | List of columns to replicate. `p_key` is automatically added. |
| `filter` | `str` | `None` | Optional SQL `WHERE` clause (e.g., `is_deleted = false`). |
| `schema_name`| `str`| `public`| Source schema. |

### 4. Observability & System
You can configure a list of external mirrors in your `TableConfig`. The library will ensure these stay in sync with your Postgres embeddings.

```python
tables={
    "products": {
        "mirrors": [
            {
                "id": "qdrant_prod",
                "type": "qdrant",
                "url": "http://qdrant:6333",
                "prefix": "search_v1_"
            }
        ]
    }
}
```

### 6. Observability & System
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
