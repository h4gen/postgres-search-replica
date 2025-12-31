# Enterprise Readiness Roadmap: Search Replica

> **Note**: This roadmap has been moved to GitHub Issues. See the repository issues for tracking progress.

This document outlines the architectural and operational requirements to move this project from a functional daemon to a production-grade, enterprise-ready service.

---

## Chapter 1: Reliability & Fault Tolerance
*Ensuring the system can survive external failures without manual intervention.*

- **pgai Orchestration**:
    - **Status**: Completed.
    - **Why**: Replaced custom Python loops with `pgai` background workers. This provides database-native retries, dead-letter queues, and atomic state tracking for embeddings.
- **Robust Retry Mechanism**:
    - **Implementation**: `pgai` handles vectorizer retries internally. The sidecar only needs to handle its own connection retries.
- **Poison Pill Handling (DLQ)**:
    - **Implementation**: Add a `failure_count` and `last_error` column to the `sink_raw_table`. 
    - **Why**: If a specific row causes a transformation crash (e.g., malformed data), the system should skip it after $N$ attempts rather than blocking the entire pipeline indefinitely.
- **Circuit Breaker Pattern**:
    - **Implementation**: Monitor failure rates of external vectorizers. If failures exceed a threshold, temporarily pause API calls.
    - **Why**: Protects against cascading failures and prevents wasting API credits when a service is known to be down.

## Chapter 2: Observability & Monitoring
- **Structured JSON Logging**:
    - **Implementation**: Switch from standard string logging to a structured format (e.g., `python-json-logger`).
    - **Why**: Allows modern log aggregators (Datadog, ELK, Grafana Loki) to index metadata like `batch_id`, `latency`, and `row_count` for better querying.
- **Prometheus Metrics**:
    - **Implementation**: Export real-time metrics for:
        - `replication_lag_bytes`: Current distance from Source WAL.
        - `processing_latency_seconds`: Time spent in Polars vs. Vectorizer.
        - `rows_processed_total`: Cumulative throughput.
    - **Why**: Enables alerting before the Watchdog self-destructs.
- **Health & Readiness Probes**:
    - **Implementation**: Add a lightweight HTTP endpoint (e.g., using `FastAPI` or a background thread).
    - **Why**: Required for Kubernetes/orchestrator liveness checks to automate restarts.

## Chapter 3: Declarative Schema & Security
*Zero-Touch configuration where the code manages the database state.*

- **Automated Schema Evolution**:
    - **Implementation**: Instead of static `CREATE TABLE`, implement a reconciliation loop at startup. Compare `PUBLICATION_COLUMNS` from settings with the existing sink table schema.
    - **Why**: Allows users to add columns to the replication list via environment variables without manually running SQL migrations. The daemon auto-applies `ALTER TABLE ... ADD COLUMN`.
- **Upstream Change Detection**:
    - **Implementation**: Perform Pre-flight checks on the Source DB to verify that configured columns exist and data types are compatible.
    - **Why**: Prevents the pipeline from starting in a broken state or crashing unexpectedly when the source schema diverges from the replica configuration.
- **Connection Pooling**:
    - **Status**: Completed.
    - **Why**: Uses `psycopg_pool` for robust management of database connections.
- **Secrets Management**:
    - **Implementation**: Support fetching `SOURCE_URL` and `SINK_URL` from a secret manager (AWS Secrets Manager, HashiCorp Vault) rather than plain environment variables.
    - **Why**: Compliance and security best practices for handling database credentials in enterprise environments.

## Chapter 4: Performance & Scalability
- **Decoupled Compute**: 
    - **Status**: Implemented via `ai-worker`.
    - **Why**: Moving vectorization to dedicated worker containers allows scaling embedding compute independently of the database and sidecar.
- **Embedding Cache**:
    - **Implementation**: Optional Redis layer to cache embeddings for identical content strings.
    - **Why**: Significantly reduces cost and latency if the source data contains many repeating text values (e.g., category names or status updates).

## Chapter 5: Operational Lifecycle
- **Refined Shutdown Logic**:
    - **Implementation**: Differentiate between `SIGTERM` (temporary restart) and a full `DECOMMISSION` flag.
    - **Why**: Currently, the system drops the subscription on every restart. In production, you often want to keep the slot during a quick upgrade to avoid a full data re-sync.
- **Automated Re-Sync/Recovery**:
    - **Status**: Completed (Hybrid Model).
    - **Why**: Essential for disaster recovery or after the Watchdog has performed an emergency self-destruct. The system now automatically detects missing slots and uses a combination of SQL Catch-up and Anti-Entropy to restore consistency.
- **Dynamic Publication & Filter Updates**:
    - **Implementation**: Detect changes in `PUBLICATION_COLUMNS` or `PUBLICATION_WHERE` via configuration hashing stored in the `_replica_state` table.
    - **Why**: Allows changing the scope of replication (adding columns or narrowing/widening filters) mid-operation. The system will automatically update the publication, refresh the subscription, and trigger a targeted backfill or anti-entropy sweep to reconcile existing data with the new rules.

## Chapter 6: Enterprise-Grade Source Integration
*Bridging the gap between developer automation and DBA security policies.*

- **Pre-provisioned Infrastructure Support**:
    - **Implementation**: Add a `SOURCE_MANAGED_BY_ADMIN` (boolean) flag.
    - **Why**: In strict environments, the daemon will not have permission to `CREATE PUBLICATION` or `CREATE SLOT`. This mode skips all DDL/Management calls and assumes the pipeline is already ready on the source.
- **Read-Only Replica Streaming (PG 16+)**:
    - **Implementation**: Optimize `setup_source` to detect if the source is a standby and skip write operations while still attempting logical streaming.
    - **Why**: Allows users to point the daemon at a read replica to completely isolate the primary production DB from replication load and Watchdog risk.
- **Periodic SQL Polling (Legacy/Strict Fallback)**:
    - **Implementation**: If logical replication is unavailable (PG < 16 on standby) or access is denied, fall back to repeating the `run_sql_catchup` logic on a configurable interval (e.g., every 60s).
    - **Why**: Provides a best effort search replica even when the admin refuses to grant anything beyond a standard Read-Only user.
- **Multi-Engine Polling (Oracle, MySQL, SQL Server)**:
    - **Implementation**: Abstract the `Source` connection using a generic engine (e.g., SQLAlchemy/pyodbc). Implement engine-specific adapters for schema discovery and keyset pagination.
    - **Why**: Allows creating a unified Postgres-based search index for legacy or non-Postgres systems without requiring CDC plugins or expensive middleware.
- **Least-Privilege DBA Scripts**:
    - **Implementation**: Provide a `docs/dba_setup.sql` template in the repository.
    - **Why**: Gives enterprises a clear audit trail of exactly what permissions are needed, making security approval significantly faster.

## Chapter 7: Search-as-Code (Declarative Reconciliation)
*Moving from imperative setup scripts to a state-enforcement engine that treats the search replica as versioned infrastructure.*

- **Unified Reconciler Engine**:
    - **Implementation**: Refactor the `Orchestrator` to use a `Reconciler` class that follows a Plan/Apply lifecycle. It calculates the delta between the desired `Settings` and the current database state.
    - **Why**: Centralizes all infrastructure logic and ensures that the system always moves towards the desired state regardless of the initial database condition.
- **State Discovery & Diffing**:
    - **Implementation**: Implement a Plan phase at startup. The daemon inspects both Source and Sink (schema, indexes, vectorizers) and compares them against the `Settings`.
    - **Why**: Provides a Terraform-like experience where the user describes the desired search infrastructure, and the daemon calculates the necessary DDL/DML to reach that state.
- **Concurrent Index Management**:
    - **Implementation**: Automatically manage GIN (full-text) and HNSW (vector) indexes. Use `CREATE INDEX CONCURRENTLY` to ensure zero-downtime during index upgrades or re-indexing experiments.
    - **Why**: Allows users to experiment with different indexing strategies (e.g., changing HNSW `m` or `ef_construction` values) without locking the search replica.
- **Experimental Versioning (Shadow Indexing)**:
    - **Implementation**: Support multiple concurrent vectorizers/indexes for the same source table.
    - **Why**: Enables A/B testing of different embedding models or chunking strategies by populating Shadow tables/columns alongside the primary ones before switching the public View.
- **Blue-Green Data Migration ( Swap Pattern)**:
    - **Implementation**: Instead of in-place `ALTER TABLE` for complex changes (like model or dimension updates), the daemon implements a Blue-Green deployment for tables. It builds the new version in the background and performs an atomic View swap once `pgai` reports 100% sync.
    - **Why**: Ensures zero-downtime migrations for un-migratable changes like embedding dimension shifts. Prevents migration nightmares by treating derived data as versioned and disposable.
- **Self-Describing Manifest (State-as-Code)**:
    - **Implementation**: Store the full JSON manifest of the desired search configuration (models, templates, columns, filters) and its hash within the `_replica_state` table.
    - **Why**: Turns the search replica into a self-describing system. Allows any future version of the daemon to instantly understand the on-disk state and reconcile it with the current configuration, similar to a `terraform.tfstate` file.
- **Cost & Experimentation Telemetry**:
    - **Implementation**: Log build-time metrics (tokens used, total wall-time, model versions, success rates) into a dedicated `experiment_logs` table during the Shadow Build phase.
    - **Why**: Provides the data needed to evaluate the ROI of different search strategies and model upgrades before committing to a production swap.
- **Autonomous Performance Tuning**:
    - **Implementation**: Bake DBRE intelligence into the Reconciler to automatically set HNSW parameters (`m`, `ef_construction`), manage `pg_prewarm` for index buffer loading after swaps, and trigger `ANALYZE` or `REINDEX` based on data drift thresholds.
    - **Why**: Ensures peak performance for average users by automating complex database tuning. Guarantees sub-10ms search latency and zero cold-start performance hits after migrations.

## Chapter 8: Search UX (Hybrid & Ranked Retrieval)
*Providing a high-level, one-query interface for complex search strategies.*

- **Declarative Search Profiles**:
    - **Implementation**: Allow users to define search strategies (e.g., `default_hybrid`, `vector_heavy`) in the configuration, specifying weights for dense vectors, sparse vectors, and full-text search.
    - **Why**: Decouples the mathematical complexity of ranking from the application logic. The app just asks for a profile, and the database handles the ranking.
- **Automated RRF (Reciprocal Rank Fusion)**:
    - **Implementation**: Automatically generate the SQL math for RRF within the search View or a dedicated PostgreSQL function.
    - **Why**: RRF is the industry standard for hybrid search but is difficult to write correctly in raw SQL. Automating this ensures optimal retrieval quality with zero developer overhead.
- **Native Sparse Vector Support (pgvector)**:
    - **Implementation**: Leverage `pgvector`'s native `sparsevec` type and HNSW indexing for learned sparse representations (e.g., SPLADE) or BM25-style scores.
    - **Why**: Standardizes on `pgvector` for all vector operations. By supporting learned sparse vectors alongside dense vectors, the system can achieve state-of-the-art retrieval that combines semantic depth with keyword precision.
- **Parameterized Search Functions**:
    - **Implementation**: Instead of static views, generate PostgreSQL functions that allow passing weights or search parameters at query time.
    - **Why**: Gives power users the freedom to tune search relevance dynamically without redeploying the daemon or re-syncing data.
