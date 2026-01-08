# Enterprise Readiness Roadmap: Search Replica

> **Note**: This roadmap has been moved to GitHub Issues. See the repository issues for tracking progress.

This document outlines the architectural and operational requirements to move this project from a functional daemon to a production-grade, enterprise-ready service.

---

## Chapter 1: Reliability & Fault Tolerance
*Ensuring the system can survive external failures without manual intervention.*

- **Readiness Probes (State-based Synchronization)**:
    - **Status**: High Priority.
    - **Implementation**: Replace all `asyncio.sleep` calls with active polling loops that verify the database state.
    - **Key Targets**:
        - **Publication Visibility**: Verify the Sink can see the Publication on the Source before creating the subscription.
        - **Slot Activation**: Ensure the replication slot exists and is ready for the subscription worker.
        - **pgai Registry Sync**: Verify `pgai` has indexed new tables in its internal metadata before attempting vectorizer creation.
        - **Extension Readiness**: Ensure `ai` and `vector` extensions are fully loaded and their tables (`ai.vectorizer`) are queryable.
        - **Subscription Health**: Wait for the subscription to move from `initializing` to `streaming` state before reporting a successful sync.
    - **Why**: Eliminates "flaky" tests and CI failures caused by post-commit lag in Postgres and Docker networking. Moves the system from "timing-based" to "state-based" reliability.
- **Fault-Tolerant Reconciler**:
    - **Status**: Completed.
    - **Implementation**: Refactored `Applier` to catch exceptions per-action. Failed targets are marked as "Failed" in `_replica_config_history` while other tables continue to reconcile.
    - **Why**: Prevents a single misconfigured table or temporary network issue from blocking the entire replica fleet.
- **Isolated Control Plane**:
    - **Status**: Completed.
    - **Implementation**: Uses `pg_advisory_xact_lock` for global concurrency control and `copy.deepcopy` for settings isolation in `PGSearchReplica`.
    - **Why**: Ensures reliable operations in multi-node deployments and prevents cross-test contamination in CI.
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
- **Migration Progress Tracking**:
    - **Implementation**: Track `total_rows` vs `pending_items` for every active vectorizer job. Calculate and expose `rows_per_second` and `estimated_time_remaining` (ETA).
    - **Why**: Critical for the Management UI. Users need to know if a "Promote to Live" action is waiting for 5 minutes or 5 hours.
- **Shadow Index Registry**:
    - **Implementation**: Expose a dedicated metadata view that lists all active "Branches" (Shadow Tables), their parent configuration, and their sync status.
    - **Why**: Allows the UI to distinguish between "Live" tables and "Experimental" branches without relying on fragile string parsing of table names.

## Chapter 3: Declarative Schema & Security
*Zero-Touch configuration where the code manages the database state.*

- **Unified Configuration & Declarative Schema**:
    - **Status**: Completed.
    - **Implementation**: Consolidated `config.py` and `config_v2.py` into a single Pydantic-based schema. Removed legacy `TableConfig`.
    - **Why**: Eliminates architectural confusion and provides a single source of truth for all pipeline definitions.
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
    - **Implementation**: Optional layer to cache embeddings for identical content strings.
    - **Why**: Significantly reduces cost and latency if the source data contains many repeating text values (e.g., category names or status updates).

## Chapter 5: Operational Lifecycle
- **Refined Shutdown Logic & Teardown**:
    - **Status**: Completed.
    - **Implementation**: `drop_subscription_completely` includes worker termination and a retry loop.
    - **Why**: Reliable logical replication management.
- **Automated Re-Sync/Recovery**:
    - **Status**: Completed (Hybrid Model).
    - **Why**: Essential for disaster recovery or after the Watchdog has performed an emergency self-destruct. The system now automatically detects missing slots and uses a combination of SQL Catch-up and Anti-Entropy to restore consistency.
- **Dynamic Publication & Filter Updates**:
    - **Implementation**: Detect changes in `PUBLICATION_COLUMNS` or `PUBLICATION_WHERE` via configuration hashing stored in the `_replica_state` table.
    - **Why**: Allows changing the scope of replication (adding columns or narrowing/widening filters) mid-operation. The system will automatically update the publication, refresh the subscription, and trigger a targeted backfill or anti-entropy sweep to reconcile existing data with the new rules.

## Chapter 6: Enterprise-Grade Source Integration
*Bridging the gap between developer automation and DBA security policies.*


- **Universal Downstream Sync (Multicast Search Architecture)**:
    - **Pattern (CQRS)**: Treat the source as the Command store and external sinks (Qdrant, Pinecone, etc.) as specialized Query stores. Postgres acts as the "Reliable Buffer" and state manager.
    - **Implementation (Outbox Handshake)**: 
        - **Registry**: Use `_sink_mirror_registry` to track downstream mirroring state and version mapping. (**Status: Core Completed**)
        - **Universal Outbox**: Implement `_sink_outbox` in the Sink DB to capture all versioned embedding changes. (**Status: Core Completed**)
        - **Mirror Triggers**: Attach standard triggers to versioned tables to clone changes into the outbox. (**Status: Core Completed**)
        - **Sink Adapters**: A plugin-based system where external engines (Qdrant, Pinecone) implement a standard `SinkAdapter` interface for batch upserts/deletes. (Qdrant: **Done**, Pinecone: **Todo**)
    - **Search Lifecycle**:
        - **Shadow Build**: Synchronize new versions (e.g., `v2`) to isolated downstream collections while `v1` remains live.
        - **SxS Validation**: Enable side-by-side search benchmarking against shadow collections before promotion.
        - **Atomic Promotion**: Use downstream-native **Aliases** to flip the "Production" pointer to the new collection once synced.
    - **Sink-Aware Client (Strategy Pattern)**:
        - **Implementation**: Update `PGSearchReplica.search()` to optionally target a configured downstream sink instead of Postgres SQL.
        - **Status**: Completed
        - **Why**: Allows swapping the underlying search infrastructure (e.g., from PG to Qdrant) with zero application code changes.
    - **Mirror Sync Handshake (Blue-Green Consistency)**:
        - **Implementation**: Ensure mirrors are 100% caught up before the Reconciler promotes a search view in Postgres. (**Status: Completed**)
    - **Why**: Ensures "at-least-once" delivery of search updates without blocking Postgres transactions. Decouples search infrastructure from database maintenance and enables seamless model/infrastructure migrations.
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
- **Direct Ingest API (Push Connector)**:
    - **Implementation**: Expose a REST/gRPC endpoint (`POST /v1/ingest`) that writes directly to the internal `_raw` queue capability.
    - **Why**: Allows users to utilize the full **SearchOps Workbench** (Branching, Diffing, Downstream Sync) for static documents (PDFs, Markdown) or data from sources that cannot support CDC (e.g., CSV uploads), treating them exactly like database rows.

## Chapter 7: Search-as-Code (Declarative Reconciliation)
*Moving from imperative setup scripts to a state-enforcement engine that treats the search replica as versioned infrastructure.*

- **Unified Reconciler Engine**:
    - **Status**: Completed.
    - **Why**: Centralizes all infrastructure logic and ensures that the system always moves towards the desired state regardless of the initial database condition.
- **State Discovery & Diffing**:
    - **Status**: Completed.
    - **Why**: Provides a Terraform-like experience where the user describes the desired search infrastructure, and the daemon calculates the necessary DDL/DML to reach that state.
- **Concurrent Index Management**:
    - **Implementation**: Automatically manage GIN (full-text) and HNSW (vector) indexes. Use `CREATE INDEX CONCURRENTLY` to ensure zero-downtime during index upgrades or re-indexing experiments.
    - **Why**: Allows users to experiment with different indexing strategies (e.g., changing HNSW `m` or `ef_construction` values) without locking the search replica.
- **Experimental Versioning (Shadow Indexing)**:
    - **Implementation**: Support multiple concurrent vectorizers/indexes for the same source table.
    - **Why**: Enables A/B testing of different embedding models or chunking strategies by populating Shadow tables/columns alongside the primary ones before switching the public View.
- **Blue-Green Data Migration (Swap Pattern)**:
    - **Status**: Completed.
    - **Why**: Ensures zero-downtime migrations for un-migratable changes like embedding dimension shifts. Prevents migration nightmares by treating derived data as versioned and disposable.
- **Self-Describing Manifest (State-as-Code)**:
    - **Status**: Completed.
    - **Why**: Turns the search replica into a self-describing system. Allows any future version of the daemon to instantly understand the on-disk state and reconcile it with the configuration.
- **Cost & Experimentation Telemetry**:
    - **Implementation**: Log build-time metrics (tokens used, total wall-time, model versions, success rates) into a dedicated `experiment_logs` table during the Shadow Build phase.
    - **Why**: Provides the data needed to evaluate the ROI of different search strategies and model upgrades before committing to a production swap.
- **Autonomous Performance Tuning**:
    - **Implementation**: Bake DBRE intelligence into the Reconciler to automatically set HNSW parameters (`m`, `ef_construction`), manage `pg_prewarm` for index buffer loading after swaps, and trigger `ANALYZE` or `REINDEX` based on data drift thresholds.
    - **Why**: Ensures peak performance for average users by automating complex database tuning. Guarantees sub-10ms search latency and zero cold-start performance hits after migrations.

## Chapter 8: Search Workbench & UX (SearchOps)
*Providing a high-level experimentation platform for Search Engineers.*

### Core Workbench Features

- **Git-style Search Versioning (Branching)**:
    - **Status**: Core Engine Completed.
    - **Why**: Treats search relevance like code. Allows safe experimentation ("dev branches") by building versioned vectorizers in the background. Swapping is controlled via the `active` flag, fulfilling the "Merge" requirement.
- **Resource & Cost Profiling**:
    - **Implementation**: Pre-flight analysis reporting the estimated indexing cost (tokens), RAM usage (HNSW), and query latency for a proposed branch.
    - **Why**: Empower engineers to make informed trade-offs between relevance quality and operational cost.
- **Side-by-Side (SxS) Diffing**:
    - **Implementation**: A split-screen UI that runs the same query against Main vs. Branch and highlights only the result differences (re-ordering, additions, deletions).
    - **Why**: Facilitates qualitative "vibe checks" for human evaluators to understand the behavioral shift of a new model.
- **Downstream Atomic Promotion (Aliases)**:
    - **Implementation**: Extend the Blue-Green swap logic to external sinks. Use target-native "Aliases" (e.g., Qdrant Aliases) to flip the "Live" pointer from `v1_collection` to `v2_collection` downstream once synced.
    - **Status**: Completed
    - **Why**: Provides zero-downtime infrastructure swaps for external search engines, mirroring the internal Postgres view-swap behavior.

### Retrieval Capabilities

- **Composable Hybrid Merging**:
    - **Implementation**: Allow "Merging" multiple branches (e.g., `keyword-branch` + `semantic-branch`) into a single "Release" View using RRF or weighted fusion.
    - **Why**: Gives users a unified interface to compose complex retrieval strategies from simple, isolated modular components.
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
