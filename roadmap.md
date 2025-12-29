# Enterprise Readiness Roadmap: Search Replica

This document outlines the architectural and operational requirements to move this project from a functional daemon to a production-grade, enterprise-ready service.

---

## Chapter 1: Reliability & Fault Tolerance
*Ensuring the system can survive external failures without manual intervention.*

- **Robust Retry Mechanism**:
    - **Implementation**: Integrate `tenacity` or similar library for exponential backoff on Database connections and Vectorizer API calls.
    - **Why**: Prevents the daemon from crashing during transient network blips or provider rate-limits.
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
    - **Implementation**: Perform "Pre-flight checks" on the Source DB to verify that configured columns exist and data types are compatible.
    - **Why**: Prevents the pipeline from starting in a broken state or crashing unexpectedly when the source schema diverges from the replica configuration.
- **Connection Pooling**:
    - **Implementation**: Use `psycopg_pool` instead of raw `AsyncConnection`.
    - **Why**: Prevents connection exhaustion and reduces the overhead of repeatedly opening/closing handshakes with the Sink DB.
- **Secrets Management**:
    - **Implementation**: Support fetching `SOURCE_URL` and `SINK_URL` from a secret manager (AWS Secrets Manager, HashiCorp Vault) rather than plain environment variables.
    - **Why**: Compliance and security best practices for handling database credentials in enterprise environments.

## Chapter 4: Performance & Scalability
- **Decoupled Producer/Consumer**:
    - **Implementation**: Use an internal `asyncio.Queue` to separate the **Fetch** (fast) from the **Vectorization** (slow/API-bound).
    - **Why**: Allows fetching the next batch while the current one is still waiting for an LLM response, maximizing throughput.
- **Embedding Cache**:
    - **Implementation**: Optional Redis layer to cache embeddings for identical content strings.
    - **Why**: Significantly reduces cost and latency if the source data contains many repeating text values (e.g., category names or status updates).

## Chapter 5: Operational Lifecycle
- **Refined Shutdown Logic**:
    - **Implementation**: Differentiate between `SIGTERM` (temporary restart) and a full `DECOMMISSION` flag.
    - **Why**: Currently, the system drops the subscription on every restart. In production, you often want to keep the slot during a quick upgrade to avoid a full data re-sync.
- **Automated Re-Sync/Recovery**:
    - **Implementation**: Logic to detect if a subscription is missing and trigger a `COPY` command to rebuild the replica from scratch safely.
    - **Why**: Essential for disaster recovery or after the Watchdog has performed an emergency self-destruct.