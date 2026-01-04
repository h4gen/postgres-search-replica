# Postgres Search Replica: Technical Vision & Architecture

## 1. Core Concept: "SearchOps"
The `postgres-search-replica` is a **Resilient Relevance Control Plane**.

At its heart lies a battle-hardened **CDC Sync Engine** that leverages PostgreSQL's native Logical Replication protocol. Unlike fragile API-based connectors, this engine provides:
*   **Exact-Once Semantics**: Utilizing Replication Slots and LSN tracking to guarantee no data loss.
*   **Anti-Entropy**: Automatic background "Ghost Cleaners" that reconcile deleted rows.
*   **Hybrid Recovery**: A sophisticated mechanism that seamlessly switches between Snapshotting (bulk copy) and Streaming (WAL tailing), allowing it to recover from weeks of downtime in minutes.

On top of this robust foundation, it builds a **SearchOps Platform** that treats search configuration as versioned code.

---

## 2. Architecture Overview

```mermaid
graph TD
    subgraph "Layer 1: Resilient Sync Engine (The Foundation)"
        SourceDB[(Primary Postgres)]
        WAL_Stream(Native WAL Stream)
        
        subgraph "Robust Ingestion"
            SlotManager[Slot Manager]
            Watchdog[Replication Watchdog]
            HybridRecovery[Hybrid Recovery (CDC + SQL KeySet)]
            AntiEntropy[Ghost Cleaner]
        end
    end

    subgraph "Layer 2: The Search Replica (The Brain)"
        Daemon[Reconciler Daemon]
        
        subgraph "Postgres Sink DB"
            RawTable[Raw Table (Queued)]
            StateTable[_replica_state]
            
            subgraph "SearchOps Workbench"
                Shadow1[(Shadow: products_v2_nomic)]
                Shadow2[(Shadow: products_v3_hybrid)]
                MainIndex[(Main: products_v1_live)]
            end
            
            Worker1[pgai Worker: Embedder]
            Worker2[pgai Worker: Embedder]
        end
    end

    subgraph "Layer 3: Deployment Targets"
        App(Client App)
        Qdrant[(Qdrant Cloud)]
    end

    SourceDB -->|CDC Stream| Daemon
    Daemon -->|Upsert| RawTable
    
    RawTable --> Worker1 --> Shadow1
    RawTable --> Worker2 --> MainIndex
    
    MainIndex -->|Sync Connector| Qdrant
    
    App -->|Search API| MainIndex
    App -->|Preview API| Shadow1
```

---

## 3. The "SearchOps" Workflow (Workbench)
We introduce **Git-style Version Control** for Search Indices.

### Phase 1: Branching (Experimentation)
Instead of modifying the live index, the Search Engineer creates a **Feature Branch** in the config.
*   **Action**: Create `products_v2` with `embedding_model: "text-embedding-3-large"` (vs `v1`'s small model).
*   **System Response**:
    *   The `Reconciler` creates a **Shadow Table** (`products_store_v2`).
    *   It spins up a background `pgai` worker to backfill this specific index from the `RawTable`.
    *   **Crucial**: Production traffic is unaffected.

### Phase 2: Evaluation (The Workbench)
The Engineer uses the Management UI to comparing `v1` (Live) vs `v2` (Branch).
*   **Golden Set Test**: Run 500 regression queries. "Recall improved by 5%."
*   **Side-by-Side Diff**: Visually compare top-10 results for "black wireless headphones".
*   **Cost Profile**: "Warning: v2 index is 3x larger (1536d vs 384d)."

### Phase 3: "Merge to Main" (Promotion)
The Engineer clicks **"Promote to Live"**.
*   **Action**: The system performs an **Atomic View Swap**.
*   **Result**: The SQL View `products_search` now points to `products_store_v2`.
*   **Zero Downtime**: The transition is instantaneous and transactional.

---

## 4. Downstream Deployment (Qdrant Cloud)
For high-scale production, Postgres might not be the final query engine. We support **Edge Replication**.

*   **The Concept**: Postgres is the **Staging Area**. Qdrant is the **CDN**.
*   **The Mechanism**:
    *   A generic **Connector Worker** subscribes to the *Final Live View* in Postgres.
    *   It treats the `products_search` view as a queue.
    *   It batches vectors and pushes them to Qdrant/Pinecone using UUID-based Idempotency.
*   **Benefit**: You get the engineering safety of Postgres (Transactions, Rollbacks, Relational Joins) with the raw Qps/Scale of a specialized Vector DB.

---

## 5. Summary of Technical Use Case

**Scenario**: An E-commerce Team wants to switch from OpenAI embeddings to a specialized Cohere Re-ranker model.

1.  **Ingest**: Data flows naturally from their Main DB into the Replica via safe Logical Replication.
2.  **Branch**: They open the UI, click "New Branch", select "Cohere Model".
3.  **Build**: The system churns in the background for 4 hours, building the new index in a shadow table.
4.  **Verify**: They preview the search. "Result relevance looks much better for typos."
5.  **Bench**: Automated regression tests confirm no major breakage.
6.  **Swap**: They hit "Promote". The API immediately serves the new results.
7.  **Sync**: The "Qdrant Connector" automatically sees the View change, wipes the downstream Qdrant collection, and re-syncs the new high-quality vectors to the Edge for massive scale serving.
