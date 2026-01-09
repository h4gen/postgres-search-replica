
import asyncio
import pytest
from pg_replica.config import (
    Settings,
    SearchPipeline,
    IngestConfig,
    PipelineConfig,
    ChunkingConfig,
    EmbeddingConfig,
    StorageConfig,
    PostgresStoreConfig,
    BranchConfig
)
from pg_replica.reconciler import Reconciler
from pg_replica.database import init_pools, close_pools, get_sink_conn, get_source_conn

TABLE_NAME = "products"

@pytest.mark.asyncio
async def test_declarative_branching_e2e(clean_db, internal_source_url):
    """
    E2E Verification:
    1. Define Branch.
    2. Reconcile (Create Infrastructure).
    3. Insert Data (Source).
    4. Wait for Branch Sync (Sink).
    5. Search Branch View.
    """
    import os
    import logging
    os.environ["SUBSCRIPTION_SOURCE_URL"] = internal_source_url

    # 1. Define Config with a declarative Branch
    config = SearchPipeline(
        ingest=IngestConfig(table=TABLE_NAME, columns=["id", "name", "description"], p_key="id"),
        pipeline=PipelineConfig(
            template="$name $chunk",
            content_column="description",
            chunking=ChunkingConfig(),
            embedding=EmbeddingConfig(model="nomic-embed-text", provider="ollama", dimension=768)
        ),
        storage=StorageConfig(
            postgres=PostgresStoreConfig(),
            branches=[
                # The Ghost Feature: This branch should spawn "products_branch_v2"
                BranchConfig(
                    name="v2",
                    pipeline=PipelineConfig(
                        template="$name $chunk",
                        content_column="description",
                        chunking=ChunkingConfig(),
                        embedding=EmbeddingConfig(model="nomic-embed-text", provider="ollama", dimension=768)
                    )
                )
            ]
        )
    )

    settings = Settings(
        source_url=os.environ.get("SOURCE_URL", "postgresql://postgres:postgres@localhost:5433/production_db"),
        sink_url=os.environ.get("SINK_URL", "postgresql://postgres:postgres@localhost:5433/sink"),
        pipelines={"products": config}
    )

    await init_pools(settings)
    
    # Ensure global infrastructure (like _sink_outbox) exists
    from pg_replica.database import ensure_outbox_infrastructure
    await ensure_outbox_infrastructure(settings)
    
    # 0. Setup Source (Cleanup & Seed)
    async with await get_source_conn("default") as conn:
        await conn.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        await conn.execute(f"CREATE TABLE {TABLE_NAME} (id TEXT PRIMARY KEY, name TEXT, description TEXT)")
        await conn.execute(f"ALTER TABLE {TABLE_NAME} REPLICA IDENTITY FULL")
        await conn.execute(
            f"INSERT INTO {TABLE_NAME} (id, name, description) VALUES ('1', 'Phone', 'A smart mobile device'), ('2', 'Laptop', 'Portable computer'), ('3', 'Apple', 'A red fruit')"
        )

    # 2. Run Reconciliation
    reconciler = Reconciler(settings)
    await reconciler.reconcile()

    # 2.5 Start background worker to process embeddings
    # We rely on the containerized worker (dev-worker-1) to process this.
    # worker = Worker(db_url=settings.resolved_sink_url, poll_interval=timedelta(seconds=2.0))
    # worker_task = asyncio.create_task(worker.run())
    worker_task = None

    # 3. Assert Shadow Infrastructure Exists
    async with await get_sink_conn() as conn:
        async with conn.cursor() as cur:
                # Check 1: Is the Shadow View created?
                # Expected: products_branch_v2_search
                await cur.execute(
                    "SELECT 1 FROM information_schema.views WHERE table_name = 'products_branch_v2_search'"
                )
                shadow_view_exists = await cur.fetchone()
    
                # Check 2: Is the Shadow Vectorizer created?
                # Expected: products_branch_v2_store_v...
                # We just check for ANY vectorizer starting with products_branch_v2
                await cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'products_branch_v2_store_v%'"
                )
                shadow_vectorizer_exists = await cur.fetchone()
    
                if not shadow_view_exists:
                    pytest.fail("FAIL: Shadow View 'products_branch_v2_search' was NOT created.")
                
                if not shadow_vectorizer_exists:
                    pytest.fail("FAIL: Shadow vectorizer for 'v2' was NOT created.")

    # 4. Wait for Branch Sync (Manual loop because standard fixture doesn't know about branches yet)
    logger_name = "test_declarative_branching"
    logger = logging.getLogger(logger_name)
    
    shadow_target_table = "products_branch_v2_store_v71bcd09b" # Derived from known hash in previous run
    # Ideally should query it dynamically, but this hash is deterministic for this config.
    
    # Actually, let's look it up dynamicall to be robust
    async with await get_sink_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'products_branch_v2_store_v%' LIMIT 1")
            row = await cur.fetchone()
            if row:
                shadow_target_table = row[0]
    
    logger.info(f"Waiting for sync on {shadow_target_table}...")
    
    synced = False
    synced = False
    for i in range(120): # 120 seconds timeout (increased for slow Ollama)
        if worker_task and worker_task.done():
            # Worker crashed!
            exc = worker_task.exception()
            raise RuntimeError(f"Worker crashed prematurely! Error: {exc}")
            
        async with await get_sink_conn() as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(f"SELECT count(*) FROM {shadow_target_table} WHERE chunk IS NOT NULL")
                    count = (await cur.fetchone())[0]
                    
                    # Debug: Check errors
                    await cur.execute("SELECT count(*) FROM ai.vectorizer_errors")
                    error_count = (await cur.fetchone())[0]

                    if i % 5 == 0:
                        logger.info(f"Waiting... Branch Count: {count}, Errors: {error_count}")

                    if count >= 3:
                        synced = True
                        break
                except Exception as e:
                    logger.warning(f"Error checking sync: {e}")
        await asyncio.sleep(1.0)
        
    if not synced:
        pytest.fail(f"Timed out waiting for {shadow_target_table} to sync.")

    try:
        # 6. Search the Branch View
        # Note: We generate the embedding client-side to match the application's search pattern.
        from ollama import AsyncClient
        ollama_host = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
        client = AsyncClient(host=ollama_host)
        res = await client.embeddings(model="nomic-embed-text", prompt="fruit")
        embedding = res["embedding"]

        async with await get_sink_conn() as conn:
            async with conn.cursor() as cur:
                # Use explicit cast to vector for robust comparison, matching common pgvector patterns
                await cur.execute(
                    "SELECT id, chunk FROM products_branch_v2_search ORDER BY embedding <-> %s::vector LIMIT 1",
                    (embedding,)
                )          
                row = await cur.fetchone()
                
                assert row is not None, "Search returned no results"
                assert str(row[0]) == '3', f"Expected Apple (id=3) for fruit query, got {row[1]} (id={row[0]})"
    finally:
        if worker_task:
            worker_task.cancel()
            try:
                await asyncio.wait_for(worker_task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        await close_pools()
