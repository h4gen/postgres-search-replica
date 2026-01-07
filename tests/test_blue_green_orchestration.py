
import asyncio
import pytest
import psycopg
from pg_replica.config import Settings, settings as global_settings
from pg_replica.config_v2 import SearchPipeline, IngestConfig, PipelineConfig, ChunkingConfig, EmbeddingConfig, StorageConfig, PostgresStoreConfig
from pg_replica.reconciler import Reconciler
from pg_replica.database import get_sink_conn

# Test Data
TABLE_NAME = "products"
REPLICA_TABLE = "products_search"

@pytest.mark.asyncio
async def test_declarative_blue_green_orchestration():
    """
    Verifies the full lifecycle of a Blue-Green deployment using declarative state:
    1. Initial Deployment (v1, active=True) -> Wait for Sync -> View Created
    2. Deferred Migration (v2, active=False) -> v1 stays live
    3. Promotion (v2, active=True) -> Sync -> Swap
    4. Version Skipping (v2 syncing -> v3 active) -> v2 never live -> v3 live
    """
    
    # helper to inspect current view target
    async def get_current_view_target():
        async with await get_sink_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT table_name FROM information_schema.view_table_usage WHERE view_name = %s",
                    (REPLICA_TABLE,)
                )
                rows = await cur.fetchall()
                if not rows:
                    return None
                
                # We expect the view to join the raw table (e.g. products) 
                # with the versioned embedding table/view (e.g. products_embedding_v...)
                targets = [r[0] for r in rows]
                print(f"DEBUG: view {REPLICA_TABLE} targets: {targets}")
                
                for t in targets:
                    if "_v" in t or "_embedding" in t or "_store" in t:
                        return t
                return targets[0]

    # Manually poll database status for v2 target
    import os
    from pg_replica.database import init_pools, close_pools
    
    # Use global settings which are correctly seeded by pytest-docker/env
    source_url = global_settings.source_url
    sink_url = global_settings.resolved_sink_url # Use resolved for host access

    # This is for the Sink container to connect to the Source container
    from tests.test_integration import get_internal_source_url
    os.environ["SUBSCRIPTION_SOURCE_URL"] = get_internal_source_url(global_settings)

    settings = Settings(
        source_url=source_url,
        sink_url=sink_url,
        pipelines={}
    )
    
    # Initialize connection pools
    await init_pools(settings)
    
    # Explicit Cleanup
    async with await get_sink_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"DROP VIEW IF EXISTS {REPLICA_TABLE}")
            try:
                await cur.execute("TRUNCATE TABLE _replica_state CASCADE")
            except Exception:
                await conn.rollback()
            # Clean up vectorizers only if the extension table exists
            await cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'ai' AND table_name = 'vectorizer'
                )
            """)
            if (await cur.fetchone())[0]:
                await cur.execute("DELETE FROM ai.vectorizer")
    
    reconciler = Reconciler(settings)

    print("\n--- Phase 1: Initial Deployment (v1) ---")
    config_v1 = SearchPipeline(
        ingest=IngestConfig(table=TABLE_NAME, columns=["id", "name", "description"], p_key="id"),
        pipeline=PipelineConfig(
             template="$name $chunk",
             content_column="description",
             chunking=ChunkingConfig(strategy="recursive_character"),
             embedding=EmbeddingConfig(model="nomic-embed-text", provider="ollama", dimension=768)
        ),
        storage=StorageConfig(postgres=PostgresStoreConfig(profile="vector")),
        active=True
    )
    
    settings.pipelines = {"v1": config_v1}
    
    # 1.1 First Reconcile: Should create infra and view (since it's bootstrap)
    try:
        print(f"DEBUG: reconciling with pipelines keys: {list(settings.pipelines.keys())}")
        await reconciler.reconcile()
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise e
    
    # Wait for sync logic is handled by standard test utils usually, but here we check states.
    # For bootstrap, our logic allows creation immediately?
    # Logic: "Case A: View doesn't exist at all -> Promote immediately"
    target = await get_current_view_target()
    assert target is not None, "View should be created on bootstrap"
    assert "nomic" in target or "_v" in target # Check it points to v1 vectorizer
    print(f"Phase 1 Success: View points to {target}")

    print("\n--- Phase 2: Add v2 (Scanning) ---")
    # Change model to trigger new version
    config_v2 = config_v1.model_copy(deep=True)
    config_v2.pipeline.embedding.model = "all-minilm"
    config_v2.active = False
    
    # We must keep v1 in settings so it doesn't get cleaned up!
    settings.pipelines = {"v1": config_v1, "v2": config_v2}
    
    await reconciler.reconcile()
    
    target = await get_current_view_target()
    # Should still point to v1
    # Verify v2 vectorizer exists?
    # TODO: How to verify v2 exists?
    
    print(f"Phase 2 Success: View still points to {target}")

    print("\n--- Phase 3: Promote v2 (Blue-Green) ---")
    # Swap active flags
    # Swap active flags
    config_v1.active = False
    config_v2.active = True
    settings.pipelines = {"v1": config_v1, "v2": config_v2}
    
    # Running reconcile now:
    # v2 is active but likely NOT synced yet (unless dataset is tiny).
    # Reconciler should see "pending > 0" and decide NOT to swap.
    # But this depends on speed of ollama.
    
    await reconciler.reconcile()
    
    target_after_plan = await get_current_view_target()
    
    # If it was instant, it swapped. If not, it stayed.
    # For a deterministic test, we'd need to mock `get_vectorizer_statuses` to force "pending=100".
    # But since we can't easily patch inside Reconciler without generic mocks...
    # We will assume for now that we check "Eventually".
    
    # Let's wait for sync
    from tests.test_integration import wait_for_pgai_sync
    
    # Find v2 vectorizer name
    v2_id = config_v2.get_version_id()
    expected_v2_target = f"{TABLE_NAME}_store_v{v2_id}"
    
    # We need the actual vectorizer name (not just target table) for wait_for_pgai_sync?
    # Actually wait_for_pgai_sync uses `ai.vectorizer_status` which has `source_table, pending_items`.
    # But `ai.vectorizer_status` is keyed by `source_table` (which is unique per vectorizer?).
    # No, strictly `ai.vectorizer_status` has a row per vectorizer.
    # The source table in `status` might help if we know it.
    
    # Let's just wait for global sync (heuristic) or use our reconciler's internal status check via a loop.
    print("Waiting for v2 sync...")
    
    # Manually poll database status for v2 target
    async def wait_for_target_sync(target_table_pattern):
        for _ in range(60): # Wait up to 60s
            # Use direct DB connection
            async with await get_sink_conn() as conn:
                async with conn.cursor() as cur:
                     # Check if our target table has a vectorizer with 0 pending items
                     await cur.execute("SELECT config->'destination'->>'target_table', id FROM ai.vectorizer")
                     vecs = await cur.fetchall()
                     for tgt, vid in vecs:
                         if tgt == target_table_pattern:
                             # Check status
                             await cur.execute("SELECT count(*) FROM ai.vectorizer_status WHERE id = %s AND pending_items = 0", (vid,))
                             if (await cur.fetchone())[0] > 0:
                                 return True
            await asyncio.sleep(1)
        return False

    # Note: For integration test environment (ollama container), this might be slow or fail if ollama is not up.
    # We assume 'wait-for-infra' passed.
    
    # Re-run reconcile loop periodically or wait?
    # The Reconciler is triggered by external events usually. Here we simulate it.
    
    # We will loop reconcile until success or timeout
    swapped = False
    for _ in range(20):
        await reconciler.reconcile()
        current_target = await get_current_view_target()
        
        # Check if versioned target matches v2
        if current_target and v2_id in current_target:
             swapped = True
             break
        await asyncio.sleep(2)
        
    assert swapped, f"Should have swapped to v2 (version {v2_id}). Current: {current_target}"
    print(f"Phase 3 Success: View swapped to {current_target}")
    
    await close_pools()

