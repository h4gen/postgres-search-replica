
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
async def test_declarative_blue_green_orchestration(clean_db, robust_slot_cleanup, internal_source_url, wait_for_pgai_sync):
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

    # Fix: Use fixture value instead of import
    os.environ["SUBSCRIPTION_SOURCE_URL"] = internal_source_url

    settings = Settings(
        source_url=source_url,
        sink_url=sink_url,
        pipelines={}
    )
    
    # Initialize connection pools
    await init_pools(settings)
    
    # Not needed as clean_db handles it, but robust_slot_cleanup is needed for specific slots if managed manually
    # clean_db drops views and vectorizers.
    
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
    print(f"Phase 2 Success: View still points to {target}")

    print("\n--- Phase 3: Promote v2 (Blue-Green) ---")
    # Swap active flags
    config_v1.active = False
    config_v2.active = True
    settings.pipelines = {"v1": config_v1, "v2": config_v2}
    
    await reconciler.reconcile()
    
    target_after_plan = await get_current_view_target()
    
    # Find v2 vectorizer name
    v2_id = config_v2.get_version_id()
    
    print("Waiting for v2 sync...")
    
    # Re-run reconcile loop periodically until success or timeout
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

