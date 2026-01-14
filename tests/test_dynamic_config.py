import pytest
import asyncio
import logging
from pg_replica import PGSearchReplica, settings as global_settings
from pg_replica.database import (
    connect_db, 
    save_table_config, 
    get_latest_table_config, 
    update_config_status,
    reconciliation_lock
)

from pg_replica.reconciler import ActionType

logger = logging.getLogger(__name__)

# Removed local robust_cleanup in favor of conftest.py's clean_db

def get_internal_source_url(settings):
    return settings.source_url.replace("localhost:5433", "source:5432").replace("127.0.0.1:5433", "source:5432")

@pytest.mark.asyncio
async def test_dynamic_config_override(clean_db):
    """Verify that the Reconciler picks up config changes from the DB."""
    from unittest.mock import patch
    from pg_replica.database import init_pools, close_pools
    from pg_replica.reconciler import Reconciler
    from pg_replica.config import SearchPipeline, IngestConfig, PipelineConfig, EmbeddingConfig
    
    target_name = "dynamic_test_v3"
    # Initial config (v1)
    base_config = SearchPipeline(
        ingest=IngestConfig(table="products", columns=["name", "description"]),
        pipeline=PipelineConfig(
            template="$chunk", 
            content_column="description",
            embedding=EmbeddingConfig(provider="ollama", model="nomic-embed-text", dimension=768)
        )
    )
    
    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(global_settings)}):
        # Pass Pipelines via constructor, using global settings to ensure correct ports
        settings = global_settings.model_copy()
        settings.pipelines = {target_name: base_config}
        replica = PGSearchReplica(verbose=True, **settings.model_dump())
        # clean_db fixture already ran, but we need to init pools for the test
        await init_pools(replica.settings)
        reconciler = Reconciler(replica.settings)
        
        try:
            # 1. Start by getting current max gen (if any) and save next
            db_state = await get_latest_table_config(replica.settings, target_name)
            initial_gen = db_state["generation"] if db_state else 0
            
            gen1 = await save_table_config(replica.settings, target_name, base_config)
            assert gen1 == initial_gen + 1
            
            await reconciler.reconcile()
            print(f"Tables after reconcile: {list(replica.settings.pipelines.keys())}")
            assert target_name in replica.settings.pipelines
            assert getattr(replica.settings.pipelines[target_name], "_generation") == gen1
            
            # 2. Update config in DB (gen + 1)
            # Create new config object with modified filter
            new_config = base_config.model_copy(deep=True)
            new_config.ingest.filter = "id < '500'"
            
            gen2 = await save_table_config(replica.settings, target_name, new_config)
            assert gen2 == gen1 + 1
            
            # 3. Reconcile again and verify override
            await reconciler.reconcile()
            
            curr_config = replica.settings.pipelines[target_name]
            assert curr_config.ingest.filter == "id < '500'"
            assert getattr(curr_config, "_generation") == gen2
            
            # 4. Verify Status in DB is updated to Ready
            db_state_after = await get_latest_table_config(replica.settings, target_name)
            assert db_state_after["status"] == "Ready"
            assert db_state_after["observed_generation"] == gen2
        finally:
            await asyncio.sleep(0.1)
            await close_pools()

@pytest.mark.asyncio
async def test_reconciliation_locking(clean_db):
    """Verify that two Reconcilers cannot run simultaneously (Advisory Lock)."""
    from pg_replica.database import init_pools, close_pools
    replica = PGSearchReplica()
    await init_pools(replica.settings)
    
    try:
        # 1. Acquire lock in one "process"
        async with reconciliation_lock():
            # 2. Try to run reconcile in another "process" (it should fail/skip)
            with pytest.raises(RuntimeError) as excinfo:
                async with reconciliation_lock():
                    pass
            assert "Could not acquire reconciliation lock" in str(excinfo.value)
    finally:
        await asyncio.sleep(0.1)
        await close_pools()

@pytest.mark.asyncio
async def test_failed_config_status(clean_db):
    """Verify that if reconciliation fails, the status is updated to 'Failed'."""
    from unittest.mock import patch
    from pg_replica.database import init_pools, close_pools
    from pg_replica.reconciler import Reconciler, Action, ActionType
    from pg_replica.config import SearchPipeline, IngestConfig, PipelineConfig, EmbeddingConfig
    
    target_name = "fail_test_v3"
    base_config = SearchPipeline(
        ingest=IngestConfig(table="products", columns=["name"]),
        pipeline=PipelineConfig(
            template="$chunk", 
            content_column="name",
            embedding=EmbeddingConfig(provider="ollama", model="nomic-embed-text", dimension=768)
        )
    )
    
    settings = global_settings.model_copy()
    settings.pipelines = {target_name: base_config}
    replica = PGSearchReplica(verbose=True, **settings.model_dump())
    # clean_db fixture handles cleanup
    await init_pools(replica.settings)
    
    reconciler = Reconciler(replica.settings)
    
    try:
        # Patch the lock ID to avoid conflicts with previous tests
        with patch("pg_replica.database.RECONCILER_ADVISORY_LOCK_ID", 999999):
            # Insert a config that we'll try to apply
            gen = await save_table_config(replica.settings, target_name, base_config)
        
        # Mock planning to ALWAYS return one action for our target
        mock_action = Action(
            type=ActionType.SOURCE_SETUP,
            description="Force Failure Action",
            params={},
            target_name=target_name
        )
        
        with patch.object(reconciler.planner, "plan", return_value=[mock_action]):
            # Mock the applier to fail
            with patch.object(reconciler.applier, "apply", side_effect=Exception("Simulated Failure")):
                try:
                    await reconciler.reconcile()
                except Exception as e:
                    print(f"Caught expected exception: {e}")
                    assert "Simulated Failure" in str(e)
                else:
                    pytest.fail("Reconcile should have raised Simulated Failure")
                
        # Verify status in DB
        db_state = await get_latest_table_config(replica.settings, target_name)
        assert db_state["status"] == "Failed"
        assert "Simulated Failure" in db_state["error_message"]
    finally:
        await asyncio.sleep(0.1)
        await close_pools()
