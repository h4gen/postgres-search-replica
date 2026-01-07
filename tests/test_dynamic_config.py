import pytest
import asyncio
import logging
from pg_replica import PGSearchReplica, settings as global_settings
from pg_replica.database import (
    connect_db, 
    save_replica_config, 
    get_latest_replica_config, 
    update_config_status,
    reconciliation_lock
)
from pg_replica.config import (
    ReplicaConfig, SourceConfig, VectorizerConfig,
    FormattingConfig, SearchConfig, MirrorsConfig
)
from pg_replica.reconciler import ActionType

logger = logging.getLogger(__name__)

async def robust_cleanup(settings):
    """Aggressively clear all slots and subscriptions for clean test state."""
    from pg_replica.database import connect_db, get_source_conn
    import asyncio
    
    # 1. Clear Sink subscriptions
    try:
        async with await connect_db(settings.resolved_sink_url) as conn:
            await conn.set_autocommit(True)
            async with conn.cursor() as cur:
                await cur.execute("SELECT subname FROM pg_subscription")
                subs = [r[0] for r in await cur.fetchall()]
                for sub in subs:
                    logger.info(f"Cleaning up subscription {sub}...")
                    try:
                        await cur.execute(f"ALTER SUBSCRIPTION {sub} DISABLE")
                        await cur.execute(f"ALTER SUBSCRIPTION {sub} SET (slot_name = NONE)")
                        await cur.execute(f"DROP SUBSCRIPTION IF EXISTS {sub}")
                    except Exception: pass
    except Exception: pass
    
    # 3. Clear Control Plane History (Prevent Zombies)
    try:
        async with await connect_db(settings.resolved_sink_url) as conn:
            await conn.set_autocommit(True)
            await conn.execute("TRUNCATE _replica_config_history CASCADE")
    except Exception: pass

    # 2. Clear Source slots
    try:
        async with await connect_db(settings.source_url) as conn:
            await conn.set_autocommit(True)
            async with conn.cursor() as cur:
                await cur.execute("SELECT slot_name, active, active_pid FROM pg_replication_slots")
                slots = await cur.fetchall()
                for slot, active, pid in slots:
                    logger.info(f"Cleaning up slot {slot}...")
                    if active and pid:
                        try: await cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
                        except Exception: pass
                    try: await cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
                    except Exception: pass
    except Exception: pass
    await asyncio.sleep(1)

def get_internal_source_url(settings):
    return settings.source_url.replace("localhost:5433", "source:5432").replace("127.0.0.1:5433", "source:5432")

@pytest.mark.asyncio
async def test_dynamic_config_override():
    """Verify that the Reconciler picks up config changes from the DB."""
    from unittest.mock import patch
    from pg_replica.database import init_pools, close_pools
    from pg_replica.reconciler import Reconciler
    
    target_name = "dynamic_test_v3"
    # Initial config (v1)
    base_config = ReplicaConfig(
        source=SourceConfig(table="products", columns=["name", "description"]),
        vectorizer=VectorizerConfig(),
        formatting=FormattingConfig(template="$chunk"),
        search=SearchConfig(),
        mirrors=MirrorsConfig()
    )
    
    # Pass 'replicas' directly as a keyword argument to override Settings
    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(global_settings)}):
        replica = PGSearchReplica(replicas={target_name: base_config})
        await robust_cleanup(replica.settings) # CLEANUP FIRST
        await init_pools(replica.settings)
        reconciler = Reconciler(replica.settings)
        
        try:
            # 1. Start by getting current max gen (if any) and save next
            db_state = await get_latest_replica_config(replica.settings, target_name)
            initial_gen = db_state["generation"] if db_state else 0
            
            gen1 = await save_replica_config(replica.settings, target_name, base_config)
            assert gen1 == initial_gen + 1
            
            await reconciler.reconcile()
            print(f"Replicas after reconcile: {list(replica.settings.replicas.keys())}")
            assert target_name in replica.settings.replicas
            assert getattr(replica.settings.replicas[target_name], "_generation") == gen1
            
            # 2. Update config in DB (gen + 1)
            new_config = base_config.model_copy(deep=True)
            new_config.source.filter = "id < 500"
            
            gen2 = await save_replica_config(replica.settings, target_name, new_config)
            assert gen2 == gen1 + 1
            
            # 3. Reconcile again and verify override
            await reconciler.reconcile()
            
            curr_config = replica.settings.replicas[target_name]
            assert curr_config.source.filter == "id < 500"
            assert getattr(curr_config, "_generation") == gen2
            
            # 4. Verify Status in DB is updated to Ready
            db_state_after = await get_latest_replica_config(replica.settings, target_name)
            assert db_state_after["status"] == "Ready"
            assert db_state_after["observed_generation"] == gen2
        finally:
            await close_pools()

@pytest.mark.asyncio
async def test_reconciliation_locking():
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
        await close_pools()

@pytest.mark.asyncio
async def test_failed_config_status():
    """Verify that if reconciliation fails, the status is updated to 'Failed'."""
    from unittest.mock import patch
    from pg_replica.database import init_pools, close_pools
    from pg_replica.reconciler import Reconciler, Action, ActionType
    
    target_name = "fail_test_v3"
    base_config = ReplicaConfig(
        source=SourceConfig(table="products", columns=["name"]),
        vectorizer=VectorizerConfig(),
        formatting=FormattingConfig(template="$chunk"),
        search=SearchConfig(),
        mirrors=MirrorsConfig()
    )
    
    replica = PGSearchReplica(replicas={target_name: base_config})
    await robust_cleanup(replica.settings) # CLEANUP FIRST
    await init_pools(replica.settings)
    reconciler = Reconciler(replica.settings)
    
    try:
        # Insert a config that we'll try to apply
        gen = await save_replica_config(replica.settings, target_name, base_config)
        
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
        db_state = await get_latest_replica_config(replica.settings, target_name)
        assert db_state["status"] == "Failed"
        assert "Simulated Failure" in db_state["error_message"]
    finally:
        await close_pools()
