import pytest
from pg_replica.config import SearchPipeline, IngestConfig, PipelineConfig, ChunkingConfig, EmbeddingConfig, Settings
from pg_replica.reconciler import Reconciler, ActionType
from pg_replica.database import init_pools, close_pools, get_source_conn, get_sink_conn

TABLE_NAME = "test_idempotency"

@pytest.mark.asyncio
async def test_vectorizer_setup_referential_integrity(clean_db, internal_source_url):
    """
    Regression Test: Ensure Reconciler does not repeatedly plan SINK_VECTORIZER_SETUP
    when the vectorizer already exists (preventing 'Loop of Death').
    """
    import os
    os.environ["SUBSCRIPTION_SOURCE_URL"] = internal_source_url
    
    # 1. Config
    config = SearchPipeline(
        ingest=IngestConfig(table=TABLE_NAME, columns=["id", "text"], p_key="id"),
        pipeline=PipelineConfig(
            template="$chunk",
            content_column="text",
            embedding=EmbeddingConfig(provider="ollama", model="nomic-embed-text", dimension=768)
        )
    )
    settings = Settings(
        source_url=os.environ.get("SOURCE_URL", "postgresql://postgres:postgres@localhost:5433/postgres"),
        sink_url=os.environ.get("SINK_URL", "postgresql://postgres:postgres@localhost:5434/postgres"),
        pipelines={TABLE_NAME: config}
    )
    
    await init_pools(settings)
    
    try:
        # 2. Setup Source Table
        async with await get_source_conn() as conn:
            await conn.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
            await conn.execute(f"CREATE TABLE {TABLE_NAME} (id TEXT PRIMARY KEY, text TEXT)")
            await conn.execute(f"ALTER TABLE {TABLE_NAME} REPLICA IDENTITY FULL")
            
        reconciler = Reconciler(settings)
        
        # 3. Round 1: Initial Reconciliation (Should Create)
        sink_state = await reconciler.inspector.get_sink_state()
        source_state = await reconciler.inspector.get_source_state()
        actions = reconciler.planner.plan(source_state, sink_state)
        
        setup_actions = [a for a in actions if a.type == ActionType.SINK_VECTORIZER_SETUP]
        assert len(setup_actions) == 1, "First run should plan vectorizer setup"
        
        # Apply actions (realize state)
        import logging
        logging.basicConfig(level=logging.INFO)
        from pg_replica.reconciler import Applier
        applier = Applier(settings)
        failed = await applier.apply(actions)
        assert not failed, f"Applier failed for targets: {failed}"
        
        # DEBUG: Direct DB Inspection
        async with await get_sink_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT extname FROM pg_extension")
                exts = await cur.fetchall()
                print(f"\nDEBUG: Extensions: {exts}")
                
                await cur.execute("SELECT name, source_table, config FROM ai.vectorizer")
                rows = await cur.fetchall()
                print(f"\nDEBUG: Full ai.vectorizer table content: {rows}")

        # 4. Round 2: Idempotency Check (Should NOT Create)
        # Refresh state
        sink_state_2 = await reconciler.inspector.get_sink_state()
        source_state_2 = await reconciler.inspector.get_source_state()
        actions_2 = reconciler.planner.plan(source_state_2, sink_state_2)
        
        setup_actions_2 = [a for a in actions_2 if a.type == ActionType.SINK_VECTORIZER_SETUP]
        
        # DEBUG: Inspect what the inspector found
        print(f"\nDEBUG: Sink Vectorizers Found for {TABLE_NAME}: {sink_state_2.get('vectorizers', {}).get(TABLE_NAME)}")
        
        assert len(setup_actions_2) == 0, f"Reconciler is not idempotent! It planned duplicate setups: {setup_actions_2}"
        
    finally:
        await close_pools()
