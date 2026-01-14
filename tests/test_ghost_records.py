import pytest
import asyncio
import logging
from pg_replica import PGSearchReplica, settings as global_settings
from pg_replica.database import connect_db, find_and_fix_ghost_records, init_pools, close_pools

logger = logging.getLogger(__name__)

@pytest.mark.asyncio
async def test_ghost_fix_uuid_regression(internal_source_url, robust_slot_cleanup, robust_subscription_cleanup, source_conn, sink_conn, wait_for_pgai_sync):
    """Reproduce the NameError in find_and_fix_ghost_records with UUID IDs."""
    from unittest.mock import patch
    custom_settings = {
        "pipelines": {
            "uuid_ghost": {
                "ingest": {"table": "table_uuid_ghost", "columns": ["id", "content"], "p_key": "id"},
                "pipeline": {"template": "$chunk $content", "content_column": "content", "chunking": {"strategy": "recursive_character"}, "embedding": {"provider": "ollama", "model": "nomic-embed-text", "dimension": 768}},
                "storage": {"postgres": {"profile": "vector"}},
                "active": True
            }
        }
    }
    
    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": internal_source_url}):
        # Setup Source
        await robust_slot_cleanup("sub_uuid_ghost")
        await source_conn.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
        await source_conn.execute("DROP TABLE IF EXISTS table_uuid_ghost CASCADE")
        await source_conn.execute("CREATE TABLE table_uuid_ghost (id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), content TEXT)")
        await source_conn.execute("INSERT INTO table_uuid_ghost (content) VALUES ('test data')")

        # Cleanup Sink
        await robust_subscription_cleanup("sub_uuid_ghost")
        await sink_conn.execute("DROP TABLE IF EXISTS table_uuid_ghost CASCADE")
        await sink_conn.execute("DELETE FROM _replica_state WHERE key = 'sub_uuid_ghost'")

        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            # Clean Break: Use the pipelines shim to get a SearchPipeline object
            config = replica.settings.pipelines["uuid_ghost"]
            
            # This SHOULD trigger Strategy 2 in find_and_fix_ghost_records
            # because ID type is UUID (non-numeric).
            
            # Initialize pools for backend function usage in test process
            await init_pools(replica.settings)
            try:
                logger = logging.getLogger("pg_replica.database")
                logger.info("Manually triggering anti-entropy sweep to check for NameError...")
                
                # Wait for sync to ensure sink table exists
                await wait_for_pgai_sync(replica.settings, "uuid_ghost", expected_count=1)
                
                await find_and_fix_ghost_records(replica.settings, config, "uuid_ghost")
                
                logger.info("Anti-entropy sweep completed successfully.")
            finally:
                await close_pools()
