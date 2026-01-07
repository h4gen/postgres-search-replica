import pytest
import asyncio
import logging
from pg_replica import PGSearchReplica, settings as global_settings
from pg_replica.database import connect_db, find_and_fix_ghost_records
from tests.test_integration import get_internal_source_url, robust_slot_cleanup, robust_subscription_cleanup

@pytest.mark.asyncio
async def test_ghost_fix_uuid_regression():
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
    
    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(global_settings)}):
        test_logger = logging.getLogger(__name__)

        async with await connect_db(global_settings.source_url, autocommit=True) as conn:
            await robust_slot_cleanup(conn, "sub_uuid_ghost", test_logger)
            await conn.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
            await conn.execute("DROP TABLE IF EXISTS table_uuid_ghost CASCADE")
            await conn.execute("CREATE TABLE table_uuid_ghost (id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), content TEXT)")
            await conn.execute("INSERT INTO table_uuid_ghost (content) VALUES ('test data')")

        async with await connect_db(global_settings.resolved_sink_url, autocommit=True) as conn:
            await robust_subscription_cleanup(conn, "sub_uuid_ghost", test_logger)
            await conn.execute("DROP TABLE IF EXISTS table_uuid_ghost CASCADE")
            await conn.execute("DELETE FROM _replica_state WHERE key = 'sub_uuid_ghost'")

        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            # Clean Break: Use the pipelines shim to get a SearchPipeline object
            config = replica.settings.pipelines["uuid_ghost"]
            
            # This SHOULD trigger Strategy 2 in find_and_fix_ghost_records
            # because ID type is UUID (non-numeric).
            # We don't even need actual ghosts to trigger the NameError, 
            
            logger = logging.getLogger("pg_replica.database")
            logger.info("Manually triggering anti-entropy sweep to check for NameError...")
            
            await find_and_fix_ghost_records(replica.settings, config, "uuid_ghost")
            
            # If it reaches here without raising NameError, the test fails (or bug is fixed)
            logger.info("Anti-entropy sweep completed successfully.")
