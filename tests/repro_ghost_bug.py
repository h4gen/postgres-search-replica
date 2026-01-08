import pytest
import asyncio
import logging
from pg_replica import PGSearchReplica, settings as global_settings
from pg_replica.database import connect_db, find_and_fix_ghost_records

@pytest.mark.asyncio
async def test_ghost_fix_uuid_repro(internal_source_url, robust_slot_cleanup, robust_subscription_cleanup, source_conn, sink_conn):
    """Reproduce the NameError in find_and_fix_ghost_records with UUID IDs."""
    from unittest.mock import patch
    custom_settings = {
        "pipelines": {
            "uuid_test": {
                "ingest": {"table": "table_uuid", "columns": ["id", "content"], "p_key": "id"},
                "pipeline": {"template": "$chunk $content", "content_column": "content", "chunking": {"strategy": "recursive_character"}, "embedding": {"provider": "ollama", "model": "nomic-embed-text", "dimension": 768}},
                "storage": {"postgres": {"profile": "vector"}},
                "active": True
            }
        }
    }
    
    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": internal_source_url}):
        test_logger = logging.getLogger(__name__)

        # Setup Source
        await robust_slot_cleanup("sub_uuid_test")
        await source_conn.execute("DROP TABLE IF EXISTS table_uuid CASCADE")
        await source_conn.execute("CREATE TABLE table_uuid (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), content TEXT)")
        await source_conn.execute("INSERT INTO table_uuid (content) VALUES ('test data')")

        # Setup Sink
        await robust_subscription_cleanup("sub_uuid_test")
        await sink_conn.execute("DROP TABLE IF EXISTS table_uuid CASCADE")
        await sink_conn.execute("DELETE FROM _replica_state WHERE key = 'sub_uuid_test'")

        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            config = replica.settings.pipelines["uuid_test"]
            try:
                await find_and_fix_ghost_records(replica.settings, config, "uuid_test")
            except NameError as e:
                if "name 'ghosts' is not defined" in str(e):
                    print("\n[BUG REPRODUCED] NameError: ghosts is not defined")
                    return
                raise e
            except Exception as e:
                raise e
            
            pytest.fail("Should have raised NameError: ghosts is not defined")
