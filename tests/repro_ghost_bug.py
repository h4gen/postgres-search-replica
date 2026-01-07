import pytest
import asyncio
import logging
from pg_replica import PGSearchReplica, settings as global_settings
from pg_replica.database import connect_db, find_and_fix_ghost_records
from tests.test_integration import get_internal_source_url, robust_slot_cleanup, robust_subscription_cleanup

@pytest.mark.asyncio
async def test_ghost_fix_uuid_repro():
    """Reproduce the NameError in find_and_fix_ghost_records with UUID IDs."""
    from unittest.mock import patch
    custom_settings = {
        "tables": {
            "uuid_test": {
                "source_table": "table_uuid",
                "id_column": "id",
                "publication_columns": ["id", "content"],
                "content_column": "content",
                "formatting_template": "$chunk $content",
            }
        }
    }
    
    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(global_settings)}):
        test_logger = logging.getLogger(__name__)

        async with await connect_db(global_settings.source_url, autocommit=True) as conn:
            await robust_slot_cleanup(conn, "sub_uuid_test", test_logger)
            await conn.execute("DROP TABLE IF EXISTS table_uuid CASCADE")
            await conn.execute("CREATE TABLE table_uuid (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), content TEXT)")
            await conn.execute("INSERT INTO table_uuid (content) VALUES ('test data')")

        async with await connect_db(global_settings.resolved_sink_url, autocommit=True) as conn:
            await robust_subscription_cleanup(conn, "sub_uuid_test", test_logger)
            await conn.execute("DROP TABLE IF EXISTS table_uuid CASCADE")
            await conn.execute("DELETE FROM _replica_state WHERE key = 'sub_uuid_test'")

        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            # Manually trigger anti-entropy to reproduce the bug
            config = replica.settings.pipelines["uuid_test"]
            # To trigger the 'ghosts' logic, we need to have a record in sink that is NOT in source.
            # But the bug is even more basic: 'ghosts' is used before definition in the 'if ghosts:' check.
            
            # Since we just want to see if it Crashes:
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
