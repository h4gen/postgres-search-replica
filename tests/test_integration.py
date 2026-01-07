import pytest
import asyncio
import logging
from pgvector.psycopg import register_vector_async as register_vector  # type: ignore
from pg_replica import PGSearchReplica, settings as global_settings
from pg_replica.database import connect_db, check_and_protect_source, dict_row


async def wait_for_pgai_sync(settings, target_name, expected_count=1, timeout=120):
    """Wait for pgai vectorizer to finish processing all rows for a target."""
    import time
    import logging
    logger = logging.getLogger(__name__)
    start_time = time.time()
    config = settings.replicas[target_name]
    embedding_table = None

    logger.info(f"Waiting for {expected_count} embeddings for target '{target_name}'...")
    while time.time() - start_time < timeout:
        async with await connect_db(settings.resolved_sink_url) as conn:
            async with conn.cursor() as cur:
                try:
                    search_view = f"{target_name}_search"
                    await cur.execute(
                        "SELECT table_name FROM information_schema.view_table_usage WHERE view_name = %s AND (table_name LIKE '%%_store_v%%' OR table_name LIKE '%%_embedding%%') LIMIT 1",
                        (search_view,),
                    )
                    row = await cur.fetchone()
                    if row: embedding_table = row[0]
                except Exception: pass

                current_table = embedding_table or f"{config.source.table}_store_v{config.get_version_id()}"

                try:
                    await cur.execute("SELECT source_table, pending_items FROM ai.vectorizer_status")
                    for table, pending in await cur.fetchall():
                        logger.info(f"Vectorizer {table} status: {pending} items pending")
                except Exception: pass

                try:
                    embedding_col = "embedding"
                    await cur.execute(f"SELECT count(*) FROM {current_table} WHERE {embedding_col} IS NOT NULL")
                    count = (await cur.fetchone())[0]
                    logger.info(f"Current embedding count for {target_name}: {count}/{expected_count}")
                    if count >= expected_count: return True
                except Exception as e:
                    logger.info(f"Waiting for {current_table}... ({e})")
        await asyncio.sleep(2)
    return False


async def robust_slot_cleanup(conn, subscription_name, logger):
    """Safely drops a replication slot, terminating any backend holding it."""
    import asyncio
    max_retries = 5
    for attempt in range(max_retries):
        async with conn.cursor() as cur:
            await cur.execute("SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = %s", (subscription_name,))
            slot_info = await cur.fetchone()
            if not slot_info: return
            active, pid = slot_info
            if active and pid:
                try: await cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
                except Exception: pass
                await asyncio.sleep(0.5)
            try:
                await cur.execute("SELECT pg_drop_replication_slot(%s)", (subscription_name,))
                return
            except Exception: await asyncio.sleep(1)
    logger.error(f"Failed to drop slot {subscription_name}")


async def robust_subscription_cleanup(conn, sub_name, logger):
    """Aggressively drop a subscription and its mapping."""
    try:
        await conn.execute(f"ALTER SUBSCRIPTION {sub_name} DISABLE")
        await conn.execute(f"ALTER SUBSCRIPTION {sub_name} SET (slot_name = NONE)")
        await conn.execute(f"DROP SUBSCRIPTION IF EXISTS {sub_name} CASCADE")
    except Exception as e:
        logger.debug(f"Subscription {sub_name} already gone or error: {e}")


def get_internal_source_url(settings):
    """Helper to translate localhost URL to internal Docker URL."""
    return settings.source_url.replace("localhost:5433", "source:5432").replace("127.0.0.1:5433", "source:5432")


@pytest.mark.asyncio
async def test_full_replication_flow():
    """Integration test for basic multi-table logic (one table)."""
    from unittest.mock import patch
    custom_settings = {
        "replicas": {
            "full_products": {
                "source": {"table": "full_products", "columns": ["name", "description"]},
                "formatting": {"template": "$chunk $name"},
            }
        }
    }
    
    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(global_settings)}):
        import logging
        test_logger = logging.getLogger(__name__)

        async with await connect_db(global_settings.source_url, autocommit=True) as conn:
            await robust_slot_cleanup(conn, "sub_full_products", test_logger)
            await conn.execute("DROP TABLE IF EXISTS full_products CASCADE")
            await conn.execute("CREATE TABLE full_products (id SERIAL PRIMARY KEY, name TEXT, description TEXT)")
            await conn.execute("INSERT INTO full_products (name, description) VALUES ('SuperGadget', 'A really useful tool for testing')")

        async with await connect_db(global_settings.resolved_sink_url, autocommit=True) as conn:
            await robust_subscription_cleanup(conn, "sub_full_products", test_logger)
            await conn.execute("DROP TABLE IF EXISTS full_products CASCADE")
            await conn.execute("DELETE FROM _replica_state WHERE key = 'sub_full_products'")
            try: await conn.execute("DELETE FROM ai.vectorizer")
            except: pass

        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            settings = replica.settings
            config = settings.replicas["full_products"]

            found = False
            for _ in range(10):
                async with await connect_db(settings.resolved_sink_url) as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(f"SELECT count(*) FROM {config.source.table} WHERE name = 'SuperGadget'")
                        if (await cur.fetchone())[0] > 0:
                            found = True
                            break
                await asyncio.sleep(1)
            assert found, "Native replication failed"

            assert await wait_for_pgai_sync(settings, "full_products")
            results = await replica.search("SuperGadget", table="full_products")
            assert len(results) > 0
            assert "SuperGadget" in results[0]["content"]




@pytest.mark.asyncio
async def test_multi_table_search():
    """Verify that multiple tables can be searched independently."""
    from unittest.mock import patch
    custom_settings = {
        "replicas": {
            "t1": {
                "source": {"table": "table1", "columns": ["content"]},
                "formatting": {"template": "$chunk $content"}
            },
            "t2": {
                "source": {"table": "table2", "columns": ["content"]},
                "formatting": {"template": "$chunk $content"}
            },
        }
    }
    
    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(global_settings)}):
        test_logger = logging.getLogger(__name__)

        async with await connect_db(global_settings.source_url, autocommit=True) as conn:
            for t in ["sub_t1", "sub_t2"]: await robust_slot_cleanup(conn, t, test_logger)
            for t in ["table1", "table2"]:
                await conn.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
                await conn.execute(f"CREATE TABLE {t} (id SERIAL PRIMARY KEY, name TEXT, description TEXT, content TEXT)")
            await conn.execute("INSERT INTO table1 (name, description, content) VALUES ('N1', 'D1', 'alpha')")
            await conn.execute("INSERT INTO table2 (name, description, content) VALUES ('N2', 'D2', 'beta')")

        async with await connect_db(global_settings.resolved_sink_url, autocommit=True) as conn:
            for s in ["sub_t1", "sub_t2"]: await robust_subscription_cleanup(conn, s, test_logger)
            await conn.execute("DROP TABLE IF EXISTS table1 CASCADE")
            await conn.execute("DROP TABLE IF EXISTS table2 CASCADE")
            await conn.execute("DELETE FROM _replica_state WHERE key IN ('sub_t1', 'sub_t2')")
            try: await conn.execute("DELETE FROM ai.vectorizer")
            except: pass

        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            assert await wait_for_pgai_sync(replica.settings, "t1")
            assert await wait_for_pgai_sync(replica.settings, "t2")

            r1 = await replica.search("alpha", table="t1")
            assert len(r1) >= 1
            assert "alpha" in r1[0]["content"]

            r2 = await replica.search("beta", table="t2")
            assert len(r2) >= 1
            assert "beta" in r2[0]["content"]


@pytest.mark.asyncio
async def test_hybrid_search_rrf():
    """Verify that hybrid search (RRF) view is created and contains ts_col."""
    from unittest.mock import patch
    custom_settings = {
        "replicas": {
            "hybrid_test": {
                "source": {"table": "hybrid_products", "content_column": "content"},
                "search": {"profile": "hybrid"},
                "formatting": {"template": "$chunk $content"},
            }
        }
    }
    
    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(global_settings)}):
        test_logger = logging.getLogger(__name__)
        async with await connect_db(global_settings.source_url, autocommit=True) as conn:
            await robust_slot_cleanup(conn, "sub_hybrid_test", test_logger)
            await conn.execute("DROP TABLE IF EXISTS hybrid_products CASCADE")
            await conn.execute("CREATE TABLE hybrid_products (id SERIAL PRIMARY KEY, name TEXT, description TEXT, content TEXT)")
            await conn.execute("INSERT INTO hybrid_products (name, description, content) VALUES ('H1', 'D1', 'hybrid search test')")

        async with await connect_db(global_settings.resolved_sink_url, autocommit=True) as conn:
            await robust_subscription_cleanup(conn, "sub_hybrid_test", test_logger)
            await conn.execute("DROP TABLE IF EXISTS hybrid_products CASCADE")
            await conn.execute("DELETE FROM _replica_state WHERE key = 'sub_hybrid_test'")
            try: await conn.execute("DELETE FROM ai.vectorizer")
            except: pass

        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            assert await wait_for_pgai_sync(replica.settings, "hybrid_test")
            
            # Verify view structure
            async with await connect_db(replica.settings.resolved_sink_url) as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute("SELECT * FROM hybrid_test_search LIMIT 1")
                    row = await cur.fetchone()
                    assert "ts_col" in row, "ts_col missing from hybrid view"
                    assert row["ts_col"] is not None


@pytest.mark.asyncio
async def test_blue_green_swap():
    """Verify atomic swap with multi-table config."""
    from unittest.mock import patch
    base_config = {
        "replicas": {
            "swap": {
                "source": {"table": "swap_products", "content_column": "content"},
                "vectorizer": {"model": "nomic-embed-text"},
                "formatting": {"template": "$chunk $content"},
            }
        }
    }
    
    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(global_settings)}):
        test_logger = logging.getLogger(__name__)
        async with await connect_db(global_settings.source_url, autocommit=True) as conn:
            await robust_slot_cleanup(conn, "sub_swap", test_logger)
            await conn.execute("DROP TABLE IF EXISTS swap_products CASCADE")
            await conn.execute("CREATE TABLE swap_products (id SERIAL PRIMARY KEY, name TEXT, description TEXT, content TEXT)")
            await conn.execute("INSERT INTO swap_products (name, description, content) VALUES ('N', 'D', 'initial content')")

        async with await connect_db(global_settings.resolved_sink_url, autocommit=True) as conn:
            await conn.execute("DROP TABLE IF EXISTS swap_products CASCADE")
            await conn.execute("DELETE FROM _replica_state WHERE key = 'sub_swap'")
            await conn.execute("DROP VIEW IF EXISTS swap_search CASCADE")
            try: await conn.execute("DELETE FROM ai.vectorizer")
            except: pass

        async with PGSearchReplica(sync=True, **base_config) as replica:
            assert await wait_for_pgai_sync(replica.settings, "swap")
            
            async with await connect_db(replica.settings.resolved_sink_url) as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT table_name FROM information_schema.view_table_usage WHERE view_name = 'swap_search' AND (table_name LIKE '%%_store_v%%' OR table_name LIKE '%%_embedding%%')")
                    row = await cur.fetchone()
                    if not row:
                        await cur.execute("SELECT view_name, table_name FROM information_schema.view_table_usage WHERE view_name = 'swap_search'")
                        all_usage = await cur.fetchall()
                        test_logger.info(f"Usage for swap_search: {all_usage}")
                    v1 = row[0] if row else None
            
        # Trigger swap
        new_config = {
            "replicas": {
                "swap": {
                    "source": {"table": "swap_products", "content_column": "content"},
                    "vectorizer": {"model": "nomic-embed-text"},
                    "formatting": {
                        "template": "$chunk NEW $content",
                        "chunking_strategy": "recursive_character_text_splitter"
                    },
                }
            }
        }
        async with PGSearchReplica(sync=True, **new_config) as replica:
            async with await connect_db(replica.settings.resolved_sink_url) as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT table_name FROM information_schema.view_table_usage WHERE view_name = 'swap_search' AND (table_name LIKE '%%_store_v%%' OR table_name LIKE '%%_embedding%%')")
                    row = await cur.fetchone()
                    if not row:
                        await cur.execute("SELECT view_name, table_name FROM information_schema.view_table_usage WHERE view_name = 'swap_search'")
                        all_usage = await cur.fetchall()
                        test_logger.info(f"Usage for swap_search (v2): {all_usage}")
                    v2 = row[0] if row else None
            assert v1 is not None and v2 is not None, f"Metadata views not created (v1={v1}, v2={v2})"
            assert v1 != v2, "Blue-Green swap failed"
