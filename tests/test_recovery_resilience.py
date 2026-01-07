import pytest
import asyncio
from pg_replica import PGSearchReplica, settings as global_settings
from pg_replica.config_v2 import SearchPipeline, IngestConfig, PipelineConfig, StorageConfig, EmbeddingConfig
from pg_replica.database import connect_db, check_slot_exists, check_and_protect_source, dict_row


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


async def wait_for_pgai_sync(settings, target_name, expected_count=1, timeout=60):
    """Wait for pgai vectorizer to finish processing all rows."""
    import time
    import logging
    logger = logging.getLogger(__name__)
    start_time = time.time()
    config = settings.pipelines[target_name]
    embedding_table = None

    logger.info(f"Waiting for {expected_count} embeddings for {target_name}...")
    while time.time() - start_time < timeout:
        async with await connect_db(settings.resolved_sink_url) as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT table_name FROM information_schema.view_table_usage WHERE view_name = %s AND (table_name LIKE '%%_store_v%%' OR table_name LIKE '%%_embedding%%') LIMIT 1",
                        (f"{config.ingest.table}_search",),
                    )
                    row = await cur.fetchone()
                    if row: embedding_table = row[0]
                except Exception: pass

                current_table = embedding_table or f"{config.ingest.table}_store_v{config.get_version_id()}"

                # Check if subscription still exists
                sub_name = f"sub_{target_name}"
                await cur.execute("SELECT 1 FROM pg_subscription WHERE subname = %s", (sub_name,))
                if not await cur.fetchone():
                    logger.info(f"Subscription {sub_name} is gone, stopping sync wait.")
                    return False

                try:
                    await cur.execute(f"SELECT count(*) FROM {current_table} WHERE embedding IS NOT NULL")
                    res = await cur.fetchone()
                    count = res[0] if res else 0
                    if count >= expected_count: return True
                except Exception: pass
        await asyncio.sleep(2)
    return False


def get_internal_source_url(settings):
    """Helper to translate localhost URL to internal Docker URL."""
    return settings.source_url.replace("localhost:5433", "source:5432").replace("127.0.0.1:5433", "source:5432")


@pytest.mark.asyncio
async def test_uuid_recovery_flow():
    """Test recovery and catch up with UUID primary keys."""
    from unittest.mock import patch
    custom_settings = {
        "pipelines": {
            "uuid": SearchPipeline(
                ingest=IngestConfig(table="uuid_products", columns=["name", "description"], p_key="id"),
                pipeline=PipelineConfig(
                    template="$chunk $name", 
                    content_column="description",
                    embedding=EmbeddingConfig(provider="ollama", model="nomic-embed-text", dimension=768)
                )
            )
        }
    }

    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(global_settings)}):
        import logging
        test_logger = logging.getLogger(__name__)

        async with await connect_db(global_settings.source_url, autocommit=True) as conn:
            await robust_slot_cleanup(conn, "sub_uuid", test_logger)
            await conn.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
            await conn.execute("DROP TABLE IF EXISTS uuid_products CASCADE")
            await conn.execute("CREATE TABLE uuid_products (id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), name TEXT, description TEXT)")
            await conn.execute("INSERT INTO uuid_products (name, description) VALUES ('UUID Item', 'Testing UUID support')")

        async with await connect_db(global_settings.resolved_sink_url, autocommit=True) as conn:
            await robust_subscription_cleanup(conn, "sub_uuid", test_logger)
            await conn.execute("DROP TABLE IF EXISTS uuid_products CASCADE")
            await conn.execute("DELETE FROM _replica_state WHERE key = 'sub_uuid'")
            try: await conn.execute("DELETE FROM ai.vectorizer")
            except: pass

        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            assert await wait_for_pgai_sync(replica.settings, "uuid", expected_count=1)
            results = await replica.search("UUID Item", table="uuid")
            assert len(results) > 0
            assert "UUID Item" in results[0]["content"]


@pytest.mark.asyncio
async def test_anti_entropy_ghost_cleaner():
    """Test that hard-deleted records are cleaned up by Anti-Entropy."""
    from unittest.mock import patch
    custom_settings = {
        "pipelines": {
            "ghost": SearchPipeline(
                ingest=IngestConfig(table="ghost_products", columns=["name", "description"], p_key="id"),
                pipeline=PipelineConfig(
                    template="$chunk $name", 
                    content_column="description",
                    embedding=EmbeddingConfig(provider="ollama", model="nomic-embed-text", dimension=768)
                )
            )
        }
    }

    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(global_settings)}):
        import logging
        test_logger = logging.getLogger(__name__)

        async with await connect_db(global_settings.source_url, autocommit=True) as conn:
            await robust_slot_cleanup(conn, "sub_ghost", test_logger)
            await conn.execute("DROP TABLE IF EXISTS ghost_products CASCADE")
            await conn.execute("CREATE TABLE ghost_products (id INT PRIMARY KEY, name TEXT, description TEXT)")
            await conn.execute("INSERT INTO ghost_products (id, name, description) VALUES (1, 'Item 1', 'Desc 1'), (2, 'Item 2', 'Desc 2')")

        async with await connect_db(global_settings.resolved_sink_url, autocommit=True) as conn:
            await robust_subscription_cleanup(conn, "sub_ghost", test_logger)
            await conn.execute("DROP TABLE IF EXISTS ghost_products CASCADE")
            await conn.execute("DELETE FROM _replica_state WHERE key = 'sub_ghost'")
            try: await conn.execute("DELETE FROM ai.vectorizer")
            except: pass

        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            assert await wait_for_pgai_sync(replica.settings, "ghost", expected_count=2)
            await replica.stop()

            async with await connect_db(global_settings.source_url, autocommit=True) as conn:
                await conn.execute("DELETE FROM ghost_products WHERE id = 1")
                # Force recovery mode on restart by dropping slot
                await conn.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'sub_ghost'")

            await replica.start(sync=True)

            # Anti-Entropy should delete Item 1 from Sink
            found = True
            for _ in range(10):
                async with await connect_db(replica.settings.resolved_sink_url) as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT count(*) FROM ghost_products WHERE id = 1")
                        if (await cur.fetchone())[0] == 0:
                            found = False
                            break
                await asyncio.sleep(1)
            assert not found, "Ghost record not deleted"


@pytest.mark.asyncio
async def test_self_destruct_and_auto_heal():
    """Test Watchdog self-destruct and subsequent auto-healing."""
    from unittest.mock import patch
    custom_settings = {
        "max_slot_wal_keep_size_mb": -1, # Trigger instant self-destruct (Global setting)
        "pipelines": {
            "heal": SearchPipeline(
                ingest=IngestConfig(table="heal_products", columns=["name", "description"], p_key="id"),
                pipeline=PipelineConfig(
                    template="$chunk $name", 
                    content_column="description",
                    embedding=EmbeddingConfig(provider="ollama", model="nomic-embed-text", dimension=768)
                )
            )
        }
    }

    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(global_settings)}):
        import logging
        test_logger = logging.getLogger(__name__)

        async with await connect_db(global_settings.source_url, autocommit=True) as conn:
            await robust_slot_cleanup(conn, "sub_heal", test_logger)
            await conn.execute("DROP TABLE IF EXISTS heal_products CASCADE")
            await conn.execute("CREATE TABLE heal_products (id INT PRIMARY KEY, name TEXT, description TEXT)")
            await conn.execute("INSERT INTO heal_products (id, name, description) VALUES (1, 'Initial', 'Pre-destruct')")

        async with await connect_db(global_settings.resolved_sink_url, autocommit=True) as conn:
            await robust_subscription_cleanup(conn, "sub_heal", test_logger)
            await conn.execute("DROP TABLE IF EXISTS heal_products CASCADE")
            await conn.execute("DELETE FROM _replica_state WHERE key = 'sub_heal'")
            try: await conn.execute("DELETE FROM ai.vectorizer")
            except: pass

        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            assert await wait_for_pgai_sync(replica.settings, "heal", expected_count=1)
            
            # Trigger self-destruct manually or wait for watchdog
            try:
                await check_and_protect_source(replica.settings, "heal")
            except RuntimeError as e:
                if "Self-destructed" not in str(e): raise

            # Verify auto-heal restarts it
            healed = False
            for _ in range(45):
                if await check_slot_exists(replica.settings, "heal"):
                    healed = True
                    break
                await asyncio.sleep(1)
            assert healed, "Auto-heal failed to recreate slot"

        # Insert data while "dead"
        async with await connect_db(global_settings.source_url, autocommit=True) as conn:
            await conn.execute("INSERT INTO heal_products (id, name, description) VALUES (2, 'During Gap', 'Post-destruct')")

        # Restart and heal
        custom_settings["max_slot_wal_keep_size_mb"] = 1024
        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            assert await wait_for_pgai_sync(replica.settings, "heal", expected_count=2, timeout=60)
            results = await replica.search("destruct", table="heal")
            assert len(results) >= 2
