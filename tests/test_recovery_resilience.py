import pytest
import asyncio
from pg_replica.client import PGSearchReplica 
from pg_replica.config import SearchPipeline, IngestConfig, PipelineConfig, StorageConfig, EmbeddingConfig
from pg_replica.database import connect_db, check_slot_exists, check_and_protect_source, dict_row, find_and_fix_ghost_records, init_pools, close_pools

@pytest.mark.asyncio
async def test_uuid_recovery_flow(clean_db, robust_slot_cleanup, internal_source_url, source_conn, sink_conn, wait_for_pgai_sync):
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

    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": internal_source_url}):
        # Setup Source
        await source_conn.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
        await source_conn.execute("DROP TABLE IF EXISTS uuid_products CASCADE")
        await source_conn.execute("CREATE TABLE uuid_products (id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), name TEXT, description TEXT)")
        await source_conn.execute("INSERT INTO uuid_products (name, description) VALUES ('UUID Item', 'Testing UUID support')")

        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            assert await wait_for_pgai_sync(replica.settings, "uuid", expected_count=1)
            results = await replica.search("UUID Item", table="uuid")
            assert len(results) > 0
            assert "UUID Item" in results[0]["content"]


@pytest.mark.asyncio
async def test_anti_entropy_ghost_cleaner(clean_db, robust_slot_cleanup, internal_source_url, source_conn, sink_conn, wait_for_pgai_sync):
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

    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": internal_source_url}):
        # Setup Source
        await source_conn.execute("DROP TABLE IF EXISTS ghost_products CASCADE")
        await source_conn.execute("CREATE TABLE ghost_products (id INT PRIMARY KEY, name TEXT, description TEXT)")
        await source_conn.execute("INSERT INTO ghost_products (id, name, description) VALUES (1, 'Item 1', 'Desc 1'), (2, 'Item 2', 'Desc 2')")
        
        # Pre-initialize infrastructure using standard path
        async with PGSearchReplica(sync=False, **custom_settings) as pre_init:
            from pg_replica.database import ensure_outbox_infrastructure, init_pools, close_pools
            await init_pools(pre_init.settings)
            await ensure_outbox_infrastructure(pre_init.settings)
            await close_pools()

        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            # Initialize pools for backend function usage
            await init_pools(replica.settings)
            try:
                assert await wait_for_pgai_sync(replica.settings, "ghost", expected_count=2)
                await replica.stop()
    
                # Create a ghost: Delete from Source while Replica is down
                async with source_conn.cursor() as cur:
                    await cur.execute("DELETE FROM ghost_products WHERE id = 1")
                    # Force recovery mode on restart by dropping slot
                    await cur.execute("SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'sub_ghost'")
    
                await replica.start(sync=True)
    
                # Anti-Entropy should delete Item 1 from Sink
                found = True
                for _ in range(30):
                    async with sink_conn.cursor() as cur:
                        await cur.execute("SELECT count(*) FROM ghost_products WHERE id = 1")
                        if (await cur.fetchone())[0] == 0:
                            found = False
                            break
                    await asyncio.sleep(1)
                assert not found, "Ghost record not deleted"
            finally:
                await close_pools()


@pytest.mark.asyncio
async def test_self_destruct_and_auto_heal(clean_db, robust_slot_cleanup, internal_source_url, source_conn, sink_conn, wait_for_pgai_sync):
    """Test Watchdog self-destruct and subsequent auto-healing."""
    from unittest.mock import patch
    import logging
    test_logger = logging.getLogger(__name__)
    
    # Start HEALTHY
    custom_settings = {
        "max_slot_wal_keep_size_mb": 1024,
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

    with patch.dict("os.environ", {"SUBSCRIPTION_SOURCE_URL": internal_source_url}):
        # Setup Source
        await source_conn.execute("DROP TABLE IF EXISTS heal_products CASCADE")
        await source_conn.execute("CREATE TABLE heal_products (id INT PRIMARY KEY, name TEXT, description TEXT)")
        await source_conn.execute("INSERT INTO heal_products (id, name, description) VALUES (1, 'Initial', 'Pre-destruct')")

        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            # Initialize pools for backend function usage (check_slot_exists)
            await init_pools(replica.settings)
            try:
                # 1. Verify stable start
                assert await wait_for_pgai_sync(replica.settings, "heal", expected_count=1)
                

                # 2. Inject POISON to trigger self-destruct
                test_logger.info("Injecting poison config (-1 MB limit)...")
                replica.settings.max_slot_wal_keep_size_mb = -1
                await replica.sync_settings()

                # 3. Wait for self-destruct
                test_logger.info("Waiting for self-destruct...")
                destructed = False
                for _ in range(300):
                    if not await check_slot_exists(replica.settings, "heal"):
                        destructed = True
                        break
                    await asyncio.sleep(0.1)

                assert destructed, "Watchdog failed to self-destruct slot"

                # 4. Remove poison immediately so it can heal
                test_logger.info("Removing poison config...")
                replica.settings.max_slot_wal_keep_size_mb = 1024
                await replica.sync_settings()

                # 5. Verify auto-heal
                test_logger.info("Waiting for auto-heal...")
                healed = False
                for _ in range(120):
                    if await check_slot_exists(replica.settings, "heal"):
                        healed = True
                        break
                    await asyncio.sleep(0.5)
                assert healed, "Auto-heal failed to recreate slot"
            finally:
                await close_pools()

        # 6. Final verification after restart
        test_logger.info("Starting final verification after restart...")
        # Insert data while "dead" (between blocks)
        await source_conn.execute("INSERT INTO heal_products (id, name, description) VALUES (2, 'During Gap', 'Post-destruct')")

        # Restart and heal
        custom_settings["max_slot_wal_keep_size_mb"] = 1024
        async with PGSearchReplica(sync=True, **custom_settings) as replica:
            assert await wait_for_pgai_sync(replica.settings, "heal", expected_count=2, timeout=60)
            results = await replica.search("destruct", table="heal")
            assert len(results) >= 2
