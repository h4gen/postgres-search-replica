import pytest
import asyncio
from pg_replica import PGSearchReplica, settings as global_settings
from pg_replica.database import connect_db, check_slot_exists


async def wait_for_pgai_sync(settings, expected_count=1, timeout=60):
    """Wait for pgai vectorizer to finish processing all rows."""
    import time
    import logging

    logger = logging.getLogger(__name__)
    start_time = time.time()

    # We resolve the actual embedding source from the search replica view
    # to be robust against versioned table names.
    embedding_table = None

    logger.info(f"Waiting for {expected_count} embeddings...")
    while time.time() - start_time < timeout:
        async with await connect_db(settings.resolved_sink_url) as conn:
            async with conn.cursor() as cur:
                # 0. Resolve the actual embedding source if possible
                try:
                    await cur.execute(
                        """
                        SELECT table_name 
                        FROM information_schema.view_table_usage 
                        WHERE view_name = %s 
                        AND (table_name LIKE '%%_store_v%%' OR table_name LIKE '%%_embedding%%')
                        LIMIT 1
                        """,
                        (settings.sink_replica_table,),
                    )
                    row = await cur.fetchone()
                    if row:
                        embedding_table = row[0]
                except Exception:
                    pass

                # If we couldn't resolve it, use a deterministic guess
                current_table = (
                    embedding_table
                    or f"{settings.sink_raw_table}_store_v{settings.get_version_id()}"
                )

                # 2. Check if subscription still exists (Optimization for self-destruct tests)
                await cur.execute(
                    "SELECT 1 FROM pg_subscription WHERE subname = %s",
                    (settings.subscription_name,),
                )
                if not await cur.fetchone():
                    logger.info("Subscription is gone, stopping sync wait.")
                    return False

                # 3. Check embedding count
                try:
                    await cur.execute(
                        f"SELECT count(*) FROM {current_table} WHERE {settings.embedding_column} IS NOT NULL"
                    )
                    res = await cur.fetchone()
                    count = res[0] if res else 0
                    logger.info(
                        f"Current embedding count: {count}/{expected_count} in {current_table}"
                    )
                    if count >= expected_count:
                        logger.info("pgai sync complete.")
                        return True
                except Exception as e:
                    logger.info(
                        f"Waiting for embedding table {current_table} to be created... ({e})"
                    )
        await asyncio.sleep(2)
    logger.error("pgai sync timed out.")
    return False


def get_internal_source_url(settings):
    """Helper to translate localhost URL to internal Docker URL for subscription."""
    return settings.source_url.replace("localhost:5433", "source:5432").replace(
        "127.0.0.1:5433", "source:5432"
    )


@pytest.mark.asyncio
async def test_uuid_recovery_flow():
    """
    Test that the system can recover and catch up using UUID primary keys.
    """
    from unittest.mock import patch

    # 1. Setup Source table with UUIDs
    custom_settings = global_settings.model_copy(
        update={
            "source_table": "uuid_products",
            "sink_raw_table": "uuid_products",
            "publication_name": "pub_uuid_products",
            "subscription_name": "sub_uuid_products",
            "id_column": "id",
        }
    )

    with patch.dict(
        "os.environ",
        {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(custom_settings)},
    ):
        # 1. Setup Source table with UUIDs
        async with await connect_db(
            custom_settings.source_url, autocommit=True
        ) as conn:
            await conn.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
            await conn.execute("DROP TABLE IF EXISTS uuid_products CASCADE")
            await conn.execute(
                "CREATE TABLE uuid_products (id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), name TEXT, description TEXT)"
            )
            await conn.execute(
                "INSERT INTO uuid_products (name, description) VALUES ('UUID Item', 'Testing UUID support')"
            )

        # Start replica in sync mode
        async with PGSearchReplica(
            sync=True, **custom_settings.model_dump()
        ) as replica:
            # Wait for initial sync
            await wait_for_pgai_sync(replica.settings, expected_count=1)

            # Verify data
            results = await replica.search("UUID Item")
            assert len(results) > 0
            assert "UUID Item" in results[0]["content"]


@pytest.mark.asyncio
async def test_anti_entropy_ghost_cleaner():
    """
    Test that hard-deleted records are cleaned up by Anti-Entropy during recovery.
    """
    from unittest.mock import patch

    custom_settings = global_settings.model_copy(
        update={
            "source_table": "ghost_products",
            "sink_raw_table": "ghost_products",
            "publication_name": "pub_ghost",
            "subscription_name": "sub_ghost",
        }
    )

    with patch.dict(
        "os.environ",
        {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(custom_settings)},
    ):
        # 1. Setup initial state with 2 items
        async with await connect_db(
            custom_settings.source_url, autocommit=True
        ) as conn:
            await conn.execute("DROP TABLE IF EXISTS ghost_products CASCADE")
            await conn.execute(
                "CREATE TABLE ghost_products (id INT PRIMARY KEY, name TEXT, description TEXT)"
            )
            await conn.execute(
                "INSERT INTO ghost_products (id, name, description) VALUES (1, 'Item 1', 'Desc 1'), (2, 'Item 2', 'Desc 2')"
            )

        # Start and sync
        async with PGSearchReplica(
            sync=True, **custom_settings.model_dump()
        ) as replica:
            await wait_for_pgai_sync(replica.settings, expected_count=2)

            # 2. Stop daemon and "Hard Delete" from Source
            # Note: We stop the orchestrator manually to simulate crash/down-time
            await replica.stop()

            async with await connect_db(
                custom_settings.source_url, autocommit=True
            ) as conn:
                await conn.execute("DELETE FROM ghost_products WHERE id = 1")
                # Also drop slot to force recovery mode on restart
                # Use a safe drop that doesn't fail if already gone
                await conn.execute(
                    f"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = '{custom_settings.subscription_name}'"
                )

            # 3. Restart and verify Ghost is gone
            await replica.start(sync=True)

            # Anti-Entropy should have deleted Item 1 from Sink
            # We check the raw table in Sink
            found = True
            for _ in range(10):
                async with await connect_db(
                    replica.settings.resolved_sink_url
                ) as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            "SELECT count(*) FROM ghost_products WHERE id = 1"
                        )
                        res = await cur.fetchone()
                        if res[0] == 0:
                            found = False
                            break
                await asyncio.sleep(1)

            assert not found, "Ghost record was not deleted by Anti-Entropy"

            # Item 2 should still be there
            async with await connect_db(
                replica.settings.resolved_sink_url
            ) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT count(*) FROM ghost_products WHERE id = 2"
                    )
                    res = await cur.fetchone()
                    assert res[0] == 1


@pytest.mark.asyncio
async def test_self_destruct_and_auto_heal():
    """
    Full lifecycle test:
    1. Replicate data
    2. Trigger Watchdog self-destruct (nukes slot)
    3. Restart daemon
    4. Verify it healed itself via SQL catch-up
    """
    from unittest.mock import patch
    from pg_replica.database import check_and_protect_source

    import logging

    logger = logging.getLogger(__name__)

    custom_settings = global_settings.model_copy(
        update={
            "source_table": "heal_products",
            "sink_raw_table": "heal_products",
            "publication_name": "pub_heal",
            "subscription_name": "sub_heal",
            "max_slot_wal_keep_size_mb": -1,  # Trigger instant self-destruct
        }
    )

    with patch.dict(
        "os.environ",
        {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(custom_settings)},
    ):
        # 1. Setup Source
        async with await connect_db(
            custom_settings.source_url, autocommit=True
        ) as conn:
            await conn.execute("DROP TABLE IF EXISTS heal_products CASCADE")
            await conn.execute(
                "CREATE TABLE heal_products (id INT PRIMARY KEY, name TEXT, description TEXT)"
            )
            await conn.execute(
                "INSERT INTO heal_products (id, name, description) VALUES (1, 'Initial', 'Pre-destruct')"
            )

        # 2. Start and Sync Initial Data
        async with PGSearchReplica(
            sync=True, **custom_settings.model_dump()
        ) as replica:
            await wait_for_pgai_sync(replica.settings, expected_count=1)

        # 3. Trigger self-destruct (Using a fresh instance with max_slot_wal_keep_size_mb=-1)
        # This simulates the daemon being killed by its own watchdog.
        custom_settings.max_slot_wal_keep_size_mb = -1
        async with PGSearchReplica(
            sync=True, **custom_settings.model_dump()
        ) as replica:
            # We wait for the background monitor to trigger it or call it manually
            try:
                await check_and_protect_source(replica.settings)
            except RuntimeError as e:
                if "Self-destructed" not in str(e):
                    raise

            # Verify slot is gone
            slot_still_exists = True
            for _ in range(10):
                if not await check_slot_exists(replica.settings):
                    slot_still_exists = False
                    break
                await asyncio.sleep(1)

            assert (
                not slot_still_exists
            ), "Replication slot was not dropped by Watchdog"

        # 4. Insert data while "dead"
        async with await connect_db(
            custom_settings.source_url, autocommit=True
        ) as conn:
            await conn.execute(
                "INSERT INTO heal_products (id, name, description) VALUES (2, 'During Gap', 'Post-destruct')"
            )

        # 5. Restart with a NEW instance (Daemon should auto-heal)
        custom_settings.max_slot_wal_keep_size_mb = 1024
        async with PGSearchReplica(
            sync=True, **custom_settings.model_dump()
        ) as replica:
            # Diagnostic: check sink raw table
            async with await connect_db(
                replica.settings.resolved_sink_url
            ) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        f"SELECT count(*) FROM {replica.settings.sink_raw_table}"
                    )
                    cnt = (await cur.fetchone())[0]
                    logger.info(
                        f"Diagnostic: Sink raw table {replica.settings.sink_raw_table} has {cnt} rows"
                    )

                    await cur.execute("SELECT * FROM ai.vectorizer_errors")
                    errs = await cur.fetchall()
                    if errs:
                        logger.error(f"Diagnostic: pgai errors: {errs}")

            # Wait for catch-up and vectorization of both records
            await wait_for_pgai_sync(
                replica.settings, expected_count=2, timeout=60
            )

            # Verify both records are there
            results = await replica.search("destruct")
            assert len(results) >= 2
            contents = [r["content"] for r in results]
            assert any("Pre-destruct" in c for c in contents)
            assert any("Post-destruct" in c for c in contents)
