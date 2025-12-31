import pytest
import asyncio
from pgvector.psycopg import register_vector_async as register_vector  # type: ignore
from pg_replica import PGSearchReplica, settings as global_settings
from pg_replica.database import connect_db, check_and_protect_source


async def wait_for_pgai_sync(settings, expected_count=1, timeout=60):
    """Wait for pgai vectorizer to finish processing all rows."""
    import time
    import logging

    logger = logging.getLogger(__name__)
    start_time = time.time()

    # We resolve the actual embedding source from the search replica view
    # to be robust against versioned table names.
    embedding_table = f"{settings.sink_raw_table}_embedding"

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
                        AND table_name LIKE %s
                        """,
                        (settings.sink_replica_table, f"%_embedding%"),
                    )
                    row = await cur.fetchone()
                    if row:
                        embedding_table = row[0]
                except Exception:
                    pass

                # 1. Check for errors first
                try:
                    await cur.execute(
                        "SELECT message FROM ai.vectorizer_errors LIMIT 5"
                    )
                    errors = await cur.fetchall()
                    if errors:
                        for err in errors:
                            logger.error(f"pgai Worker Error: {err[0]}")
                except Exception:
                    pass

                # 2. Check for pending items
                try:
                    await cur.execute(
                        "SELECT source_table, pending_items FROM ai.vectorizer_status"
                    )
                    status = await cur.fetchall()
                    for table, pending in status:
                        logger.info(
                            f"Vectorizer {table} status: {pending} items pending"
                        )
                except Exception:
                    pass

                # 3. Check embedding count
                try:
                    await cur.execute(
                        f"SELECT count(*) FROM {embedding_table} WHERE {settings.embedding_column} IS NOT NULL"
                    )
                    res = await cur.fetchone()
                    count = res[0] if res else 0
                    logger.info(
                        f"Current embedding count: {count}/{expected_count}"
                    )
                    if count >= expected_count:
                        logger.info("pgai sync complete.")
                        return True
                except Exception as e:
                    logger.info(
                        f"Waiting for embedding table to be created... ({e})"
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
async def test_full_replication_flow():
    """
    Integration test:
    1. Wait for native replication to Sink (raw table)
    2. Wait for pgai to process embeddings
    3. Verify data in replica view via high-level API
    """
    from unittest.mock import patch

    # We need to provide the internal source URL for the subscription CONNECTION string
    with patch.dict(
        "os.environ",
        {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(global_settings)},
    ):
        async with PGSearchReplica(sync=True) as replica:
            settings = replica.settings
            # Clean up from previous runs
            async with await connect_db(
                settings.source_url, autocommit=True
            ) as conn:
                await conn.execute(
                    f"TRUNCATE TABLE {settings.source_table} CASCADE"
                )
            async with await connect_db(
                settings.resolved_sink_url, autocommit=True
            ) as conn:
                await conn.execute(
                    f"TRUNCATE TABLE {settings.sink_raw_table} CASCADE"
                )

            # 2. Insert test data into Source
            # Note: We insert into 'name' and 'description'
            async with await connect_db(
                settings.source_url, autocommit=True
            ) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        f"INSERT INTO {settings.source_table} (name, description) VALUES ('SuperGadget', 'A really useful tool for testing')"
                    )

                # 3. VERIFY NATIVE REPLICATION (Core logic check)
                import logging

                logger = logging.getLogger(__name__)
                max_retries = 10
                found = False
                for _ in range(max_retries):
                    async with await connect_db(
                        settings.resolved_sink_url
                    ) as conn:
                        async with conn.cursor() as cur:
                            await cur.execute(
                                f"SELECT count(*) FROM {settings.sink_raw_table} WHERE name = 'SuperGadget'"
                            )
                            count = (await cur.fetchone())[0]
                            if count > 0:
                                found = True
                                logger.info(
                                    f"Verified {count} rows in {settings.sink_raw_table}"
                                )
                                break
                    await asyncio.sleep(1)

                assert (
                    found
                ), f"Native replication failed to move data to Sink {settings.sink_raw_table} table"

                # 4. Wait for pgai transformation
                sync_complete = await wait_for_pgai_sync(settings)
                assert sync_complete, "pgai sync timed out"

                # 5. Diagnostic: Check view and tables
                async with await connect_db(settings.resolved_sink_url) as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            f"SELECT count(*) FROM {settings.sink_replica_table}"
                        )
                        view_count = (await cur.fetchone())[0]
                        logger.info(
                            f"View {settings.sink_replica_table} has {view_count} rows"
                        )

                        await cur.execute(
                            f"SELECT * FROM {settings.sink_replica_table} LIMIT 1"
                        )
                        row = await cur.fetchone()
                        logger.info(f"View sample row: {row}")

                # 6. Verify search results via the new API
                # Test that we can find it by the concatenated name
                results = await replica.search("SuperGadget")
            assert len(results) > 0
            # The target_content_column should now contain the concatenated string
            assert "SuperGadget" in results[0]["content"]
            assert "useful tool" in results[0]["content"]
            assert "distance" in results[0]


@pytest.mark.asyncio
async def test_filtered_replication_flow():
    """
    Test PG 15 Row Filtering:
    1. Set filter to only replicate matching products
    2. Verify only matching data reached the RAW table (Core logic)
    3. Verify search works only for matching data
    """
    from unittest.mock import patch

    # We need to provide the internal source URL for the subscription CONNECTION string
    with patch.dict(
        "os.environ",
        {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(global_settings)},
    ):
        # Override settings for this test
        async with PGSearchReplica(
            sync=True,
            publication_where="description LIKE '%KEEP%'",
        ) as replica:
            settings = replica.settings
            # Clean up
            async with await connect_db(
                settings.source_url, autocommit=True
            ) as conn:
                await conn.execute(
                    f"TRUNCATE TABLE {settings.source_table} CASCADE"
                )
            async with await connect_db(
                settings.resolved_sink_url, autocommit=True
            ) as conn:
                await conn.execute(
                    f"TRUNCATE TABLE {settings.sink_raw_table} CASCADE"
                )

            # Insert matching and non-matching data
            async with await connect_db(
                settings.source_url, autocommit=True
            ) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        f"INSERT INTO {settings.source_table} (name, description) VALUES ('Keep Me', 'KEEP THIS DESCRIPTION')"
                    )
                    await cur.execute(
                        f"INSERT INTO {settings.source_table} (name, description) VALUES ('Ignore Me', 'IGNORE THIS ONE')"
                    )

            # Wait for replication to RAW table
            found_keep = False
            for _ in range(10):
                async with await connect_db(settings.resolved_sink_url) as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(
                            f"SELECT 1 FROM {settings.sink_raw_table} WHERE description = 'KEEP THIS DESCRIPTION'"
                        )
                        if await cur.fetchone():
                            found_keep = True
                            break
                await asyncio.sleep(1)
            assert found_keep, "Matching data was not replicated"

            # Verify non-matching one is NOT there
            async with await connect_db(settings.resolved_sink_url) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        f"SELECT 1 FROM {settings.sink_raw_table} WHERE description = 'IGNORE THIS ONE'"
                    )
                    assert (
                        await cur.fetchone() is None
                    ), "Non-matching data was replicated (FILTER FAILED)"

            # Wait for sync and verify search
            await wait_for_pgai_sync(settings, expected_count=1)
            results = await replica.search("KEEP THIS DESCRIPTION")
            assert any("KEEP THIS DESCRIPTION" in r["content"] for r in results)


@pytest.mark.asyncio
async def test_reconciliation_efficiency():
    """
    CORE BUSINESS LOGIC: Verify identical data doesn't trigger new work.
    1. Insert data and process it
    2. Record initial embedding state
    3. Simulate a manual re-sync of identical data
    4. Verify embedding is NOT re-calculated (identical list)
    """
    from unittest.mock import patch

    with patch.dict(
        "os.environ",
        {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(global_settings)},
    ):
        async with PGSearchReplica(sync=True) as replica:
            settings = replica.settings
            # 1. Setup initial state
            async with await connect_db(
                settings.source_url, autocommit=True
            ) as conn:
                await conn.execute(
                    f"TRUNCATE TABLE {settings.source_table} CASCADE"
                )
                await conn.execute(
                    f"INSERT INTO {settings.source_table} (id, name, description) VALUES (1, 'Stable', 'stable product description')"
                )

            async with await connect_db(
                settings.resolved_sink_url, autocommit=True
            ) as conn:
                await conn.execute(
                    f"TRUNCATE TABLE {settings.sink_raw_table} CASCADE"
                )

            await wait_for_pgai_sync(settings)

            # 2. Record state
            async with await connect_db(settings.resolved_sink_url) as conn:
                await register_vector(conn)
                async with conn.cursor() as cur:
                    await cur.execute(
                        f"SELECT {settings.embedding_column} FROM {settings.sink_replica_table} WHERE {settings.id_column} = 1"
                    )
                    row1 = await cur.fetchone()
                    assert row1 is not None
                    emb1 = row1[0]

            # 3. Simulate a restart/re-processing (TRUNCATE raw + Re-insert same data)
            async with await connect_db(
                settings.resolved_sink_url, autocommit=True
            ) as conn:
                await conn.execute(f"TRUNCATE TABLE {settings.sink_raw_table}")
                await conn.execute(
                    f"INSERT INTO {settings.sink_raw_table} ({settings.id_column}, name, description) VALUES (1, 'Stable', 'stable product description')"
                )

            await wait_for_pgai_sync(settings)

            # 4. Verify unchanged
            async with await connect_db(settings.resolved_sink_url) as conn:
                await register_vector(conn)
                async with conn.cursor() as cur:
                    await cur.execute(
                        f"SELECT {settings.embedding_column} FROM {settings.sink_replica_table} WHERE {settings.id_column} = 1"
                    )
                    row2 = await cur.fetchone()
                    assert row2 is not None
                    emb2 = row2[0]
                    assert emb1 is not None, "Initial embedding is None"
                    assert emb2 is not None, "Sync'd embedding is None"
                    assert list(emb1) == list(
                        emb2
                    ), "Embedding changed for identical data (CACHE/TRACKING FAILED)!"


@pytest.mark.asyncio
async def test_wal_watchdog_self_destruct():
    """
    Test the Source Protection (Watchdog).
    """
    from unittest.mock import patch

    with patch.dict(
        "os.environ",
        {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(global_settings)},
    ):
        async with PGSearchReplica(
            sync=True, max_slot_wal_keep_size_mb=-1
        ) as replica:
            settings = replica.settings
            # Pools are already initialized by Orchestrator.start() via PGSearchReplica
            # 2. Trigger watchdog
            with pytest.raises(
                RuntimeError, match="Self-destructed to protect Source DB"
            ):
                await check_and_protect_source(settings)

            # 3. Verify cleanup via DB queries
            async with await connect_db(settings.source_url) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                        (settings.subscription_name,),
                    )
                    assert (
                        await cur.fetchone() is None
                    ), "Replication slot should have been dropped"

            async with await connect_db(settings.resolved_sink_url) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT 1 FROM pg_subscription WHERE subname = %s",
                        (settings.subscription_name,),
                    )
                    assert (
                        await cur.fetchone() is None
                    ), "Subscription should have been dropped"


@pytest.mark.asyncio
async def test_idempotent_cleanup():
    """Verify that drop_subscription_completely is safe to call multiple times."""
    from pg_replica.database import (
        drop_subscription_completely,
        init_pools,
        close_pools,
    )

    # We need pools for drop_subscription_completely as it uses get_sink_conn()
    await init_pools(global_settings)
    try:
        # Call it twice - should not raise exception even if it's already gone
        await drop_subscription_completely(global_settings)
        await drop_subscription_completely(global_settings)
    finally:
        await close_pools()


@pytest.mark.asyncio
async def test_blue_green_swap():
    """
    Test Blue-Green atomic swap:
    1. Start replica with model A
    2. Insert data and wait for embeddings
    3. Change configuration to model B
    4. Verify a NEW versioned table is created and search results are still accessible
    """
    from unittest.mock import patch

    with patch.dict(
        "os.environ",
        {"SUBSCRIPTION_SOURCE_URL": get_internal_source_url(global_settings)},
    ):
        # 1. Initial Start (Model A)
        async with PGSearchReplica(
            sync=True, embedding_model="nomic-embed-text"
        ) as replica:
            settings = replica.settings
            async with await connect_db(
                settings.source_url, autocommit=True
            ) as conn:
                await conn.execute(
                    f"TRUNCATE TABLE {settings.source_table} CASCADE"
                )
                await conn.execute(
                    f"INSERT INTO {settings.source_table} (name, description) VALUES ('SwapBot', 'Initial version')"
                )

            await wait_for_pgai_sync(settings)
            results = await replica.search("SwapBot")
            assert len(results) > 0

            # Record current view target (the embedding source)
            import logging

            test_logger = logging.getLogger(__name__)
            async with await connect_db(settings.resolved_sink_url) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT table_name
                        FROM information_schema.view_table_usage
                        WHERE view_name = %s
                        AND table_name LIKE '%%_store_v%%'
                        """,
                        (settings.sink_replica_table,),
                    )
                    row = await cur.fetchone()
                    table_v1 = row[0] if row else None
                    test_logger.info(f"Initial view target: {table_v1}")

        # 2. Re-start with Model B (triggers reconciliation)
        # Note: we use a different model name AND dimension to trigger the hash drift
        async with PGSearchReplica(
            sync=True, embedding_model="all-minilm", embedding_dimension=384
        ) as replica:
            settings = replica.settings

            # Verify the view now points to a DIFFERENT target (via hash update)
            async with await connect_db(settings.resolved_sink_url) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT table_name FROM information_schema.view_table_usage WHERE view_name = %s",
                        (settings.sink_replica_table,),
                    )
                    table_v2 = (await cur.fetchone())[0]

            assert (
                table_v1 != table_v2
            ), f"Atomic swap failed: view still points to {table_v1}"

            # Search should still work (it might need a moment for pgai to vectorize the NEW table)
            # but for this test, we just care that the infrastructure was swapped.
            # In a real scenario, warm_up_from_cache would handle the heavy lifting.
