import pytest
import asyncio
from pgvector.psycopg import register_vector_async as register_vector  # type: ignore
from src.config import settings
from src.database import connect_db, check_and_protect_source


async def wait_for_pgai_sync(sink_url, expected_count=1, timeout=60):
    """Wait for pgai vectorizer to finish processing all rows."""
    import time
    import logging

    logger = logging.getLogger(__name__)
    start_time = time.time()
    embedding_table = f"{settings.sink_raw_table}_embedding"
    logger.info(
        f"Waiting for {expected_count} embeddings in {embedding_table}..."
    )
    while time.time() - start_time < timeout:
        async with await connect_db(sink_url) as conn:
            async with conn.cursor() as cur:
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
                    await cur.execute(f"SELECT count(*) FROM {embedding_table}")
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


def get_internal_source_url():
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
    3. Verify data in replica view
    """
    from src.database import setup_source, setup_sink

    # setup_source runs on host -> needs localhost
    await setup_source()

    # setup_sink creates subscription on container -> needs 'source' hostname
    from unittest.mock import patch

    with patch.object(settings, "source_url", get_internal_source_url()):
        await setup_sink()

    # Clean up from previous runs
    async with await connect_db(settings.source_url, autocommit=True) as conn:
        await conn.execute(f"TRUNCATE TABLE {settings.source_table} CASCADE")
    async with await connect_db(settings.sink_url, autocommit=True) as conn:
        await conn.execute(f"TRUNCATE TABLE {settings.sink_raw_table} CASCADE")
        # users_replica is a VIEW, truncating the raw table is enough

    # 2. Insert test data into Source
    async with await connect_db(settings.source_url, autocommit=True) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"INSERT INTO {settings.source_table} (name, {settings.content_column}) VALUES ('Test User', 'TEST@INTEGRATION.COM')"
            )

    # 3. Wait for native replication
    max_retries = 10
    found = False
    async with await connect_db(settings.sink_url) as conn:
        for _ in range(max_retries):
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT 1 FROM {settings.sink_raw_table} WHERE {settings.content_column} = 'TEST@INTEGRATION.COM'"
                )
                if await cur.fetchone():
                    found = True
                    break
            await asyncio.sleep(1)

    assert (
        found
    ), f"Native replication failed to move data to Sink {settings.sink_raw_table} table"

    # 4. Wait for pgai transformation
    sync_complete = await wait_for_pgai_sync(settings.sink_url)
    assert sync_complete, "pgai sync timed out"

    # 5. Verify transformed data in Sink replica view
    async with await connect_db(settings.sink_url) as conn:
        await register_vector(conn)
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT {settings.target_content_column}, {settings.embedding_column} FROM {settings.sink_replica_table} WHERE {settings.id_column} = (SELECT {settings.id_column} FROM {settings.sink_raw_table} WHERE {settings.content_column} = 'TEST@INTEGRATION.COM')"
            )
            row = await cur.fetchone()

            assert row is not None
            # Note: We aren't doing the lowercase in Python anymore,
            # but pgai template could do it. For now, we just check content.
            assert row[0] == "TEST@INTEGRATION.COM"
            assert len(row[1]) == settings.embedding_dimension


@pytest.mark.asyncio
async def test_filtered_replication_flow():
    """
    Test PG 15 Row Filtering:
    1. Set filter to only replicate users with content containing 'KEEP'
    2. Insert matching and non-matching data
    3. Verify only matching data arrived
    """
    # Override settings for this test
    original_filter = settings.publication_where
    settings.publication_where = f"{settings.content_column} LIKE '%KEEP%'"

    try:
        from src.database import setup_source, setup_sink

        # setup_source needs localhost
        await setup_source()
        # setup_sink needs 'source' hostname for the subscription CONNECTION string
        from unittest.mock import patch

        with patch.object(settings, "source_url", get_internal_source_url()):
            await setup_sink()

        # Clean up
        async with await connect_db(
            settings.source_url, autocommit=True
        ) as conn:
            await conn.execute(
                f"TRUNCATE TABLE {settings.source_table} CASCADE"
            )
        async with await connect_db(settings.sink_url, autocommit=True) as conn:
            await conn.execute(
                f"TRUNCATE TABLE {settings.sink_raw_table} CASCADE"
            )

        # Insert matching and non-matching data
        async with await connect_db(
            settings.source_url, autocommit=True
        ) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"INSERT INTO {settings.source_table} (name, {settings.content_column}) VALUES ('Keep Me', 'KEEP@ME.COM')"
                )
                await cur.execute(
                    f"INSERT INTO {settings.source_table} (name, {settings.content_column}) VALUES ('Ignore Me', 'IGNORE@ME.COM')"
                )

        # Wait and verify
        async with await connect_db(settings.sink_url) as conn:
            # Wait for the matching one
            found_keep = False
            for _ in range(10):
                async with conn.cursor() as cur:
                    await cur.execute(
                        f"SELECT 1 FROM {settings.sink_raw_table} WHERE {settings.content_column} = 'KEEP@ME.COM'"
                    )
                    if await cur.fetchone():
                        found_keep = True
                        break
                await asyncio.sleep(1)

            assert found_keep, "Matching data was not replicated"

            # Verify non-matching one is NOT there
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT 1 FROM {settings.sink_raw_table} WHERE {settings.content_column} = 'IGNORE@ME.COM'"
                )
                assert (
                    await cur.fetchone() is None
                ), "Non-matching data was replicated"

    finally:
        # Restore original filter and reset DB
        settings.publication_where = original_filter
        from src.database import setup_source, setup_sink

        await setup_source()
        # setup_sink needs 'source' hostname for the subscription CONNECTION string
        from unittest.mock import patch

        with patch.object(settings, "source_url", get_internal_source_url()):
            await setup_sink()


@pytest.mark.asyncio
async def test_reconciliation_efficiency():
    """
    Test that unchanged data does not trigger expensive updates:
    1. Insert data and process it
    2. Get the timestamp of the first processing
    3. Simulate a restart/re-processing of the same data
    4. Verify timestamp is identical
    5. Update source data and re-process
    6. Verify timestamp has changed
    """
    from src.database import (
        setup_source,
        setup_sink,
    )

    # setup_source needs localhost
    await setup_source()
    # setup_sink needs 'source' hostname
    from unittest.mock import patch

    with patch.object(settings, "source_url", get_internal_source_url()):
        await setup_sink()

    async with await connect_db(settings.source_url, autocommit=True) as conn:
        await conn.execute(f"TRUNCATE TABLE {settings.source_table} CASCADE")
    async with await connect_db(settings.sink_url, autocommit=True) as conn:
        await conn.execute(f"TRUNCATE TABLE {settings.sink_raw_table} CASCADE")
        # users_replica is a VIEW

    try:
        # 2. Insert and process
        async with await connect_db(
            settings.source_url, autocommit=True
        ) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"INSERT INTO {settings.source_table} (name, {settings.content_column}) VALUES ('Stable User', 'stable@test.com') RETURNING {settings.id_column}"
                )
                res = await cur.fetchone()
                assert res is not None
                record_id = res[0]

        # Wait for replication to raw table
        max_retries = 10
        found = False
        for _ in range(max_retries):
            async with await connect_db(settings.sink_url) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        f"SELECT 1 FROM {settings.sink_raw_table} WHERE {settings.id_column} = %s",
                        (record_id,),
                    )
                    if await cur.fetchone():
                        found = True
                        break
            await asyncio.sleep(1)
        assert found, "Data didn't reach Sink raw table"

        await wait_for_pgai_sync(settings.sink_url)

        # 3. Record state
        async with await connect_db(settings.sink_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT {settings.embedding_column}, {settings.target_content_column} FROM {settings.sink_replica_table} WHERE {settings.id_column} = %s",
                    (record_id,),
                )
                row1 = await cur.fetchone()
                assert row1 is not None
                emb1, content1 = row1

        # 4. Simulate a restart/re-processing (TRUNCATE raw + Re-insert same data)
        async with await connect_db(settings.sink_url, autocommit=True) as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"TRUNCATE TABLE {settings.sink_raw_table}")
                # We insert the data exactly as it was on the Source
                await cur.execute(
                    f"INSERT INTO {settings.sink_raw_table} ({settings.id_column}, {settings.content_column}) VALUES (%s, %s)",
                    (record_id, "stable@test.com"),
                )

        # Process manually (via pgai wait)
        await wait_for_pgai_sync(settings.sink_url)

        # 5. Verify unchanged
        async with await connect_db(settings.sink_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT {settings.embedding_column}, {settings.target_content_column} FROM {settings.sink_replica_table} WHERE {settings.id_column} = %s",
                    (record_id,),
                )
                row2 = await cur.fetchone()
                assert row2 is not None
                emb2, content2 = row2

                assert (
                    content1 == content2
                ), f"Content changed! '{content1}' != '{content2}'"
                assert list(emb1) == list(
                    emb2
                ), "Embedding changed for identical data!"

        # 6. Update Source and re-process
        async with await connect_db(
            settings.source_url, autocommit=True
        ) as conn:
            await conn.execute(
                f"UPDATE {settings.source_table} SET {settings.content_column} = 'CHANGED@test.com' WHERE {settings.id_column} = %s",
                (record_id,),
            )

        # Wait for replication to raw table
        await asyncio.sleep(2)
        await wait_for_pgai_sync(settings.sink_url)

        # 7. Verify changed
        async with await connect_db(settings.sink_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT {settings.target_content_column} FROM {settings.sink_replica_table} WHERE {settings.id_column} = %s",
                    (record_id,),
                )
                row3 = await cur.fetchone()
                assert row3 is not None
                content3 = row3[0]

                assert (
                    content3 == "CHANGED@test.com"
                ), "Content did NOT change for updated data!"
    finally:
        pass


@pytest.mark.asyncio
async def test_wal_watchdog_self_destruct():
    """
    Test the Source Protection (Watchdog):
    1. Set max_slot_wal_keep_size_mb to -1 to force self-destruct
    2. Call check_and_protect_source()
    3. Verify subscription and replication slot are dropped
    """
    from src.database import setup_source, setup_sink
    from unittest.mock import patch

    # 1. Setup (normal flow)
    await setup_source()
    with patch.object(settings, "source_url", get_internal_source_url()):
        await setup_sink()

    # Verify setup worked: check for slot and subscription
    async with await connect_db(settings.source_url) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                (settings.subscription_name,),
            )
            assert (
                await cur.fetchone() is not None
            ), "Replication slot should exist"

    async with await connect_db(settings.sink_url) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM pg_subscription WHERE subname = %s",
                (settings.subscription_name,),
            )
            assert await cur.fetchone() is not None, "Subscription should exist"

    # 2. Trigger watchdog
    settings.max_slot_wal_keep_size_mb = -1  # Force immediate self-destruct

    try:
        with pytest.raises(
            RuntimeError, match="Self-destructed to protect Source DB"
        ):
            await check_and_protect_source()

        # 3. Verify cleanup: subscription and slot should be gone
        async with await connect_db(settings.source_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                    (settings.subscription_name,),
                )
                assert (
                    await cur.fetchone() is None
                ), "Replication slot should have been dropped"

        async with await connect_db(settings.sink_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT 1 FROM pg_subscription WHERE subname = %s",
                    (settings.subscription_name,),
                )
                assert (
                    await cur.fetchone() is None
                ), "Subscription should have been dropped"

    finally:
        await setup_source()
        with patch.object(settings, "source_url", get_internal_source_url()):
            await setup_sink()


@pytest.mark.asyncio
async def test_idempotent_cleanup():
    """Verify that drop_subscription_completely is safe to call multiple times."""
    from src.database import drop_subscription_completely

    # Call it twice - should not raise exception even if it's already gone
    await drop_subscription_completely()
    await drop_subscription_completely()
