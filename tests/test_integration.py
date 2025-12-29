import pytest
import asyncio
from pgvector.psycopg import register_vector_async as register_vector  # type: ignore
from src.config import settings
from src.database import connect_db, check_and_protect_source
from src.main import process_cycle


def get_internal_source_url():
    """Helper to translate localhost URL to internal Docker URL for subscription."""
    return settings.source_url.replace("localhost:5433", "source:5432").replace(
        "127.0.0.1:5433", "source:5432"
    )


@pytest.mark.asyncio
async def test_full_replication_flow():
    """
    Integration test:
    1. Wait for native replication to Sink (users table)
    2. Run process_cycle()
    3. Verify data in users_replica
    """
    from src.database import setup_source, setup_sink

    # setup_source runs on host -> needs localhost
    await setup_source()

    # setup_sink creates subscription on container -> needs 'source' hostname
    # We patch settings.source_url only for this call
    from unittest.mock import patch

    with patch.object(settings, "source_url", get_internal_source_url()):
        await setup_sink()

    # Clean up from previous runs
    async with await connect_db(settings.source_url, autocommit=True) as conn:
        await conn.execute("TRUNCATE TABLE users CASCADE")
    async with await connect_db(settings.sink_url, autocommit=True) as conn:
        await conn.execute("TRUNCATE TABLE users CASCADE")
        await conn.execute("TRUNCATE TABLE users_replica CASCADE")

    # 2. Insert test data into Source
    async with await connect_db(settings.source_url, autocommit=True) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO users (name, email) VALUES ('Test User', 'TEST@INTEGRATION.COM')"
            )

    # 3. Wait for native replication (Postgres -> Postgres)
    # We poll the 'users' table in the Sink
    max_retries = 10
    found = False
    async with await connect_db(settings.sink_url) as conn:
        for _ in range(max_retries):
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT 1 FROM users WHERE email = 'TEST@INTEGRATION.COM'"
                )
                if await cur.fetchone():
                    found = True
                    break
            await asyncio.sleep(1)

    assert found, "Native replication failed to move data to Sink 'users' table"

    # 4. Run transformation cycle
    await process_cycle()

    # 5. Verify transformed data in Sink users_replica
    async with await connect_db(settings.sink_url) as conn:
        await register_vector(conn)
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT transformed_email, embedding FROM users_replica WHERE id = (SELECT id FROM users WHERE email = 'TEST@INTEGRATION.COM')"
            )
            row = await cur.fetchone()

            assert row is not None
            assert row[0] == "test@masked-replica.com"
            # Now that we use register_vector, row[1] should be a list/numpy array
            assert len(row[1]) == 3  # Our dummy embedding size


@pytest.mark.asyncio
async def test_filtered_replication_flow():
    """
    Test PG 15 Row Filtering:
    1. Set filter to only replicate users with email containing 'KEEP'
    2. Insert matching and non-matching data
    3. Verify only matching data arrived
    """
    # Override settings for this test
    original_filter = settings.publication_where
    settings.publication_where = "email LIKE '%KEEP%'"

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
            await conn.execute("TRUNCATE TABLE users CASCADE")
        async with await connect_db(settings.sink_url, autocommit=True) as conn:
            await conn.execute("TRUNCATE TABLE users CASCADE")

        # Insert matching and non-matching data
        async with await connect_db(
            settings.source_url, autocommit=True
        ) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO users (name, email) VALUES ('Keep Me', 'KEEP@ME.COM')"
                )
                await cur.execute(
                    "INSERT INTO users (name, email) VALUES ('Ignore Me', 'IGNORE@ME.COM')"
                )

        # Wait and verify
        async with await connect_db(settings.sink_url) as conn:
            # Wait for the matching one
            found_keep = False
            for _ in range(10):
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT 1 FROM users WHERE email = 'KEEP@ME.COM'"
                    )
                    if await cur.fetchone():
                        found_keep = True
                        break
                await asyncio.sleep(1)

            assert found_keep, "Matching data was not replicated"

            # Verify non-matching one is NOT there
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT 1 FROM users WHERE email = 'IGNORE@ME.COM'"
                )
                assert (
                    await cur.fetchone() is None
                ), "Non-matching data was replicated"

    finally:
        # Restore original filter and reset DB
        settings.publication_where = original_filter
        from src.database import setup_source, setup_sink

        await setup_source()
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
        await conn.execute("TRUNCATE TABLE users CASCADE")
    async with await connect_db(settings.sink_url, autocommit=True) as conn:
        await conn.execute("TRUNCATE TABLE users_replica CASCADE")
        # Disable trigger again to be absolutely sure the daemon doesn't touch it
        await conn.execute("ALTER TABLE users DISABLE TRIGGER trg_new_user_raw")

    try:
        # 2. Insert and process
        async with await connect_db(
            settings.source_url, autocommit=True
        ) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO users (name, email) VALUES ('Stable User', 'stable@test.com') RETURNING id"
                )
                res = await cur.fetchone()
                assert res is not None
                user_id = res[0]

        # Wait for replication to raw table
        max_retries = 10
        found = False
        for _ in range(max_retries):
            async with await connect_db(settings.sink_url) as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT 1 FROM users WHERE id = %s", (user_id,)
                    )
                    if await cur.fetchone():
                        found = True
                        break
            await asyncio.sleep(1)
        assert found, "Data didn't reach Sink raw table"

        # Make sure processed is FALSE before we run cycle
        async with await connect_db(settings.sink_url, autocommit=True) as conn:
            await conn.execute(
                "UPDATE users SET processed = FALSE WHERE id = %s", (user_id,)
            )

        await process_cycle()

        # 3. Record state
        async with await connect_db(settings.sink_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT updated_at, embedding, transformed_email FROM users_replica WHERE id = %s",
                    (user_id,),
                )
                row1 = await cur.fetchone()
                assert row1 is not None
                ts1, emb1, email1 = row1

        # 4. Simulate a restart/re-processing (TRUNCATE raw + Re-insert same data)
        async with await connect_db(settings.sink_url, autocommit=True) as conn:
            async with conn.cursor() as cur:
                await cur.execute("TRUNCATE TABLE users")
                # We insert the data exactly as it was on the Source
                await cur.execute(
                    "INSERT INTO users (id, email, processed) VALUES (%s, %s, FALSE)",
                    (user_id, "stable@test.com"),
                )

        # Process manually
        await process_cycle()

        # 5. Verify unchanged
        async with await connect_db(settings.sink_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT updated_at, embedding, transformed_email FROM users_replica WHERE id = %s",
                    (user_id,),
                )
                row2 = await cur.fetchone()
                assert row2 is not None
                ts2, emb2, email2 = row2

                assert (
                    email1 == email2
                ), f"Email changed! '{email1}' != '{email2}'"
                assert ts1 == ts2, f"Timestamp changed! {ts1} != {ts2}"
                assert list(emb1) == list(
                    emb2
                ), "Embedding changed for identical data!"

        # 6. Update Source and re-process
        async with await connect_db(
            settings.source_url, autocommit=True
        ) as conn:
            await conn.execute(
                "UPDATE users SET email = 'CHANGED@test.com' WHERE id = %s",
                (user_id,),
            )

        # Wait for replication to raw table
        await asyncio.sleep(2)
        # Force processed=FALSE because trigger is disabled
        async with await connect_db(settings.sink_url, autocommit=True) as conn:
            await conn.execute(
                "UPDATE users SET processed = FALSE WHERE id = %s", (user_id,)
            )

        await process_cycle()

        # 7. Verify changed
        async with await connect_db(settings.sink_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT updated_at FROM users_replica WHERE id = %s",
                    (user_id,),
                )
                row3 = await cur.fetchone()
                assert row3 is not None
                ts3 = row3[0]

                assert ts3 > ts1, "Timestamp did NOT change for updated data!"
    finally:
        # Re-enable trigger
        async with await connect_db(settings.sink_url, autocommit=True) as conn:
            await conn.execute(
                "ALTER TABLE users ENABLE ALWAYS TRIGGER trg_new_user_raw"
            )


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
    original_max_size = settings.max_slot_wal_keep_size_mb
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
        # Restore settings and ensure cleanup
        settings.max_slot_wal_keep_size_mb = original_max_size
        # Re-run setup to leave DB in a clean state for other tests
        await setup_source()
        with patch.object(settings, "source_url", get_internal_source_url()):
            await setup_sink()
