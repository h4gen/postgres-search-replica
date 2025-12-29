import pytest
import asyncio
from src.config import settings
from src.database import setup_source, setup_sink, connect_db
from src.main import process_cycle


@pytest.mark.asyncio
async def test_full_replication_flow():
    """
    Integration test:
    1. Setup Source & Sink
    2. Insert data into Source
    3. Wait for native replication to Sink (users table)
    4. Run process_cycle()
    5. Verify data in users_replica
    """
    # 1. Setup
    await setup_source()
    await setup_sink()

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
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT transformed_email, embedding FROM users_replica WHERE id = (SELECT id FROM users WHERE email = 'TEST@INTEGRATION.COM')"
            )
            row = await cur.fetchone()

            assert row is not None
            assert row[0] == "test@masked-replica.com"
            # Now that we use register_vector, row[1] should be a list/numpy array
            assert len(row[1]) == 3  # Our dummy embedding size
