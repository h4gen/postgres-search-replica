import pytest
import asyncio
import logging
import os
import time
from pg_replica import settings as global_settings
from pg_replica.database import connect_db

logger = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def internal_source_url():
    return global_settings.source_url.replace("localhost:5433", "source:5432") \
                                     .replace("127.0.0.1:5433", "source:5432")

@pytest.fixture
async def sink_conn():
    async with await connect_db(global_settings.resolved_sink_url, autocommit=True) as conn:
        yield conn

@pytest.fixture
async def source_conn():
    async with await connect_db(global_settings.source_url, autocommit=True) as conn:
        yield conn

@pytest.fixture
async def clean_db(sink_conn, source_conn):
    """
    Robustly cleans up the database state for tests.
    """
    logger.info("FIXTURE: clean_db starting...")
    
    # 1. Drop Search Views
    async with sink_conn.cursor() as cur:
        await cur.execute(
            """
            DO $$
            DECLARE r RECORD;
            BEGIN
                FOR r IN (
                    SELECT table_name 
                    FROM information_schema.views 
                    WHERE table_schema = 'public' 
                    AND table_name LIKE '%_search'
                ) LOOP
                    EXECUTE 'DROP VIEW IF EXISTS ' || quote_ident(r.table_name) || ' CASCADE';
                END LOOP;
            END $$;
            """
        )
    
    # 2. Drop Vectorizers (if pgai exists)
    async with sink_conn.cursor() as cur:
        await cur.execute(
            """
            DO $$
            BEGIN
                IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ai' AND table_name = 'vectorizer') THEN
                    DECLARE r RECORD;
                    BEGIN
                        FOR r IN (SELECT id FROM ai.vectorizer) LOOP
                            PERFORM ai.drop_vectorizer(r.id, drop_all => true);
                        END LOOP;
                    END;
                END IF;
            END $$;
            """
        )
        
    # 3. Drop Subscriptions (Nuclear)
    async with sink_conn.cursor() as cur:
        await cur.execute(
            """
            DO $$
            DECLARE r RECORD;
            BEGIN
                FOR r IN (SELECT subname FROM pg_subscription) LOOP
                    EXECUTE 'ALTER SUBSCRIPTION ' || quote_ident(r.subname) || ' DISABLE';
                    EXECUTE 'ALTER SUBSCRIPTION ' || quote_ident(r.subname) || ' SET (slot_name = NONE)';
                    EXECUTE 'DROP SUBSCRIPTION IF EXISTS ' || quote_ident(r.subname) || ' CASCADE';
                END LOOP;
            END $$;
            """
        )

    # 4. Truncate Control Plane
    try:
        await sink_conn.execute("TRUNCATE TABLE _replica_state, _replica_config_history, _sink_outbox, _sink_mirror_registry CASCADE")
    except Exception: pass

    yield

@pytest.fixture
async def robust_slot_cleanup(source_conn):
    async def _cleanup(slot_name):
        logger.info(f"Cleaning up slot {slot_name}...")
        try:
            async with source_conn.cursor() as cur:
                await cur.execute("SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = %s", (slot_name,))
                row = await cur.fetchone()
                if row:
                    active, pid = row
                    if active and pid:
                        await cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
                        await asyncio.sleep(0.5)
                    await cur.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))
        except Exception: pass
            
    return _cleanup

@pytest.fixture
async def robust_subscription_cleanup(sink_conn):
    async def _cleanup(sub_name):
        logger.info(f"Cleaning up subscription {sub_name}...")
        try:
            async with sink_conn.cursor() as cur:
                await cur.execute(f"ALTER SUBSCRIPTION {sub_name} DISABLE")
                await cur.execute(f"ALTER SUBSCRIPTION {sub_name} SET (slot_name = NONE)")
                await cur.execute(f"DROP SUBSCRIPTION IF EXISTS {sub_name} CASCADE")
        except Exception as e:
            logger.debug(f"Subscription cleanup error (expected): {e}")

    return _cleanup

@pytest.fixture
async def wait_for_pgai_sync(sink_conn):
    """
    Waits for pgai vectorizer to finish processing.
    """
    async def _wait(settings, target_name, expected_count=1, timeout=120):
        start_time = time.time()
        config = settings.pipelines[target_name]
        embedding_table = None

        logger.info(f"Waiting for {expected_count} embeddings for target '{target_name}'...")
        while time.time() - start_time < timeout:
            async with sink_conn.cursor() as cur:
                try:
                    await cur.execute(
                        "SELECT table_name FROM information_schema.view_table_usage WHERE view_name = %s AND (table_name LIKE '%%_store_v%%' OR table_name LIKE '%%_embedding%%') LIMIT 1",
                        (f"{target_name}_search",),
                    )
                    row = await cur.fetchone()
                    if row: embedding_table = row[0]
                except Exception: pass

                current_table = embedding_table or f"{config.ingest.table}_store_v{config.get_version_id()}"
                
                # Check actual count
                try:
                    await cur.execute(f"SELECT count(*) FROM {current_table} WHERE {config.pipeline.content_column} IS NOT NULL")
                    count = (await cur.fetchone())[0]
                    if count >= expected_count: return True
                except Exception: pass
            
            await asyncio.sleep(2)
        return False
        
    return _wait
