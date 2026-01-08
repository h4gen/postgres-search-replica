import logging
import asyncio
import json
from typing import Dict, List, Optional, Any
from contextlib import asynccontextmanager
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pgvector.psycopg import register_vector_async as register_vector  # type: ignore
from .config import Settings, SearchPipeline, IngestConfig
from .utils import wait_until

logger = logging.getLogger(__name__)

# Global pools
_source_pool: AsyncConnectionPool | None = None
_sink_pool: AsyncConnectionPool | None = None

# Control Plane Constants
RECONCILER_ADVISORY_LOCK_ID = 133742  # Unique ID for the reconciler lock


async def init_pools(settings: Settings):
    """Initialize connection pools for source and sink."""
    global _source_pool, _sink_pool
    
    # Check if existing pools are closed and need recreation
    if _source_pool and _source_pool.closed:
        _source_pool = None
    if _sink_pool and _sink_pool.closed:
        _sink_pool = None

    if not _source_pool:
        logger.info(f"Initializing source connection pool with {settings.source_url}...")
        _source_pool = AsyncConnectionPool(
            conninfo=settings.source_url,
            min_size=1,
            max_size=5,
            open=False,
        )
        await _source_pool.open()

    if not _sink_pool:
        logger.info("Initializing sink connection pool...")
        _sink_pool = AsyncConnectionPool(
            conninfo=settings.resolved_sink_url,
            min_size=1,
            max_size=10,
            open=False,
        )
        await _sink_pool.open()


async def close_pools():
    """Close connection pools."""
    global _source_pool, _sink_pool
    if _source_pool:
        logger.info("Closing source connection pool...")
        await _source_pool.close()
        _source_pool = None
    if _sink_pool:
        logger.info("Closing sink connection pool...")
        await _sink_pool.close()
        _sink_pool = None


async def get_source_conn():
    """Get a connection from the source pool."""
    if not _source_pool:
        raise RuntimeError("Source pool not initialized")
    return _source_pool.connection()


async def get_sink_conn():
    """Get a connection from the sink pool."""
    if not _sink_pool:
        raise RuntimeError("Sink pool not initialized")
    return _sink_pool.connection()


async def connect_db(url: str, **kwargs):
    """
    Connect to database (one-off connection).
    Use pools (get_source_conn/get_sink_conn) for long-running processes.
    """
    conn = await psycopg.AsyncConnection.connect(url, **kwargs)
    return conn


async def wait_for_source_table(settings: Settings, config: SearchPipeline, timeout: int = 30):
    """Wait for a table to exist on the Source DB."""
    async def table_exists():
        async with await get_source_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
                    (config.ingest.table,),
                )
                if await cur.fetchone():
                    return True
                
                await cur.execute(
                    "SELECT 1 FROM pg_class WHERE relname = %s",
                    (config.ingest.table,),
                )
                return await cur.fetchone() is not None

    logger.info(f"Waiting for source table {config.ingest.table} to be visible...")
    try:
        await wait_until(table_exists, timeout=timeout, interval=0.1)
        return True
    except asyncio.TimeoutError:
        logger.error(f"Timed out waiting for source table {config.ingest.table}")
        return False


async def is_extension_loaded(ext_name: str) -> bool:
    """Check if a Postgres extension is loaded on the Sink DB."""
    async with await get_sink_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 FROM pg_extension WHERE extname = %s", (ext_name,))
            return await cur.fetchone() is not None


async def is_replication_slot_active(slot_name: str, on_source: bool = True) -> bool:
    """Check if a replication slot is active."""
    get_conn = get_source_conn if on_source else get_sink_conn
    async with await get_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT active FROM pg_replication_slots WHERE slot_name = %s", (slot_name,))
            row = await cur.fetchone()
            return row[0] if row else False


async def is_publication_valid(pub_name: str) -> bool:
    """Check if a publication exists on the Source DB."""
    async with await get_source_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (pub_name,))
            return await cur.fetchone() is not None


async def get_source_column_types(
    settings: Settings, config: SearchPipeline
) -> Dict[str, str]:
    """Query the Source DB's information_schema to get column types."""
    logger.info(
        f"Detecting column types for {config.ingest.table} on source..."
    )
    
    # Pre-flight readiness check to avoid race conditions in tests
    if not await wait_for_source_table(settings, config):
        raise RuntimeError(f"Source table {config.ingest.table} not found after timeout")

    async with await get_source_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT column_name, data_type, udt_name
                FROM information_schema.columns 
                WHERE table_name = %s 
                AND column_name = ANY(%s)
                """,
                (config.ingest.table, config.ingest.columns),
            )
            rows = await cur.fetchall()
            # Map data_type to something we can use in CREATE TABLE
            types = {}
            for name, dtype, udt in rows:
                if udt == "uuid":
                    types[name] = "UUID"
                elif dtype == "integer":
                    types[name] = "INT"
                elif dtype == "bigint":
                    types[name] = "BIGINT"
                elif "character" in dtype or dtype == "text":
                    types[name] = "TEXT"
                else:
                    types[name] = dtype
            return types


async def setup_source(settings: Settings, config: SearchPipeline, target_name: str):
    """Remotely initialize the source publication."""
    pub_name = f"pub_{target_name}"
    logger.info(f"Setting up remote source publication {pub_name}...")
    
    # Pre-flight readiness check to avoid race conditions in tests
    if not await wait_for_source_table(settings, config):
        raise RuntimeError(f"Source table {config.ingest.table} not found after timeout")

    async with await get_source_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            cols = ", ".join(config.ingest.columns)
            where_clause = (
                f" WHERE ({config.ingest.filter})"
                if config.ingest.filter
                else ""
            )

            await cur.execute(
                f"SELECT 1 FROM pg_publication WHERE pubname = '{pub_name}'"
            )
            if not await cur.fetchone():
                logger.info(
                    f"Creating publication {pub_name} on Source for columns ({cols}){where_clause}..."
                )
                await cur.execute(
                    f"CREATE PUBLICATION {pub_name} FOR TABLE {config.ingest.table} ({cols}){where_clause}"
                )
            else:
                await cur.execute(
                    f"ALTER PUBLICATION {pub_name} SET TABLE {config.ingest.table} ({cols}){where_clause}"
                )
            await conn.commit()


async def setup_state_table(settings: Settings, target_name: str):
    """Create the _replica_state table in the Sink DB and init key."""
    sub_name = f"sub_{target_name}"
    logger.info(f"Setting up replica state table index for {sub_name} in Sink...")
    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS _replica_state (
                    key TEXT PRIMARY KEY,
                    last_id TEXT,
                    last_lsn TEXT,
                    config_hash TEXT,
                    updated_at TIMESTAMP DEFAULT NOW()
                )
                """
            )
            # Initialize if not exists
            await cur.execute(
                "INSERT INTO _replica_state (key, last_id) VALUES (%s, '0') ON CONFLICT DO NOTHING",
                (sub_name,),
            )


async def ensure_config_history_table(settings: Settings):
    """Create the _replica_config_history table in the Sink DB."""
    logger.info("Ensuring Control Plane config history table exists in Sink...")
    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS _replica_config_history (
                    id SERIAL PRIMARY KEY,
                    target_name TEXT NOT NULL,
                    config_json JSONB NOT NULL,
                    config_hash TEXT NOT NULL,
                    generation INT NOT NULL DEFAULT 1,
                    status TEXT NOT NULL DEFAULT 'Pending',
                    error_message TEXT,
                    observed_generation INT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT NOW()
                )
                """
            )
            await cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_config_history_target ON _replica_config_history(target_name)"
            )


async def save_table_config(settings: Settings, target_name: str, config: SearchPipeline) -> int:
    """Save a new configuration for a table and return the new generation."""
    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            # 1. Get latest generation
            await cur.execute(
                "SELECT COALESCE(MAX(generation), 0) FROM _replica_config_history WHERE target_name = %s",
                (target_name,)
            )
            curr_gen = (await cur.fetchone())[0]
            new_gen = curr_gen + 1

            # 2. Insert new config
            await cur.execute(
                """
                INSERT INTO _replica_config_history (target_name, config_json, config_hash, generation, status)
                VALUES (%s, %s, %s, %s, 'Pending')
                """,
                (
                    target_name, 
                    psycopg.types.json.Json(config.model_dump()), 
                    config.get_version_id(), 
                    new_gen
                )
            )
            return new_gen


async def get_latest_table_config(settings: Settings, target_name: str) -> dict | None:
    """Retrieve the latest configuration for a table."""
    async with await get_sink_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                """
                SELECT config_json, config_hash, generation, status, error_message, observed_generation 
                FROM _replica_config_history 
                WHERE target_name = %s 
                ORDER BY generation DESC LIMIT 1
                """,
                (target_name,)
            )
            return await cur.fetchone()


async def update_config_status(
    settings: Settings, 
    target_name: str, 
    generation: int, 
    status: str, 
    error_message: str | None = None,
    observed_generation: int | None = None
):
    """Update the status of a specific configuration generation."""
    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            updates = ["status = %s", "error_message = %s"]
            params = [status, error_message]
            
            if observed_generation is not None:
                updates.append("observed_generation = %s")
                params.append(observed_generation)
            
            params.append(target_name)
            params.append(generation)
            
            sql = f"UPDATE _replica_config_history SET {', '.join(updates)} WHERE target_name = %s AND generation = %s"
            await cur.execute(sql, params)


@asynccontextmanager
async def reconciliation_lock():
    """Context manager for distributed locking using Postgres advisory locks."""
    async with await get_sink_conn() as conn:
        # Advisory locks are session-level or transaction-level. 
        # We use transaction-level for safety (automatically released on commit/rollback).
        await conn.set_autocommit(False)
        try:
            async with conn.cursor() as cur:
                logger.debug(f"Attempting to acquire reconciler lock ({RECONCILER_ADVISORY_LOCK_ID})...")
                # pg_try_advisory_xact_lock returns True/False immediately
                await cur.execute("SELECT pg_try_advisory_xact_lock(%s)", (RECONCILER_ADVISORY_LOCK_ID,))
                locked = (await cur.fetchone())[0]
                
                if not locked:
                    raise RuntimeError("Could not acquire reconciliation lock. Another instance matches.")
                
                logger.debug("Reconciliation lock acquired.")
                yield
                await conn.commit()
        except Exception as e:
            await conn.rollback()
            raise e
        finally:
            logger.debug("Reconciliation lock released.")


async def get_replica_state(
    settings: Settings, target_name: str
) -> tuple[str | None, str | None]:
    """Get (last_id, last_lsn) from the state table for a specific target."""
    sub_name = f"sub_{target_name}"
    async with await get_sink_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT last_id, last_lsn FROM _replica_state WHERE key = %s",
                (sub_name,),
            )
            row = await cur.fetchone()
            if row:
                return str(row[0]) if row[0] is not None else None, (
                    str(row[1]) if row[1] is not None else None
                )
            return None, None


    return statuses


async def get_source_health(settings: Settings) -> dict:
    """Query current health state of the Source DB replication."""
    logger.debug("Fetching source health metadata...")
    try:
        async with await get_source_conn() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # 1. Check replication slots
                await cur.execute(
                    "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots"
                )
                slots = await cur.fetchall()

                # 2. Get current WAL LSN
                await cur.execute("SELECT pg_current_wal_lsn() as current_lsn")
                row = await cur.fetchone()
                current_lsn = str(row["current_lsn"]) if row and row["current_lsn"] else None

                return {
                    "slots": slots,
                    "current_lsn": current_lsn,
                    "is_connected": True
                }
    except Exception as e:
        logger.error(f"Failed to fetch source health: {e}")
        return {"is_connected": False, "error": str(e)}


async def get_vectorizer_statuses(settings: Settings) -> dict[str, int]:
    """
    Get synchronization status for all vectorizers.
    Returns: Dict[vectorizer_name, pending_items_count]
    """
    statuses = {}
    async with await get_sink_conn() as conn:
        async with conn.cursor() as cur:
            # 1. Try generic ai.vectorizer_status (pgai 0.4.0+)
            try:
                await cur.execute(
                    "SELECT source_table, pending_items FROM ai.vectorizer_status"
                )
                rows = await cur.fetchall()
                for table, pending in rows:
                    statuses[table] = pending
            except Exception:
                # Fallback implementation if specific view unavailable
                pass
    return statuses


async def get_pipeline_summary(settings: Settings) -> dict:
    """Aggregate health signals from source, vectorizers, and mirrors."""
    summary = {
        "source": await get_source_health(settings),
        "vectorizers": [],
        "mirrors": []
    }

    async with await get_sink_conn() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            # 1. Get pgai vectorizer statuses
            try:
                await cur.execute(
                    """
                    SELECT 
                        v.name, 
                        v.source_table, 
                        s.pending_items,
                        v.config->'destination'->>'target_table' as target_table,
                        v.config->'destination'->>'view_name' as view_name
                    FROM ai.vectorizer v
                    LEFT JOIN ai.vectorizer_status s ON v.id = s.id
                    """
                )
                summary["vectorizers"] = await cur.fetchall()
            except Exception:
                # Fallback if ai.vectorizer_status is not exactly as expected
                pass

            # 2. Get mirror statuses from our registry
            await cur.execute(
                """
                SELECT 
                    mirror_id, 
                    target_name, 
                    last_processed_id, 
                    promoted_version_id, 
                    error_count, 
                    last_error, 
                    last_sync_latency_ms,
                    updated_at
                FROM _sink_mirror_registry
                """
            )
            summary["mirrors"] = await cur.fetchall()

            # 3. Calculate Outbox Lag
            await cur.execute("SELECT MAX(id) as max_id FROM _sink_outbox")
            row = await cur.fetchone()
            max_outbox_id = row["max_id"] if row and row["max_id"] else 0
            
    return summary


def estimate_hnsw_ram(dimension: int, row_count: int, M: int = 16) -> int:
    """
    Estimate RAM usage for pgvector HNSW index.
    Formula: (dim * 4 + M * 8) * rows * 1.5 (overhead factor)
    """
    bytes_per_row = (dimension * 4) + (M * 8)
    total_bytes = int(bytes_per_row * row_count * 1.5)
    return total_bytes


async def get_resource_projections(settings: Settings, config: SearchPipeline) -> dict:
    """Estimate costs and hardware needs for a build."""
    logger.debug(f"Calculating projections for {config.ingest.table}...")
    
    async with await get_source_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"SELECT count(*) FROM {config.ingest.table}")
            row = await cur.fetchone()
            row_count = row[0] if row else 0

    # Simplified cost model (e.g., $0.10 per 1M tokens)
    # Average 100 tokens per chunk for estimation
    estimated_tokens = row_count * 100 
    estimated_cost_usd = (estimated_tokens / 1_000_000) * 0.10
    
    ram_bytes = estimate_hnsw_ram(config.pipeline.embedding.dimension, row_count)

    return {
        "row_count": row_count,
        "estimated_tokens": estimated_tokens,
        "estimated_cost_usd": round(estimated_cost_usd, 4),
        "estimated_ram_mb": round(ram_bytes / (1024 * 1024), 2),
        "embedding_model": config.pipeline.embedding.model,
        "dimension": config.pipeline.embedding.dimension
    }


async def log_experiment_start(settings: Settings, target_name: str, version_id: str):
    """Log the beginning of a new search experiment (shadow build)."""
    async with await get_sink_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO _search_experiment_logs (target_name, version_id, started_at, metadata)
                VALUES (%s, %s, NOW(), %s)
                """,
                (target_name, version_id, psycopg.types.json.Json({"status": "starting"}))
            )
            await conn.commit()


async def log_experiment_finish(settings: Settings, target_name: str, version_id: str, success: bool = True):
    """Finalize experiment log with results."""
    async with await get_sink_conn() as conn:
        async with conn.cursor() as cur:
            # Get final stats from vectorizer status if available
            await cur.execute(
                "SELECT pending_items FROM ai.vectorizer_status WHERE source_table ILIKE %s",
                (f"%{target_name}%",)
            )
            row = await cur.fetchone()
            # If successfully promoted, we assume it's caught up
            await cur.execute(
                """
                UPDATE _search_experiment_logs 
                SET finished_at = NOW(), 
                    success_rate = %s,
                    metadata = metadata || %s::jsonb
                WHERE target_name = %s AND version_id = %s AND finished_at IS NULL
                """,
                (1.0 if success else 0.0, psycopg.types.json.Json({"status": "promoted"}), target_name, version_id)
            )
            await conn.commit()


async def audit_pipeline_failures(settings: Settings):
    """Sync errors from pgai's internal logs into our tracking table."""
    async with await get_sink_conn() as conn:
        async with conn.cursor() as cur:
            # Check if pgai error view exists (pgai 0.6.0+)
            try:
                # This is a guestimate of where pgai stores errors
                # In latest pgai, it's often in ai.vectorizer_errors
                await cur.execute(
                    """
                    INSERT INTO _pipeline_failures (target_name, version_id, source_id, error_message, context)
                    SELECT 
                        v.name, 
                        (v.config->'destination'->>'target_table'), 
                        e.id::text, 
                        e.message, 
                        e.details
                    FROM ai.vectorizer_errors e
                    JOIN ai.vectorizer v ON e.vectorizer_id = v.id
                    ON CONFLICT DO NOTHING
                    """
                )
                await conn.commit()
            except Exception:
                # Silently skip if pgai error table is missing or schema differs
                pass


async def update_replica_state(
    settings: Settings, target_name: str, last_id: str | None = None, lsn: str | None = None
):
    """Update high-water mark or LSN in the state table for a specific target."""
    sub_name = f"sub_{target_name}"
    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            if last_id is not None and lsn is not None:
                await cur.execute(
                    "UPDATE _replica_state SET last_id = %s, last_lsn = %s, updated_at = NOW() WHERE key = %s",
                    (str(last_id), str(lsn), sub_name),
                )
            elif last_id is not None:
                await cur.execute(
                    "UPDATE _replica_state SET last_id = %s, updated_at = NOW() WHERE key = %s",
                    (str(last_id), sub_name),
                )
            elif lsn is not None:
                await cur.execute(
                    "UPDATE _replica_state SET last_lsn = %s, updated_at = NOW() WHERE key = %s",
                    (str(lsn), sub_name),
                )


async def ensure_embedding_cache_table(settings: Settings, config: SearchPipeline):
    """Create the Postgres-native embedding cache table."""
    logger.info("Ensuring embedding cache table exists...")
    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            await cur.execute("CREATE EXTENSION IF NOT EXISTS vector")

            # Protection: If dimension changed, existing cache is invalid/incompatible
            await cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name = '_embedding_cache'"
            )
            if await cur.fetchone():
                await cur.execute(
                    """
                    SELECT atttypmod FROM pg_attribute 
                    WHERE attrelid = '_embedding_cache'::regclass 
                    AND attname = 'embedding'
                    """
                )
                row = await cur.fetchone()
                if row:
                    current_dim = row[0]
                    if (
                        current_dim != config.pipeline.embedding.dimension
                        and current_dim != -1
                    ):
                        logger.warning(
                            f"Cache dimension mismatch ({current_dim} vs {config.pipeline.embedding.dimension}). Purging cache."
                        )
                        await cur.execute("DROP TABLE _embedding_cache CASCADE")

            await cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS _embedding_cache (
                    text_hash TEXT PRIMARY KEY,
                    embedding vector({config.pipeline.embedding.dimension}),
                    model_name TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
                """
            )

async def cleanup_vectorizer_infrastructure(
    settings: Settings, config: SearchPipeline, vectorizer_name: str
):
    """Robustly clean up all infrastructure for a specific vectorizer."""
    logger.info(f"Robust cleanup for vectorizer {vectorizer_name}...")
    
    embedding_view = vectorizer_name.replace("_store", "_embedding")
    
    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            cleanup_sql = f"""
            DO $$
            DECLARE live_target TEXT;
            BEGIN
                -- 1. Check if ANY view is using this as its target (safety)
                SELECT table_name INTO live_target 
                FROM information_schema.view_table_usage 
                WHERE view_name = '{config.ingest.table}_search' 
                AND table_name IN ('{vectorizer_name}', '{embedding_view}') 
                LIMIT 1;
                
                -- 2. If it's live, we MUST drop the replica view first
                IF live_target IS NOT NULL THEN
                    EXECUTE 'DROP VIEW IF EXISTS ' || quote_ident('{config.ingest.table}_search') || ' CASCADE';
                END IF;

                -- 3. Drop the pgai vectorizer if it exists
                IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'ai' AND table_name = 'vectorizer') THEN
                    IF EXISTS (SELECT 1 FROM ai.vectorizer WHERE name = '{vectorizer_name}') THEN
                        PERFORM ai.drop_vectorizer('{vectorizer_name}', drop_all => true);
                    END IF;
                END IF;

                -- 4. Final safety drops for orphans
                BEGIN
                    EXECUTE 'DROP VIEW IF EXISTS ' || quote_ident('{vectorizer_name}') || ' CASCADE';
                EXCEPTION WHEN OTHERS THEN NULL;
                END;

                BEGIN
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident('{vectorizer_name}') || ' CASCADE';
                EXCEPTION WHEN OTHERS THEN NULL;
                END;

                BEGIN
                    EXECUTE 'DROP VIEW IF EXISTS ' || quote_ident('{embedding_view}') || ' CASCADE';
                EXCEPTION WHEN OTHERS THEN NULL;
                END;

                BEGIN
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident('{embedding_view}') || ' CASCADE';
                EXCEPTION WHEN OTHERS THEN NULL;
                END;

                -- 5. Search & Destroy Zombie Triggers on the Raw Table
                -- Find ANY trigger on the source raw table whose function source mentions the vectorizer target
                FOR live_target IN 
                    SELECT tgname 
                    FROM pg_trigger tg
                    JOIN pg_class c ON tg.tgrelid = c.oid
                    JOIN pg_proc p ON tg.tgfoid = p.oid
                    JOIN pg_namespace n ON c.relnamespace = n.oid
                    WHERE n.nspname = 'public' 
                      AND c.relname = '{config.ingest.table}'
                      AND p.prosrc ILIKE '%{vectorizer_name}%'
                LOOP
                    EXECUTE 'DROP TRIGGER IF EXISTS ' || quote_ident(live_target) || ' ON ' || quote_ident('{config.ingest.table}') || ' CASCADE';
                END LOOP;
            END $$;
            """
            await cur.execute(cleanup_sql)

async def atomic_view_swap(
    settings: Settings,
    config: SearchPipeline,
    target_name: str,
    config_hash: str,
    target_table: Optional[str] = None,
    vectorizer_target: Optional[str] = None,
):
    """
    Update the search view to point to the latest table version and
    record the new config hash atomically. Supports Hybrid RRF.
    """
    raw_table = target_table or config.ingest.table
    sub_name = f"sub_{target_name}"
    
    # Resolve embedding view name
    embedding_view = f"{raw_table}_embedding"
    async with await get_sink_conn() as conn:
        if vectorizer_target:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT config->'destination'->>'view_name' FROM ai.vectorizer WHERE name = %s",
                    (vectorizer_target,),
                )
                row = await cur.fetchone()
                if row and row[0]:
                    embedding_view = row[0]
                else:
                    embedding_view = vectorizer_target.replace("_store", "_embedding")

        logger.info(
            f"Performing atomic view swap targeting {raw_table} (Profile: {config.storage.postgres.profile})..."
        )

        await conn.set_autocommit(False)
        try:
            async with conn.cursor() as cur:
                await cur.execute(f"DROP VIEW IF EXISTS {config.ingest.table}_search")
                
                extra_cols = ",\n                            ".join([f"r.{c}" for c in config.ingest.columns if c != config.ingest.p_key])
                if extra_cols:
                    extra_cols = ",\n                            " + extra_cols

                if config.storage.postgres.profile == "hybrid":
                    # HYBRID SEARCH (RRF): Vector + Full-Text
                    # We create a view that exposes both, allowing the SEARCH query 
                    # to perform Rank Fusion logic.
                    logger.info("Implementing Hybrid View with RRF scoring support...")
                    await cur.execute(
                        f"""
                        CREATE VIEW {config.ingest.table}_search AS
                        SELECT 
                            r.{config.ingest.p_key},
                            e.chunk as chunk,
                            e.embedding,
                            to_tsvector('english', e.chunk) as ts_col{extra_cols}
                        FROM {raw_table} r
                        LEFT JOIN {embedding_view} e ON r.{config.ingest.p_key} = e.{config.ingest.p_key}
                    """
                    )
                else:
                    # VECTOR SEARCH ONLY
                    await cur.execute(
                        f"""
                        CREATE VIEW {config.ingest.table}_search AS
                        SELECT 
                            r.{config.ingest.p_key},
                            e.chunk as chunk,
                            e.embedding{extra_cols}
                        FROM {raw_table} r
                        LEFT JOIN {embedding_view} e ON r.{config.ingest.p_key} = e.{config.ingest.p_key}
                    """
                    )

                # Update State Hash
                await cur.execute(
                    "UPDATE _replica_state SET config_hash = %s, updated_at = NOW() WHERE key = %s",
                    (config_hash, sub_name),
                )
            await conn.commit()
        except Exception as e:
            await conn.rollback()
            raise e

async def ensure_outbox_infrastructure(settings: Settings):
    """Create the _sink_outbox and _sink_mirror_registry tables."""
    logger.info("Ensuring Universal Outbox infrastructure exists in Sink...")
    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            # 1. The Outbox: Transactional log of vectorized changes
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS _sink_outbox (
                    id BIGSERIAL PRIMARY KEY,
                    target_name TEXT NOT NULL,
                    version_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    action TEXT NOT NULL, -- UPSERT, DELETE
                    payload JSONB, -- {content: "...", embedding: [...]}
                    created_at TIMESTAMP DEFAULT NOW()
                )
                """
            )
            # Index for the MirrorWorker to poll efficiently
            await cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_sink_outbox_id ON _sink_outbox(id)"
            )

            # 2. Mirror Registry: Track progress of external sinks
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS _sink_mirror_registry (
                    mirror_id TEXT NOT NULL,
                    target_name TEXT NOT NULL,
                    last_processed_id BIGINT DEFAULT 0,
                    promoted_version_id TEXT,
                    error_count INT DEFAULT 0,
                    last_error TEXT,
                    last_sync_latency_ms INT,
                    updated_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (mirror_id, target_name)
                )
                """
            )
            # Migration: Ensure operational columns exist
            await cur.execute("ALTER TABLE _sink_mirror_registry ADD COLUMN IF NOT EXISTS promoted_version_id TEXT")
            await cur.execute("ALTER TABLE _sink_mirror_registry ADD COLUMN IF NOT EXISTS error_count INT DEFAULT 0")
            await cur.execute("ALTER TABLE _sink_mirror_registry ADD COLUMN IF NOT EXISTS last_error TEXT")
            await cur.execute("ALTER TABLE _sink_mirror_registry ADD COLUMN IF NOT EXISTS last_sync_latency_ms INT")

            # 3. Experiment Logs: Track build history
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS _search_experiment_logs (
                    id SERIAL PRIMARY KEY,
                    target_name TEXT NOT NULL,
                    version_id TEXT NOT NULL,
                    started_at TIMESTAMP DEFAULT NOW(),
                    finished_at TIMESTAMP,
                    total_rows INT DEFAULT 0,
                    success_rate FLOAT,
                    tokens_used BIGINT DEFAULT 0,
                    metadata JSONB
                )
                """
            )

            # 4. Pipeline Failures: Trace poison pills
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS _pipeline_failures (
                    id SERIAL PRIMARY KEY,
                    target_name TEXT NOT NULL,
                    version_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    error_message TEXT,
                    failed_at TIMESTAMP DEFAULT NOW(),
                    context JSONB,
                    resolved BOOLEAN DEFAULT FALSE
                )
                """
            )


async def setup_outbox_trigger(
    settings: Settings, target_name: str, vectorizer_name: str, config: SearchPipeline
):
    """
    Attach a trigger to the internal pgai store table to capture 
    all vectorization events into the _sink_outbox.
    """
    version_id = config.get_version_id()
    # The pgai store table is the vectorizer_name itself (v_store_v1)
    # We want to capture when vectors ARE CREATED.
    
    logger.info(f"Setting up outbox trigger for {vectorizer_name}...")
    
    trigger_fn_name = f"fn_capture_outbox_{target_name}_{version_id}"
    trigger_name = f"trg_outbox_{target_name}_{version_id}"

    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            # 1. Create the Trigger Function
            # Note: pgai keeps source PK in its store table.
            # We assume config.ingest.p_key is present in the pgai store.
            await cur.execute(
                f"""
                CREATE OR REPLACE FUNCTION {trigger_fn_name}() RETURNS TRIGGER AS $$
                BEGIN
                    IF (TG_OP = 'DELETE') THEN
                        INSERT INTO _sink_outbox (target_name, version_id, source_id, action)
                        VALUES ('{target_name}', '{version_id}', OLD.{config.ingest.p_key}::text, 'DELETE');
                    ELSE
                        INSERT INTO _sink_outbox (target_name, version_id, source_id, action, payload)
                        VALUES (
                            '{target_name}', 
                            '{version_id}', 
                            NEW.{config.ingest.p_key}::text, 
                            'UPSERT', 
                            jsonb_build_object(
                                'content', NEW.chunk,
                                'embedding', NEW.embedding::text -- cast to text for JSON
                            )
                        );
                    END IF;
                    RETURN NULL;
                END;
                $$ LANGUAGE plpgsql;
                """
            )

            # 2. Attach Trigger to pgai STORE table
            # We use AFTER INSERT OR UPDATE OR DELETE
            await cur.execute(
                f"""
                DROP TRIGGER IF EXISTS {trigger_name} ON {vectorizer_name};
                CREATE TRIGGER {trigger_name}
                AFTER INSERT OR UPDATE OR DELETE ON {vectorizer_name}
                FOR EACH ROW EXECUTE FUNCTION {trigger_fn_name}();
                """
            )

            # 3. Backfill existing rows (Handling the Race Condition)
            # Since the vectorizer might have already processed rows before we attached the trigger,
            # we must check for any existing rows and insert them into the outbox if missing.
            await cur.execute(
                f"""
                INSERT INTO _sink_outbox (target_name, version_id, source_id, action, payload)
                SELECT 
                    '{target_name}', 
                    '{version_id}', 
                    {config.ingest.p_key}::text, 
                    'UPSERT', 
                    jsonb_build_object(
                        'content', chunk,
                        'embedding', embedding::text
                    )
                FROM {vectorizer_name}
                ON CONFLICT DO NOTHING
                """
            )




async def warm_up_from_cache(
    settings: Settings, config: SearchPipeline, source_table: str, target_store_table: str
):
    """Populate a new embedding table from the cache to avoid re-calls."""
    logger.info(f"Warming up {target_store_table} from cache...")

    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
                (target_store_table,),
            )
            if not await cur.fetchone():
                logger.info(f"Table {target_store_table} not found yet, skipping warm-up.")
                return

            await cur.execute(
                f"""
                INSERT INTO {target_store_table} ({config.ingest.p_key}, embedding)
                SELECT r.{config.ingest.p_key}, c.embedding
                FROM {source_table} r
                JOIN _embedding_cache c ON md5(COALESCE(r.{config.pipeline.content_column}, '')::text || %s) = c.text_hash
                ON CONFLICT DO NOTHING
                """,
                (config.pipeline.embedding.model,),
            )


async def check_slot_exists(settings: Settings, target_name: str) -> bool:
    """Check if the replication slot exists on the Source DB."""
    sub_name = f"sub_{target_name}"
    async with await get_source_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                (sub_name,),
            )
            return await cur.fetchone() is not None


async def create_placeholder_slot(settings: Settings, target_name: str) -> str:
    """Create a logical replication slot on Source and return its consistent LSN."""
    sub_name = f"sub_{target_name}"
    logger.info(f"Creating placeholder replication slot {sub_name} on Source...")
    async with await get_source_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = %s",
                (sub_name,),
            )
            res = await cur.fetchone()
            if res:
                lsn = str(res[0])
                logger.info(f"Slot already exists at LSN: {lsn}")
                return lsn

            await cur.execute(
                "SELECT lsn FROM pg_create_logical_replication_slot(%s, 'pgoutput', false, true)",
                (sub_name,),
            )
            res = await cur.fetchone()
            if not res:
                raise RuntimeError("Failed to create replication slot")
            lsn = str(res[0])
            logger.info(f"Created slot at LSN: {lsn}")
            return lsn


async def ensure_sink_raw_table(settings: Settings, config: SearchPipeline):
    """Ensure the raw table exists in the Sink DB with correct types."""
    target = config.ingest.table
    source_types = await get_source_column_types(settings, config)
    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            cols_sql = []
            for col in config.ingest.columns:
                dtype = source_types.get(col, "TEXT")
                if col == config.ingest.p_key:
                    cols_sql.append(f"{col} {dtype} PRIMARY KEY")
                else:
                    cols_sql.append(f"{col} {dtype}")

            await cur.execute(
                f"CREATE TABLE IF NOT EXISTS {target} ({', '.join(cols_sql)})"
            )


async def setup_sink(
    settings: Settings,
    config: SearchPipeline,
    target_name: str,
    vectorizer_target: Optional[str] = None,
):
    """Initialize the sink table and subscription."""
    sub_name = f"sub_{target_name}"
    pub_name = f"pub_{target_name}"
    target = config.ingest.table

    await setup_state_table(settings, target_name)
    source_types = await get_source_column_types(settings, config)

    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            await cur.execute("CREATE EXTENSION IF NOT EXISTS vector")

        await register_vector(conn)

        async with conn.cursor() as cur:
            cols_sql = []
            for col in config.ingest.columns:
                dtype = source_types.get(col, "TEXT")
                if col == config.ingest.p_key:
                    cols_sql.append(f"{col} {dtype} PRIMARY KEY")
                else:
                    cols_sql.append(f"{col} {dtype}")

            await cur.execute(
                f"CREATE TABLE IF NOT EXISTS {target} ({', '.join(cols_sql)})"
            )

            try:
                await cur.execute("CREATE EXTENSION IF NOT EXISTS ai CASCADE")
            except Exception:
                import pgai
                pgai.install(settings.resolved_sink_url)
            target = config.ingest.table
            vectorizer_name = vectorizer_target or f"{target}_store"
            logger.info(f"Setting up sink for {target_name}, target table: {target}, vectorizer: {vectorizer_name}")
            await cur.execute(
                "SELECT 1 FROM ai.vectorizer WHERE name = %s",
                (vectorizer_name,),
            )

            if not await cur.fetchone():
                versioned_view = vectorizer_name.replace("_store", "_embedding")
                destination_sql = f", destination => ai.destination_table(target_table => '{vectorizer_name}', view_name => '{versioned_view}')"
            else:
                destination_sql = "" # If vectorizer exists, destination is already set

            # Resolve pgai chunking function name
            c_strat = config.pipeline.chunking.strategy
            if c_strat == "recursive_character":
                c_func = "recursive_character_text_splitter"
            elif c_strat == "markdown":
                c_func = "markdown_header_text_splitter"
            elif c_strat == "sentence":
                c_func = "sentence_splitter" # Assumption, or check? But verified only recursive_character implies text_splitter
            else:
                c_func = c_strat

            # Resolve api_key_name for pgai
            api_key_sql = ""
            if config.pipeline.embedding.api_key_name:
                api_key_sql = f", api_key_name => '{config.pipeline.embedding.api_key_name}'"

            async def try_create_vectorizer():
                try:
                    await cur.execute(
                        f"""
                        SELECT ai.create_vectorizer(
                            '{target}'::regclass,
                            name => %s,
                            loading => ai.loading_column('{config.pipeline.content_column}'),
                            embedding => ai.embedding_{config.pipeline.embedding.provider}('{config.pipeline.embedding.model}', {config.pipeline.embedding.dimension}{api_key_sql}),
                            chunking => ai.chunking_{c_func}(),
                            formatting => ai.formatting_python_template('{config.pipeline.template}'),
                            if_not_exists => true
                            {destination_sql}
                        )
                    """,
                        (vectorizer_name,),
                    )
                    return True
                except Exception as e:
                    if "does not exist" in str(e):
                        logger.warning(f"Relation not yet visible to pgai, retrying: {e}")
                        return False
                    raise e

            await wait_until(try_create_vectorizer, timeout=20.0, interval=2.0)

            # Enable triggers
            await cur.execute(
                f"""
                DO $$
                DECLARE
                    trg_name TEXT;
                BEGIN
                    FOR trg_name IN 
                        SELECT trigger_name FROM information_schema.triggers 
                        WHERE event_object_table = '{target}' AND trigger_name LIKE '_vectorizer_%'
                    LOOP
                        EXECUTE 'ALTER TABLE {target} ENABLE ALWAYS TRIGGER ' || quote_ident(trg_name);
                    END LOOP;
                END $$;
                """
            )

            # Subscription
            await cur.execute(f"SELECT 1 FROM pg_subscription WHERE subname = '{sub_name}'")
            if not await cur.fetchone():
                last_id, last_lsn = await get_replica_state(settings, target_name)
                slot_exists_on_source = await check_slot_exists(settings, target_name)
                
                copy_data = "false" if (slot_exists_on_source or last_id != "0" or last_lsn is not None) else "true"
                options_dict = settings.subscription_options.copy()
                options_dict["copy_data"] = f"'{copy_data}'"
                if slot_exists_on_source:
                    options_dict["create_slot"] = "false"

                options = ", ".join([f"{k} = {v}" for k, v in options_dict.items()])
                # Give Source catalog a moment to settle after any recent drops
                await asyncio.sleep(2.0)

                # Force release any zombie worker using this slot on the Source
                if slot_exists_on_source:
                    try:
                        async with await get_source_conn() as s_conn:
                            async with s_conn.cursor() as s_cur:
                                await s_cur.execute(
                                    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = %s",
                                    (sub_name,),
                                )
                    except Exception as e:
                        logger.warning(f"Failed to force release slot {sub_name} on source: {e}")

                # Retry loop for subscription to handle source-side visibility lag
                async def try_create_subscription():
                    try:
                        await cur.execute(
                            f"""
                            CREATE SUBSCRIPTION {sub_name} 
                            CONNECTION '{settings.subscription_connection_url}' 
                            PUBLICATION {pub_name}
                            WITH ({options})
                            """
                        )
                        return True
                    except Exception as e:
                        err_msg = str(e).lower()
                        if "already exists" in err_msg:
                            return True
                        if "in use" in err_msg:
                            # One more attempt to kick the zombie
                            try:
                                async with await get_source_conn() as s_conn:
                                    async with s_conn.cursor() as s_cur:
                                        await s_cur.execute(
                                            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = %s",
                                            (sub_name,),
                                        )
                            except Exception: pass
                        logger.warning(f"Subscription creation for {sub_name} failed, will retry: {e}")
                        return False

                await wait_until(try_create_subscription, timeout=30.0, interval=3.0)
            else:
                await cur.execute(f"ALTER SUBSCRIPTION {sub_name} ENABLE")
                await cur.execute(f"ALTER SUBSCRIPTION {sub_name} REFRESH PUBLICATION")


async def run_sql_catchup(settings: Settings, config: SearchPipeline, target_name: str):
    """Perform Keyset Pagination for catch-up."""
    last_id_str, _ = await get_replica_state(settings, target_name)
    last_id = last_id_str if last_id_str != "0" else None
    batch_size = 5000
    total_synced = 0

    while True:
        async with await get_source_conn() as source_conn:
            async with source_conn.cursor(row_factory=dict_row) as cur:
                cols = ", ".join(config.ingest.columns)
                where_clause = f"({config.ingest.filter.replace('%', '%%')})" if config.ingest.filter else "TRUE"

                if last_id is None:
                    await cur.execute(
                        f"SELECT {cols} FROM {config.ingest.table} WHERE {where_clause} ORDER BY {config.ingest.p_key} ASC LIMIT %s",
                        (batch_size,),
                    )
                else:
                    await cur.execute(
                        f"SELECT {cols} FROM {config.ingest.table} WHERE {where_clause} AND {config.ingest.p_key} > %s ORDER BY {config.ingest.p_key} ASC LIMIT %s",
                        (last_id, batch_size),
                    )
                rows = await cur.fetchall()

        if not rows:
            break

        async with await get_sink_conn() as sink_conn:
            await sink_conn.set_autocommit(True)
            async with sink_conn.cursor() as cur:
                col_names = list(rows[0].keys())
                placeholders = ", ".join(["%s"] * len(col_names))
                update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in col_names if c != config.ingest.p_key])
                upsert_query = f"""
                    INSERT INTO {config.ingest.table} ({', '.join(col_names)})
                    VALUES ({placeholders})
                    ON CONFLICT ({config.ingest.p_key}) DO UPDATE SET {update_set}
                """
                data = [tuple(row.values()) for row in rows]
                await cur.executemany(upsert_query, data)

        last_id = rows[-1][config.ingest.p_key]
        total_synced += len(rows)
        await update_replica_state(settings, target_name, last_id=str(last_id))
    logger.info(f"Catch-up complete for {target_name}: {total_synced} rows.")


async def find_and_fix_ghost_records(settings: Settings, config: SearchPipeline, target_name: str):
    """Anti-Entropy sweep to find and delete hard-deleted records."""
    logger.info(f"Starting Anti-Entropy sweep for {target_name}...")
    
    # Pre-flight readiness check to avoid race conditions in tests
    if not await wait_for_source_table(settings, config):
        raise RuntimeError(f"Source table {config.ingest.table} not found after timeout")

    chunk_size = 50000
    
    # 1. Range discovery: absolute union of Source and Sink IDs
    # This ensures we catch deletions at the very beginning or end of the tables.
    all_ids = []
    
    async with await get_sink_conn() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute(f"SELECT {config.ingest.p_key} FROM {config.ingest.table} ORDER BY {config.ingest.p_key} ASC LIMIT 1")
                row_min = await cur.fetchone()
                await cur.execute(f"SELECT {config.ingest.p_key} FROM {config.ingest.table} ORDER BY {config.ingest.p_key} DESC LIMIT 1")
                row_max = await cur.fetchone()
                
                if row_min: # If row_min is not None, table is not empty
                    all_ids.append(row_min[0])
                    all_ids.append(row_max[0])
            except Exception as e:
                logger.warning(f"Failed to discover ID range from sink: {e}")
            
    async with await get_source_conn() as s_conn:
        async with s_conn.cursor() as s_cur:
            try:
                await s_cur.execute(f"SELECT {config.ingest.p_key} FROM {config.ingest.table} ORDER BY {config.ingest.p_key} ASC LIMIT 1")
                row_min = await s_cur.fetchone()
                await s_cur.execute(f"SELECT {config.ingest.p_key} FROM {config.ingest.table} ORDER BY {config.ingest.p_key} DESC LIMIT 1")
                row_max = await s_cur.fetchone()
                
                if row_min:
                    all_ids.append(row_min[0])
                    all_ids.append(row_max[0])
            except Exception as e:
                logger.warning(f"Failed to discover ID range from source: {e}")

    if not all_ids:
        logger.debug(f"No records found for anti-entropy in {target_name}")
        return

    min_id_raw, max_id_raw = min(all_ids), max(all_ids)
    source_types = await get_source_column_types(settings, config)
    id_type = source_types.get(config.ingest.p_key, "TEXT")

    # 2. Strategy: Set Comparison for UUIDs/Strings or Small Tables
    if id_type not in ("INT", "BIGINT"):
        async with await get_source_conn() as s_conn:
            async with s_conn.cursor() as s_cur:
                await s_cur.execute(f"SELECT {config.ingest.p_key} FROM {config.ingest.table}")
                source_ids = set(r[0] for r in await s_cur.fetchall())
        async with await get_sink_conn() as k_conn:
            async with k_conn.cursor() as k_cur:
                await k_cur.execute(f"SELECT {config.ingest.p_key} FROM {config.ingest.table}")
                sink_ids = [r[0] for r in await k_cur.fetchall()]
        
        ghosts = [kid for kid in sink_ids if kid not in source_ids]
        if ghosts:
            logger.info(f"Found {len(ghosts)} ghosts in {target_name} via set comparison")
            async with await get_sink_conn() as k_conn:
                async with k_conn.cursor() as k_cur:
                    await k_cur.execute(f"DELETE FROM {config.ingest.table} WHERE {config.ingest.p_key} = ANY(%s)", (ghosts,))
                await k_conn.commit()
        return

    # 3. Strategy: Numeric Range bit_xor sweep for Large Tables
    min_id, max_id = int(min_id_raw), int(max_id_raw)
    logger.info(f"Starting Anti-Entropy bit_xor sweep for {target_name} range {min_id}-{max_id}")
    for start_id in range(min_id, max_id + 1, chunk_size):
        end_id = start_id + chunk_size
        async with await get_source_conn() as s_conn:
            async with s_conn.cursor() as s_cur:
                await s_cur.execute(f"SELECT count(*), bit_xor({config.ingest.p_key}) FROM {config.ingest.table} WHERE {config.ingest.p_key} BETWEEN %s AND %s", (start_id, end_id))
                s_count, s_xor = await s_cur.fetchone()
        async with await get_sink_conn() as k_conn:
            async with k_conn.cursor() as k_cur:
                await k_cur.execute(f"SELECT count(*), bit_xor({config.ingest.p_key}) FROM {config.ingest.table} WHERE {config.ingest.p_key} BETWEEN %s AND %s", (start_id, end_id))
                k_count, k_xor = await k_cur.fetchone()
        
        logger.debug(f"Range {start_id}-{end_id}: Source(count={s_count}, xor={s_xor}), Sink(count={k_count}, xor={k_xor})")
        
        if s_count != k_count or s_xor != k_xor:
            logger.info(f"Drift detected in range {start_id}-{end_id} for {target_name}. Performing deep check...")
            async with await get_source_conn() as s_conn:
                async with s_conn.cursor() as s_cur:
                    await s_cur.execute(f"SELECT {config.ingest.p_key} FROM {config.ingest.table} WHERE {config.ingest.p_key} BETWEEN %s AND %s", (start_id, end_id))
                    s_ids = set(r[0] for r in await s_cur.fetchall())
            async with await get_sink_conn() as k_conn:
                async with k_conn.cursor() as k_cur:
                    await k_cur.execute(f"SELECT {config.ingest.p_key} FROM {config.ingest.table} WHERE {config.ingest.p_key} BETWEEN %s AND %s", (start_id, end_id))
                    k_ids = [r[0] for r in await k_cur.fetchall()]
                    ghosts = [kid for kid in k_ids if kid not in s_ids]
                    if ghosts:
                        logger.warning(f"Found {len(ghosts)} ghosts in range {start_id}-{end_id} for {target_name}: {ghosts}")
                        async with await get_sink_conn() as del_conn:
                            async with del_conn.cursor() as del_cur:
                                await del_cur.execute(f"DELETE FROM {config.ingest.table} WHERE {config.ingest.p_key} = ANY(%s)", (ghosts,))
                            await del_conn.commit()


async def drop_subscription_completely(settings: Settings, config: SearchPipeline, target_name: str):
    """Drop replication objects for a specific target."""
    sub_name = f"sub_{target_name}"
    logger.info(f"Dropping replication {sub_name} for {target_name}...")
    try:
        async with await connect_db(settings.resolved_sink_url) as conn:
            await conn.set_autocommit(True)
            await conn.execute(f"DROP VIEW IF EXISTS {config.ingest.table}_search CASCADE")
            
            # Retry loop for dropping subscription to handle "sync in progress"
            async def try_drop_subscription():
                try:
                    # Force kill workers for this subscription
                    try:
                        await conn.execute("""
                            SELECT pg_terminate_backend(pid) 
                            FROM pg_stat_activity 
                            WHERE application_name LIKE 'pg_logical_worker%' 
                            AND datname = current_database()
                        """)
                    except Exception: pass

                    try: await conn.execute(f"ALTER SUBSCRIPTION {sub_name} DISABLE")
                    except Exception: pass
                    
                    try: await conn.execute(f"ALTER SUBSCRIPTION {sub_name} SET (slot_name = NONE)")
                    except Exception: pass
                    
                    await conn.execute(f"DROP SUBSCRIPTION IF EXISTS {sub_name} CASCADE")
                    return True
                except Exception as e:
                    logger.warning(f"Failed to drop subscription {sub_name}, retrying: {e}")
                    return False

            try:
                await wait_until(try_drop_subscription, timeout=15.0, interval=1.0)
            except asyncio.TimeoutError:
                logger.warning(f"Timed out waiting to drop subscription {sub_name}")
                        
            # Cleanup vectorizers
            async with conn.cursor() as cur:
                await cur.execute("SELECT id FROM ai.vectorizer WHERE name LIKE %s", (f"{config.ingest.table}_store%",))
                for (vid,) in await cur.fetchall():
                    await cur.execute(f"SELECT ai.drop_vectorizer({vid}, drop_all => true)")
    except Exception as e:
        logger.warning(f"teardown sink error: {e}")

    try:
        async with await connect_db(settings.source_url) as conn:
            await conn.set_autocommit(True)
            async with conn.cursor() as cur:
                await cur.execute("SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = %s", (sub_name,))
                await cur.execute(f"SELECT pg_drop_replication_slot('{sub_name}')")
    except Exception as e: logger.warning(f"teardown source error: {e}")


async def check_and_protect_source(settings: Settings, target_name: str) -> float:
    """Monitor lag and self-destruct if needed."""
    sub_name = f"sub_{target_name}"
    config = settings.pipelines[target_name]
    try:
        async with await get_source_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) / 1024 / 1024 FROM pg_replication_slots WHERE slot_name = %s", (sub_name,))
                res = await cur.fetchone()
                if res and float(res[0]) > settings.max_slot_wal_keep_size_mb:
                    await drop_subscription_completely(settings, config, target_name)
                    raise RuntimeError("Self-destructed to protect Source DB.")
                return float(res[0]) if res else 0.0
    except Exception as e:
        if "Self-destructed" in str(e): raise
        return 0.0
