import logging
import asyncio
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pgvector.psycopg import register_vector_async as register_vector  # type: ignore
from .config import Settings, TableConfig

logger = logging.getLogger(__name__)

# Global pools
_source_pool: AsyncConnectionPool | None = None
_sink_pool: AsyncConnectionPool | None = None


async def init_pools(settings: Settings):
    """Initialize connection pools for source and sink."""
    global _source_pool, _sink_pool
    
    # Check if existing pools are closed and need recreation
    if _source_pool and _source_pool.closed:
        _source_pool = None
    if _sink_pool and _sink_pool.closed:
        _sink_pool = None

    if not _source_pool:
        logger.info("Initializing source connection pool...")
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


async def wait_for_source_table(settings: Settings, config: TableConfig, timeout: int = 30):
    """Wait for a table to exist on the Source DB."""
    import asyncio
    start_time = asyncio.get_event_loop().time()
    logger.info(f"Waiting for source table {config.source_table} to be visible...")
    while asyncio.get_event_loop().time() - start_time < timeout:
        async with await get_source_conn() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SHOW search_path")
                spath = await cur.fetchone()
                
                await cur.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
                    (config.source_table,),
                )
                if await cur.fetchone():
                    return True
                
                await cur.execute(
                    "SELECT 1 FROM pg_class WHERE relname = %s",
                    (config.source_table,),
                )
                if await cur.fetchone():
                    return True
        await asyncio.sleep(1)
    logger.error(f"Timed out waiting for source table {config.source_table}")
    return False


async def get_source_column_types(
    settings: Settings, config: TableConfig
) -> dict[str, str]:
    """Query the Source DB's information_schema to get column types."""
    logger.info(
        f"Detecting column types for {config.source_table} on source..."
    )
    
    # Pre-flight readiness check to avoid race conditions in tests
    if not await wait_for_source_table(settings, config):
        raise RuntimeError(f"Source table {config.source_table} not found after timeout")

    async with await get_source_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT column_name, data_type, udt_name
                FROM information_schema.columns 
                WHERE table_name = %s 
                AND column_name = ANY(%s)
                """,
                (config.source_table, config.publication_columns),
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


async def setup_source(settings: Settings, config: TableConfig, target_name: str):
    """Remotely initialize the source publication."""
    pub_name = f"pub_{target_name}"
    logger.info(f"Setting up remote source publication {pub_name}...")
    
    # Pre-flight readiness check to avoid race conditions in tests
    if not await wait_for_source_table(settings, config):
        raise RuntimeError(f"Source table {config.source_table} not found after timeout")

    async with await get_source_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            cols = ", ".join(config.publication_columns)
            where_clause = (
                f" WHERE ({config.publication_where})"
                if config.publication_where
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
                    f"CREATE PUBLICATION {pub_name} FOR TABLE {config.source_table} ({cols}){where_clause}"
                )
            else:
                await cur.execute(
                    f"ALTER PUBLICATION {pub_name} SET TABLE {config.source_table} ({cols}){where_clause}"
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
                # This could happen on older versions or if permissions deny access
                logger.warning("Could not query ai.vectorizer_status directly")
                pass
    return statuses


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


async def ensure_embedding_cache_table(settings: Settings, config: TableConfig):
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
                        current_dim != config.embedding_dimension
                        and current_dim != -1
                    ):
                        logger.warning(
                            f"Cache dimension mismatch ({current_dim} vs {config.embedding_dimension}). Purging cache."
                        )
                        await cur.execute("DROP TABLE _embedding_cache CASCADE")

            await cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS _embedding_cache (
                    text_hash TEXT PRIMARY KEY,
                    embedding vector({config.embedding_dimension}),
                    model_name TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
                """
            )

async def cleanup_vectorizer_infrastructure(
    settings: Settings, config: TableConfig, vectorizer_name: str
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
                WHERE view_name = '{config.sink_replica_table}' 
                AND table_name IN ('{vectorizer_name}', '{embedding_view}') 
                LIMIT 1;
                
                -- 2. If it's live, we MUST drop the replica view first
                IF live_target IS NOT NULL THEN
                    EXECUTE 'DROP VIEW IF EXISTS ' || quote_ident('{config.sink_replica_table}') || ' CASCADE';
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
            END $$;
            """
            await cur.execute(cleanup_sql)

async def atomic_view_swap(
    settings: Settings,
    config: TableConfig,
    target_name: str,
    config_hash: str,
    target_table: str | None = None,
    vectorizer_target: str | None = None,
):
    """
    Update the search view to point to the latest table version and
    record the new config hash atomically. Supports Hybrid RRF.
    """
    raw_table = target_table or config.sink_raw_table
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
            f"Performing atomic view swap targeting {raw_table} (Profile: {config.search_profile})..."
        )

        await conn.set_autocommit(False)
        try:
            async with conn.cursor() as cur:
                await cur.execute(f"DROP VIEW IF EXISTS {config.sink_replica_table}")
                
                if config.search_profile == "hybrid":
                    # HYBRID SEARCH (RRF): Vector + Full-Text
                    # We create a view that exposes both, allowing the SEARCH query 
                    # to perform Rank Fusion logic.
                    logger.info("Implementing Hybrid View with RRF scoring support...")
                    await cur.execute(
                        f"""
                        CREATE VIEW {config.sink_replica_table} AS
                        SELECT 
                            r.{config.id_column},
                            e.chunk as {config.target_content_column},
                            e.{config.embedding_column},
                            to_tsvector('english', e.chunk) as ts_col
                        FROM {raw_table} r
                        LEFT JOIN {embedding_view} e ON r.{config.id_column} = e.{config.id_column}
                    """
                    )
                else:
                    # VECTOR SEARCH ONLY
                    await cur.execute(
                        f"""
                        CREATE VIEW {config.sink_replica_table} AS
                        SELECT 
                            r.{config.id_column},
                            e.chunk as {config.target_content_column},
                            e.{config.embedding_column}
                        FROM {raw_table} r
                        LEFT JOIN {embedding_view} e ON r.{config.id_column} = e.{config.id_column}
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


async def warm_up_from_cache(
    settings: Settings, config: TableConfig, source_table: str, target_store_table: str
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
                INSERT INTO {target_store_table} ({config.id_column}, {config.embedding_column})
                SELECT r.{config.id_column}, c.embedding
                FROM {source_table} r
                JOIN _embedding_cache c ON md5(COALESCE(r.{config.content_column}, '')::text || %s) = c.text_hash
                ON CONFLICT DO NOTHING
                """,
                (config.embedding_model,),
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


async def ensure_sink_raw_table(settings: Settings, config: TableConfig):
    """Ensure the raw table exists in the Sink DB with correct types."""
    target = config.sink_raw_table
    source_types = await get_source_column_types(settings, config)
    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            cols_sql = []
            for col in config.publication_columns:
                dtype = source_types.get(col, "TEXT")
                if col == config.id_column:
                    cols_sql.append(f"{col} {dtype} PRIMARY KEY")
                else:
                    cols_sql.append(f"{col} {dtype}")

            await cur.execute(
                f"CREATE TABLE IF NOT EXISTS {target} ({', '.join(cols_sql)})"
            )


async def setup_sink(
    settings: Settings,
    config: TableConfig,
    target_name: str,
    target_table: str | None = None,
    vectorizer_target: str | None = None,
):
    """Initialize the sink table and subscription."""
    sub_name = f"sub_{target_name}"
    pub_name = f"pub_{target_name}"
    target = target_table or config.sink_raw_table

    await setup_state_table(settings, target_name)
    source_types = await get_source_column_types(settings, config)

    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            await cur.execute("CREATE EXTENSION IF NOT EXISTS vector")

        await register_vector(conn)

        async with conn.cursor() as cur:
            cols_sql = []
            for col in config.publication_columns:
                dtype = source_types.get(col, "TEXT")
                if col == config.id_column:
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
            target = config.sink_raw_table
            vectorizer_name = vectorizer_target or f"{target}_store"
            logger.info(f"Setting up sink for {target_name}, target table: {target}, vectorizer: {vectorizer_name}")
            await cur.execute(
                "SELECT 1 FROM ai.vectorizer WHERE name = %s",
                (vectorizer_name,),
            )

            if not await cur.fetchone():
                versioned_view = vectorizer_name.replace("_store", "_embedding")
                destination_sql = f", destination => ai.destination_table(target_table => '{vectorizer_name}', view_name => '{versioned_view}')"

            # Retry loop for pgai creation to handle intermittent registration lag
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    await cur.execute(
                        f"""
                        SELECT ai.create_vectorizer(
                            '{target}'::regclass,
                            name => %s,
                            loading => ai.loading_column('{config.content_column}'),
                            embedding => ai.embedding_{config.embedding_provider}('{config.embedding_model}', {config.embedding_dimension}),
                            chunking => ai.chunking_{config.chunking_strategy}(),
                            formatting => ai.formatting_python_template('{config.formatting_template}'),
                            if_not_exists => true
                            {destination_sql}
                        )
                    """,
                        (vectorizer_name,),
                    )
                    break
                except Exception as e:
                    if "does not exist" in str(e) and attempt < max_retries -1:
                        logger.warning(f"Attempt {attempt+1} failed with relation error, retrying in 2s: {e}")
                        await asyncio.sleep(2)
                        continue
                    raise e

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
                # Retry loop for subscription to handle source-side visibility lag
                for attempt in range(max_retries):
                    try:
                        await cur.execute(
                            f"""
                            CREATE SUBSCRIPTION {sub_name} 
                            CONNECTION '{settings.subscription_connection_url}' 
                            PUBLICATION {pub_name}
                            WITH ({options})
                            """
                        )
                        break
                    except Exception as e:
                        if "does not exist" in str(e) and attempt < max_retries - 1:
                            logger.warning(f"Attempt {attempt+1} failed to create subscription, retrying: {e}")
                            await asyncio.sleep(2)
                            continue
                        raise e
            else:
                await cur.execute(f"ALTER SUBSCRIPTION {sub_name} ENABLE")
                await cur.execute(f"ALTER SUBSCRIPTION {sub_name} REFRESH PUBLICATION")


async def run_sql_catchup(settings: Settings, config: TableConfig, target_name: str):
    """Perform Keyset Pagination for catch-up."""
    last_id_str, _ = await get_replica_state(settings, target_name)
    last_id = last_id_str if last_id_str != "0" else None
    batch_size = 5000
    total_synced = 0

    while True:
        async with await get_source_conn() as source_conn:
            async with source_conn.cursor(row_factory=dict_row) as cur:
                cols = ", ".join(config.publication_columns)
                where_clause = f"({config.publication_where.replace('%', '%%')})" if config.publication_where else "TRUE"

                if last_id is None:
                    await cur.execute(
                        f"SELECT {cols} FROM {config.source_table} WHERE {where_clause} ORDER BY {config.id_column} ASC LIMIT %s",
                        (batch_size,),
                    )
                else:
                    await cur.execute(
                        f"SELECT {cols} FROM {config.source_table} WHERE {where_clause} AND {config.id_column} > %s ORDER BY {config.id_column} ASC LIMIT %s",
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
                update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in col_names if c != config.id_column])
                upsert_query = f"""
                    INSERT INTO {config.sink_raw_table} ({', '.join(col_names)})
                    VALUES ({placeholders})
                    ON CONFLICT ({config.id_column}) DO UPDATE SET {update_set}
                """
                data = [tuple(row.values()) for row in rows]
                await cur.executemany(upsert_query, data)

        last_id = rows[-1][config.id_column]
        total_synced += len(rows)
        await update_replica_state(settings, target_name, last_id=str(last_id))
    logger.info(f"Catch-up complete for {target_name}: {total_synced} rows.")


async def find_and_fix_ghost_records(settings: Settings, config: TableConfig, target_name: str):
    """Anti-Entropy sweep to find and delete hard-deleted records."""
    logger.info(f"Starting Anti-Entropy sweep for {target_name}...")
    
    # Pre-flight readiness check to avoid race conditions in tests
    if not await wait_for_source_table(settings, config):
        raise RuntimeError(f"Source table {config.source_table} not found after timeout")

    chunk_size = 50000
    
    # 1. Range discovery: absolute union of Source and Sink IDs
    # This ensures we catch deletions at the very beginning or end of the tables.
    all_ids = []
    
    async with await get_sink_conn() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute(f"SELECT {config.id_column} FROM {config.sink_raw_table} ORDER BY {config.id_column} ASC LIMIT 1")
                row_min = await cur.fetchone()
                await cur.execute(f"SELECT {config.id_column} FROM {config.sink_raw_table} ORDER BY {config.id_column} DESC LIMIT 1")
                row_max = await cur.fetchone()
                
                if row_min: # If row_min is not None, table is not empty
                    all_ids.append(row_min[0])
                    all_ids.append(row_max[0])
            except Exception as e:
                logger.warning(f"Failed to discover ID range from sink: {e}")
            
    async with await get_source_conn() as s_conn:
        async with s_conn.cursor() as s_cur:
            try:
                await s_cur.execute(f"SELECT {config.id_column} FROM {config.source_table} ORDER BY {config.id_column} ASC LIMIT 1")
                row_min = await s_cur.fetchone()
                await s_cur.execute(f"SELECT {config.id_column} FROM {config.source_table} ORDER BY {config.id_column} DESC LIMIT 1")
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
    id_type = source_types.get(config.id_column, "TEXT")

    # 2. Strategy: Set Comparison for UUIDs/Strings or Small Tables
    if id_type not in ("INT", "BIGINT"):
        async with await get_source_conn() as s_conn:
            async with s_conn.cursor() as s_cur:
                await s_cur.execute(f"SELECT {config.id_column} FROM {config.source_table}")
                source_ids = set(r[0] for r in await s_cur.fetchall())
        async with await get_sink_conn() as k_conn:
            async with k_conn.cursor() as k_cur:
                await k_cur.execute(f"SELECT {config.id_column} FROM {config.sink_raw_table}")
                sink_ids = [r[0] for r in await k_cur.fetchall()]
        
        ghosts = [kid for kid in sink_ids if kid not in source_ids]
        if ghosts:
            logger.info(f"Found {len(ghosts)} ghosts in {target_name} via set comparison")
            async with await get_sink_conn() as k_conn:
                async with k_conn.cursor() as k_cur:
                    await k_cur.execute(f"DELETE FROM {config.sink_raw_table} WHERE {config.id_column} = ANY(%s)", (ghosts,))
                await k_conn.commit()
        return

    # 3. Strategy: Numeric Range bit_xor sweep for Large Tables
    min_id, max_id = int(min_id_raw), int(max_id_raw)
    logger.info(f"Starting Anti-Entropy bit_xor sweep for {target_name} range {min_id}-{max_id}")
    for start_id in range(min_id, max_id + 1, chunk_size):
        end_id = start_id + chunk_size
        async with await get_source_conn() as s_conn:
            async with s_conn.cursor() as s_cur:
                await s_cur.execute(f"SELECT count(*), bit_xor({config.id_column}) FROM {config.source_table} WHERE {config.id_column} BETWEEN %s AND %s", (start_id, end_id))
                s_count, s_xor = await s_cur.fetchone()
        async with await get_sink_conn() as k_conn:
            async with k_conn.cursor() as k_cur:
                await k_cur.execute(f"SELECT count(*), bit_xor({config.id_column}) FROM {config.sink_raw_table} WHERE {config.id_column} BETWEEN %s AND %s", (start_id, end_id))
                k_count, k_xor = await k_cur.fetchone()
        
        logger.debug(f"Range {start_id}-{end_id}: Source(count={s_count}, xor={s_xor}), Sink(count={k_count}, xor={k_xor})")
        
        if s_count != k_count or s_xor != k_xor:
            logger.info(f"Drift detected in range {start_id}-{end_id} for {target_name}. Performing deep check...")
            async with await get_source_conn() as s_conn:
                async with s_conn.cursor() as s_cur:
                    await s_cur.execute(f"SELECT {config.id_column} FROM {config.source_table} WHERE {config.id_column} BETWEEN %s AND %s", (start_id, end_id))
                    s_ids = set(r[0] for r in await s_cur.fetchall())
            async with await get_sink_conn() as k_conn:
                async with k_conn.cursor() as k_cur:
                    await k_cur.execute(f"SELECT {config.id_column} FROM {config.sink_raw_table} WHERE {config.id_column} BETWEEN %s AND %s", (start_id, end_id))
                    k_ids = [r[0] for r in await k_cur.fetchall()]
                    ghosts = [kid for kid in k_ids if kid not in s_ids]
                    if ghosts:
                        logger.warning(f"Found {len(ghosts)} ghosts in range {start_id}-{end_id} for {target_name}: {ghosts}")
                        async with await get_sink_conn() as del_conn:
                            async with del_conn.cursor() as del_cur:
                                await del_cur.execute(f"DELETE FROM {config.sink_raw_table} WHERE {config.id_column} = ANY(%s)", (ghosts,))
                            await del_conn.commit()


async def drop_subscription_completely(settings: Settings, config: TableConfig, target_name: str):
    """Drop replication objects for a specific target."""
    sub_name = f"sub_{target_name}"
    logger.info(f"Dropping replication {sub_name} for {target_name}...")
    try:
        async with await connect_db(settings.resolved_sink_url) as conn:
            await conn.set_autocommit(True)
            await conn.execute(f"DROP VIEW IF EXISTS {config.sink_replica_table} CASCADE")
            try:
                await conn.execute(f"DROP SUBSCRIPTION IF EXISTS {sub_name} CASCADE")
            except Exception: pass

            # Cleanup vectorizers
            async with conn.cursor() as cur:
                await cur.execute("SELECT id FROM ai.vectorizer WHERE name LIKE %s", (f"{config.sink_raw_table}_store%",))
                for (vid,) in await cur.fetchall():
                    await cur.execute(f"SELECT ai.drop_vectorizer({vid}, drop_all => true)")
    except Exception as e: logger.warning(f"teardown sink error: {e}")

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
    config = settings.tables[target_name]
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
