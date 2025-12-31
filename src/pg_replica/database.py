import logging
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pgvector.psycopg import register_vector_async as register_vector  # type: ignore
from .config import Settings

logger = logging.getLogger(__name__)

# Global pools
_source_pool: AsyncConnectionPool | None = None
_sink_pool: AsyncConnectionPool | None = None


async def init_pools(settings: Settings):
    """Initialize connection pools for source and sink."""
    global _source_pool, _sink_pool
    if not _source_pool:
        logger.info("Initializing source connection pool...")
        _source_pool = AsyncConnectionPool(
            conninfo=settings.source_url,
            min_size=1,
            max_size=5,
            open=False,  # Don't open yet
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


async def get_source_column_types(settings: Settings) -> dict[str, str]:
    """Query the Source DB's information_schema to get column types."""
    logger.info(
        f"Detecting column types for {settings.source_table} on source..."
    )
    async with await get_source_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT column_name, data_type, udt_name
                FROM information_schema.columns 
                WHERE table_name = %s 
                AND column_name = ANY(%s)
                """,
                (settings.source_table, settings.publication_columns),
            )
            rows = await cur.fetchall()
            # Map data_type to something we can use in CREATE TABLE
            # udt_name is often more precise (e.g. 'uuid')
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


async def setup_source(settings: Settings):
    """Remotely initialize the source publication."""
    logger.info("Setting up remote source publication...")
    async with await get_source_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            cols = ", ".join(settings.publication_columns)
            where_clause = (
                f" WHERE ({settings.publication_where})"
                if settings.publication_where
                else ""
            )

            await cur.execute(
                f"SELECT 1 FROM pg_publication WHERE pubname = '{settings.publication_name}'"
            )
            if not await cur.fetchone():
                logger.info(
                    f"Creating publication {settings.publication_name} on Source for columns ({cols}){where_clause}..."
                )
                await cur.execute(
                    f"CREATE PUBLICATION {settings.publication_name} FOR TABLE {settings.source_table} ({cols}){where_clause}"
                )
            else:
                logger.info(
                    f"Syncing publication {settings.publication_name} with columns ({cols}){where_clause}..."
                )
                await cur.execute(
                    f"ALTER PUBLICATION {settings.publication_name} SET TABLE {settings.source_table} ({cols}){where_clause}"
                )


async def setup_state_table(settings: Settings):
    """Create the _replica_state table in the Sink DB."""
    logger.info("Setting up replica state table in Sink...")
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
                (settings.subscription_name,),
            )


async def get_replica_state(
    settings: Settings,
) -> tuple[str | None, str | None]:
    """Get (last_id, last_lsn) from the state table."""
    async with await get_sink_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT last_id, last_lsn FROM _replica_state WHERE key = %s",
                (settings.subscription_name,),
            )
            row = await cur.fetchone()
            if row:
                return str(row[0]) if row[0] is not None else None, (
                    str(row[1]) if row[1] is not None else None
                )
            return None, None


async def update_replica_state(
    settings: Settings, last_id: str | None = None, lsn: str | None = None
):
    """Update high-water mark or LSN in the state table."""
    async with await get_sink_conn() as conn:
        # We ensure autocommit for state updates
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            if last_id is not None and lsn is not None:
                await cur.execute(
                    "UPDATE _replica_state SET last_id = %s, last_lsn = %s, updated_at = NOW() WHERE key = %s",
                    (str(last_id), str(lsn), settings.subscription_name),
                )
            elif last_id is not None:
                await cur.execute(
                    "UPDATE _replica_state SET last_id = %s, updated_at = NOW() WHERE key = %s",
                    (str(last_id), settings.subscription_name),
                )
            elif lsn is not None:
                await cur.execute(
                    "UPDATE _replica_state SET last_lsn = %s, updated_at = NOW() WHERE key = %s",
                    (str(lsn), settings.subscription_name),
                )


async def ensure_embedding_cache_table(settings: Settings):
    """Create the Postgres-native embedding cache table."""
    logger.info("Ensuring embedding cache table exists...")
    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            await cur.execute("CREATE EXTENSION IF NOT EXISTS vector")

            # Protection: If dimension changed, existing cache is invalid/incompatible
            # atttypmod stores the dimension (e.g. 768) plus 4 bytes of header
            # We check if table exists first to avoid regclass error
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
                    # -1 means unspecified dimension. Otherwise it's dimension
                    if (
                        current_dim != settings.embedding_dimension
                        and current_dim != -1
                    ):
                        logger.warning(
                            f"Cache dimension mismatch ({current_dim} vs {settings.embedding_dimension}). Purging cache."
                        )
                        await cur.execute("DROP TABLE _embedding_cache CASCADE")

            await cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS _embedding_cache (
                    text_hash TEXT PRIMARY KEY,
                    embedding vector({settings.embedding_dimension}),
                    model_name TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
                """
            )


async def atomic_view_swap(
    settings: Settings,
    config_hash: str,
    target_table: str | None = None,
    vectorizer_target: str | None = None,
):
    """
    Update the search view to point to the latest table version and
    record the new config hash atomically.
    """
    raw_table = target_table or settings.sink_raw_table
    # Default guess if we don't have a vectorizer target or it's not found
    embedding_view = f"{raw_table}_embedding"

    async with await get_sink_conn() as conn:
        # 1. Query pgai for the actual view name to be robust (Dynamic Join)
        if vectorizer_target:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT config->'destination'->>'view_name' FROM ai.vectorizer WHERE name = %s",
                    (vectorizer_target,),
                )
                row = await cur.fetchone()
                if row and row[0]:
                    embedding_view = row[0]
                    logger.info(
                        f"Resolved embedding view from pgai: {embedding_view}"
                    )
                else:
                    # Fallback to deterministic naming if not in catalog yet
                    embedding_view = vectorizer_target.replace(
                        "_store", "_embedding"
                    )

        logger.info(
            f"Performing atomic view swap targeting {raw_table} with embedding source {embedding_view}..."
        )

        view_content_sql = f"'Product: ' || COALESCE(r.name, '') || ' Description: ' || COALESCE(r.{settings.content_column}, '')"

        await conn.set_autocommit(False)  # Start transaction
        try:
            async with conn.cursor() as cur:
                # 1. Update View
                await cur.execute(
                    f"DROP VIEW IF EXISTS {settings.sink_replica_table}"
                )
                await cur.execute(
                    f"""
                    CREATE VIEW {settings.sink_replica_table} AS
                    SELECT 
                        r.{settings.id_column},
                        {view_content_sql} as {settings.target_content_column},
                        e.{settings.embedding_column}
                    FROM {raw_table} r
                    LEFT JOIN {embedding_view} e ON r.{settings.id_column} = e.{settings.id_column}
                """
                )

                # 2. Update State Hash
                await cur.execute(
                    "UPDATE _replica_state SET config_hash = %s, updated_at = NOW() WHERE key = %s",
                    (config_hash, settings.subscription_name),
                )
            await conn.commit()
        except Exception as e:
            await conn.rollback()
            raise e


async def warm_up_from_cache(
    settings: Settings, source_table: str, target_store_table: str
):
    """Populate a new embedding table from the cache to avoid re-calls."""
    logger.info(f"Warming up {target_store_table} from cache...")

    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            # Ensure the store table exists before attempting warm-up
            await cur.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
                (target_store_table,),
            )
            if not await cur.fetchone():
                logger.info(
                    f"Table {target_store_table} not found yet, skipping warm-up."
                )
                return

            await cur.execute(
                f"""
                INSERT INTO {target_store_table} ({settings.id_column}, {settings.embedding_column})
                SELECT r.{settings.id_column}, c.embedding
                FROM {source_table} r
                JOIN _embedding_cache c ON md5(COALESCE(r.{settings.content_column}, '')::text || %s) = c.text_hash
                ON CONFLICT DO NOTHING
                """,
                (settings.embedding_model,),
            )


async def check_slot_exists(settings: Settings) -> bool:
    """Check if the replication slot exists on the Source DB."""
    async with await get_source_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                (settings.subscription_name,),
            )
            return await cur.fetchone() is not None


async def create_placeholder_slot(settings: Settings) -> str:
    """Create a logical replication slot on Source and return its consistent LSN."""
    logger.info(
        f"Creating placeholder replication slot {settings.subscription_name} on Source..."
    )
    async with await get_source_conn() as conn:
        async with conn.cursor() as cur:
            # Check if exists first
            await cur.execute(
                "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = %s",
                (settings.subscription_name,),
            )
            res = await cur.fetchone()
            if res:
                lsn = str(res[0])
                logger.info(f"Slot already exists at LSN: {lsn}")
                return lsn

            # pg_create_logical_replication_slot(slot_name, plugin, temporary, wait_for_ready)
            # wait_for_ready=True ensures it returns a consistent LSN
            await cur.execute(
                "SELECT lsn FROM pg_create_logical_replication_slot(%s, 'pgoutput', false, true)",
                (settings.subscription_name,),
            )
            res = await cur.fetchone()
            if not res:
                raise RuntimeError("Failed to create replication slot")
            lsn = str(res[0])
            logger.info(f"Created slot at LSN: {lsn}")
            return lsn


async def ensure_sink_raw_table(
    settings: Settings, table_name: str | None = None
):
    """Ensure the raw table exists in the Sink DB with correct types."""
    target = table_name or settings.sink_raw_table
    source_types = await get_source_column_types(settings)
    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            cols_sql = []
            for col in settings.publication_columns:
                dtype = source_types.get(col, "TEXT")
                if col == settings.id_column:
                    cols_sql.append(f"{col} {dtype} PRIMARY KEY")
                else:
                    cols_sql.append(f"{col} {dtype}")

            await cur.execute(
                f"CREATE TABLE IF NOT EXISTS {target} ({', '.join(cols_sql)})"
            )


async def setup_sink(
    settings: Settings,
    target_table: str | None = None,
    vectorizer_target: str | None = None,
):
    """Initialize the sink table and subscription."""
    logger.info("Setting up local sink database...")

    target = target_table or settings.sink_raw_table

    # 0. Ensure state table exists first
    await setup_state_table(settings)

    # 1. Detect source types for dynamic creation
    source_types = await get_source_column_types(settings)

    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            # Extensions
            await cur.execute("CREATE EXTENSION IF NOT EXISTS vector")

        # Register pgvector ONLY after it exists in the DB
        await register_vector(conn)

        async with conn.cursor() as cur:
            # Tables - Dynamically create all columns based on source types
            cols_sql = []
            for col in settings.publication_columns:
                dtype = source_types.get(col, "TEXT")
                if col == settings.id_column:
                    cols_sql.append(f"{col} {dtype} PRIMARY KEY")
                else:
                    cols_sql.append(f"{col} {dtype}")

            await cur.execute(
                f"CREATE TABLE IF NOT EXISTS {target} ({', '.join(cols_sql)})"
            )

            # Define the vectorizer
            try:
                await cur.execute("CREATE EXTENSION IF NOT EXISTS ai CASCADE")
            except Exception:
                logger.info(
                    "Extension 'ai' not found. Attempting to install via pgai python package..."
                )
                import pgai

                pgai.install(settings.resolved_sink_url)
                logger.info("Extension 'ai' installed successfully.")

            # Check if vectorizer already exists
            vectorizer_name = vectorizer_target or f"{target}_store"

            await cur.execute(
                "SELECT 1 FROM ai.vectorizer WHERE name = %s",
                (vectorizer_name,),
            )

            if not await cur.fetchone():
                logger.info(
                    f"Creating pgai vectorizer '{vectorizer_name}' for {target}..."
                )

                destination_sql = ""
                if vectorizer_target:
                    # Explicitly version both the target table and the view name
                    # We use target_table and view_name explicitly to avoid pgai's
                    # automatic '_store' suffixing logic when using the 'destination' arg.
                    versioned_view = vectorizer_target.replace(
                        "_store", "_embedding"
                    )
                    destination_sql = f", destination => ai.destination_table(target_table => '{vectorizer_target}', view_name => '{versioned_view}')"

                await cur.execute(
                    f"""
                    SELECT ai.create_vectorizer(
                        '{target}'::regclass,
                        name => %s,
                        loading => ai.loading_column('{settings.content_column}'),
                        embedding => ai.embedding_{settings.embedding_provider}('{settings.embedding_model}', {settings.embedding_dimension}),
                        chunking => ai.chunking_{settings.chunking_strategy}(),
                        formatting => ai.formatting_python_template('{settings.formatting_template}'),
                        if_not_exists => true
                        {destination_sql}
                    )
                """,
                    (vectorizer_name,),
                )

            # Enable pgai triggers for native replication
            await cur.execute(
                f"""
                DO $$
                DECLARE
                    trg_name TEXT;
                BEGIN
                    FOR trg_name IN 
                        SELECT trigger_name 
                        FROM information_schema.triggers 
                        WHERE event_object_table = '{target}' 
                        AND trigger_name LIKE '_vectorizer_%'
                    LOOP
                        EXECUTE 'ALTER TABLE {target} ENABLE ALWAYS TRIGGER ' || quote_ident(trg_name);
                    END LOOP;
                END $$;
            """
            )

            # Subscription
            await cur.execute(
                f"SELECT 1 FROM pg_subscription WHERE subname = '{settings.subscription_name}'"
            )
            if not await cur.fetchone():
                # For dynamic recovery, we start with copy_data = false if we have a state
                last_id, last_lsn = await get_replica_state(settings)

                # If we pre-created the slot, we must set create_slot = false
                slot_exists_on_source = await check_slot_exists(settings)

                # CRITICAL: We set copy_data = false if we are performing a hybrid recovery
                # (slot already exists because we just created it) or if we have existing state.
                copy_data = (
                    "false"
                    if (
                        slot_exists_on_source
                        or last_id != "0"
                        or last_lsn is not None
                    )
                    else "true"
                )

                options_dict = settings.subscription_options.copy()
                options_dict["copy_data"] = f"'{copy_data}'"

                if slot_exists_on_source:
                    options_dict["create_slot"] = "false"

                options = ", ".join(
                    [f"{k} = {v}" for k, v in options_dict.items()]
                )
                logger.info(
                    f"Creating subscription {settings.subscription_name} WITH ({options})..."
                )
                await cur.execute(
                    f"""
                    CREATE SUBSCRIPTION {settings.subscription_name} 
                    CONNECTION '{settings.subscription_connection_url}' 
                    PUBLICATION {settings.publication_name}
                    WITH ({options})
                """
                )
            else:
                logger.info(
                    f"Refreshing subscription {settings.subscription_name}..."
                )
                await cur.execute(
                    f"ALTER SUBSCRIPTION {settings.subscription_name} ENABLE"
                )
                await cur.execute(
                    f"ALTER SUBSCRIPTION {settings.subscription_name} REFRESH PUBLICATION"
                )


async def run_sql_catchup(settings: Settings):
    """
    Perform Keyset Pagination to bridge the gap between Source and Sink.
    Uses ON CONFLICT to ensure idempotency.
    """
    last_id_str, _ = await get_replica_state(settings)
    last_id = last_id_str if last_id_str != "0" else None

    batch_size = 5000
    total_synced = 0

    logger.info(f"Starting SQL catch-up from ID {last_id}...")

    while True:
        # 1. Fetch batch from Source
        async with await get_source_conn() as source_conn:
            async with source_conn.cursor(row_factory=dict_row) as cur:
                cols = ", ".join(settings.publication_columns)

                # Hybrid Recovery must honor the publication filter if it exists.
                # We escape % to %% because psycopg uses % for placeholders.
                where_clause = (
                    f"({settings.publication_where.replace('%', '%%')})"
                    if settings.publication_where
                    else "TRUE"
                )

                if last_id is None:
                    # First batch
                    await cur.execute(
                        f"SELECT {cols} FROM {settings.source_table} WHERE {where_clause} ORDER BY {settings.id_column} ASC LIMIT %s",
                        (batch_size,),
                    )
                else:
                    await cur.execute(
                        f"SELECT {cols} FROM {settings.source_table} WHERE {where_clause} AND {settings.id_column} > %s ORDER BY {settings.id_column} ASC LIMIT %s",
                        (last_id, batch_size),
                    )
                rows = await cur.fetchall()

        if not rows:
            break

        # 2. Upsert batch into Sink
        async with await get_sink_conn() as sink_conn:
            await sink_conn.set_autocommit(True)
            async with sink_conn.cursor() as cur:
                col_names = list(rows[0].keys())
                placeholders = ", ".join(["%s"] * len(col_names))
                update_set = ", ".join(
                    [
                        f"{c} = EXCLUDED.{c}"
                        for c in col_names
                        if c != settings.id_column
                    ]
                )

                upsert_query = f"""
                    INSERT INTO {settings.sink_raw_table} ({', '.join(col_names)})
                    VALUES ({placeholders})
                    ON CONFLICT ({settings.id_column}) DO UPDATE SET {update_set}
                """

                # Convert dict rows to tuples for executemany
                data = [tuple(row.values()) for row in rows]
                await cur.executemany(upsert_query, data)

        last_id = rows[-1][settings.id_column]
        total_synced += len(rows)
        await update_replica_state(settings, last_id=str(last_id))
        logger.info(f"Synced {total_synced} rows... (Last ID: {last_id})")

    logger.info(f"SQL catch-up complete. Total rows synced: {total_synced}")


async def find_and_fix_ghost_records(settings: Settings):
    """
    Find records in Sink that no longer exist in Source and delete them.
    Uses XOR checksums over chunks of IDs for efficiency.
    """
    logger.info("Starting Anti-Entropy (Ghost Cleaner) sweep...")
    chunk_size = 50000

    # We need to know the max ID in Sink to know where to stop
    async with await get_sink_conn() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"SELECT MIN({settings.id_column}), MAX({settings.id_column}) FROM {settings.sink_raw_table}"
            )
            res = await cur.fetchone()
            if not res or res[0] is None:
                logger.info("Sink is empty, skipping Anti-Entropy.")
                return
            min_id_raw, max_id_raw = res

    # Handle different ID types (INT vs UUID)
    source_types = await get_source_column_types(settings)
    id_type = source_types.get(settings.id_column, "TEXT")

    if id_type not in ("INT", "BIGINT"):
        logger.info(
            f"Non-numeric ID type ({id_type}) detected. Using simple ID-list comparison for Anti-Entropy..."
        )
        async with await get_source_conn() as s_conn:
            async with s_conn.cursor() as s_cur:
                await s_cur.execute(
                    f"SELECT {settings.id_column} FROM {settings.source_table}"
                )
                source_ids = set(r[0] for r in await s_cur.fetchall())

        async with await get_sink_conn() as k_conn:
            async with k_conn.cursor() as k_cur:
                await k_cur.execute(
                    f"SELECT {settings.id_column} FROM {settings.sink_raw_table}"
                )
                sink_ids = [r[0] for r in await k_cur.fetchall()]

        ghosts = [sid for sid in sink_ids if sid not in source_ids]
        if ghosts:
            logger.warning(f"Found {len(ghosts)} ghost records. Deleting...")
            async with await get_sink_conn() as k_conn:
                await k_conn.set_autocommit(True)
                async with k_conn.cursor() as k_cur:
                    await k_cur.execute(
                        f"DELETE FROM {settings.sink_raw_table} WHERE {settings.id_column} = ANY(%s)",
                        (ghosts,),
                    )
        logger.info("Anti-Entropy sweep complete.")
        return

    # Numeric ID logic (XOR Checksums)
    min_id, max_id = int(min_id_raw), int(max_id_raw)
    total_deleted = 0

    for start_id in range(min_id, max_id + 1, chunk_size):
        end_id = start_id + chunk_size

        # 1. Get checksum from Source
        async with await get_source_conn() as s_conn:
            async with s_conn.cursor() as s_cur:
                await s_cur.execute(
                    f"SELECT count(*), COALESCE(bit_xor({settings.id_column}), 0) FROM {settings.source_table} WHERE {settings.id_column} BETWEEN %s AND %s",
                    (start_id, end_id),
                )
                s_count, s_xor = await s_cur.fetchone()

        # 2. Get checksum from Sink
        async with await get_sink_conn() as k_conn:
            async with k_conn.cursor() as k_cur:
                await k_cur.execute(
                    f"SELECT count(*), COALESCE(bit_xor({settings.id_column}), 0) FROM {settings.sink_raw_table} WHERE {settings.id_column} BETWEEN %s AND %s",
                    (start_id, end_id),
                )
                k_count, k_xor = await k_cur.fetchone()

        # 3. If mismatch, find and fix
        if s_count != k_count or s_xor != k_xor:
            logger.warning(
                f"Drift detected in ID range {start_id}-{end_id}. Repairing..."
            )
            async with await get_source_conn() as s_conn:
                async with s_conn.cursor() as s_cur:
                    await s_cur.execute(
                        f"SELECT {settings.id_column} FROM {settings.source_table} WHERE {settings.id_column} BETWEEN %s AND %s",
                        (start_id, end_id),
                    )
                    s_ids = set(r[0] for r in await s_cur.fetchall())

            async with await get_sink_conn() as k_conn:
                async with k_conn.cursor() as k_cur:
                    await k_cur.execute(
                        f"SELECT {settings.id_column} FROM {settings.sink_raw_table} WHERE {settings.id_column} BETWEEN %s AND %s",
                        (start_id, end_id),
                    )
                    k_ids = [r[0] for r in await k_cur.fetchall()]

                    ghosts = [kid for kid in k_ids if kid not in s_ids]
                    if ghosts:
                        await k_conn.set_autocommit(True)
                        await k_cur.execute(
                            f"DELETE FROM {settings.sink_raw_table} WHERE {settings.id_column} = ANY(%s)",
                            (ghosts,),
                        )
                        total_deleted += len(ghosts)

    logger.info(
        f"Anti-Entropy sweep complete. Total ghost records deleted: {total_deleted}"
    )


async def drop_subscription_completely(settings: Settings):
    """Drop subscription, slot, and optionally the pgai vectorizer on shutdown."""
    logger.info("Dropping subscription and slot from source...")

    # 1. Drop Subscription (Sink)
    # We use a direct connection here to bypass any pool exhaustion or state issues during teardown.
    try:
        async with await connect_db(settings.resolved_sink_url) as conn:
            await conn.set_autocommit(True)

            # CRITICAL: We must drop the replica view FIRST, because it depends on the vectorizer views.
            # If we don't, dropping the vectorizer will fail with a dependency error.
            await conn.execute(
                f"DROP VIEW IF EXISTS {settings.sink_replica_table} CASCADE"
            )

            # 1b. Disable Subscription before Drop (Prevents Hang)
            # PostgreSQL's DROP SUBSCRIPTION can block if the apply worker is active.
            # Forcefully disabling it first helps ensure the worker exits quickly.
            logger.info(
                f"Disabling subscription {settings.subscription_name}..."
            )
            try:
                # Use a short timeout for these operations
                async with conn.cursor() as cur:
                    await cur.execute("SET statement_timeout = '10s'")
                    await cur.execute(
                        f"ALTER SUBSCRIPTION {settings.subscription_name} DISABLE"
                    )
                    # 1c. Decouple slot from subscription to prevent DROP SUBSCRIPTION from
                    # trying to drop the slot itself, which can hang if the worker hasn't exited yet.
                    await cur.execute(
                        f"ALTER SUBSCRIPTION {settings.subscription_name} SET (slot_name = NONE)"
                    )
            except Exception as e:
                logger.debug(f"Could not disable or decouple subscription: {e}")

            logger.info(
                f"Dropping subscription {settings.subscription_name}..."
            )
            try:
                await conn.execute(
                    f"DROP SUBSCRIPTION IF EXISTS {settings.subscription_name}"
                )
            except Exception as e:
                logger.warning(f"Failed to drop subscription: {e}")

            # Also cleanup vectorizers for this table
            async with conn.cursor() as cur:
                await cur.execute("SET statement_timeout = '10s'")
                await cur.execute(
                    "SELECT id FROM ai.vectorizer WHERE source_table::text = %s OR source_table::text = %s",
                    (
                        settings.sink_raw_table,
                        f"public.{settings.sink_raw_table}",
                    ),
                )
                v_ids = [r[0] for r in await cur.fetchall()]
                for vid in v_ids:
                    logger.info(f"Cleanup: Dropping pgai vectorizer {vid}")
                    try:
                        await cur.execute(
                            f"SELECT ai.drop_vectorizer({vid}, drop_all => true)"
                        )
                    except Exception as e:
                        logger.warning(f"Failed to drop vectorizer {vid}: {e}")
    except Exception as e:
        logger.warning(f"Failed to connect to sink or perform cleanup: {e}")

    # 2. Drop Slot (Source)
    try:
        async with await connect_db(settings.source_url) as conn:
            await conn.set_autocommit(True)
            logger.info(
                f"Dropping slot {settings.subscription_name} on source..."
            )
            async with conn.cursor() as cur:
                await cur.execute("SET statement_timeout = '10s'")
                # 2b. Forcefully terminate any backends using this slot to prevent hang
                await cur.execute(
                    """
                    SELECT pg_terminate_backend(active_pid) 
                    FROM pg_replication_slots 
                    WHERE slot_name = %s AND active_pid IS NOT NULL
                    """,
                    (settings.subscription_name,),
                )

                await cur.execute(
                    f"SELECT pg_drop_replication_slot('{settings.subscription_name}') "
                    f"WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '{settings.subscription_name}')"
                )
    except Exception as e:
        logger.warning(f"Failed to drop replication slot: {e}")


async def check_and_protect_source(settings: Settings) -> float:
    """
    Monitor replication lag on Source and self-destruct if it exceeds safety limits.
    Returns current lag in MB if safe, raises Exception if self-destruct triggered.
    """
    lag_mb = 0.0
    try:
        async with await get_source_conn() as conn:
            async with conn.cursor() as cur:
                # Query lag for our specific slot
                await cur.execute(
                    """
                    SELECT 
                        pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) / 1024 / 1024 as lag_mb
                    FROM pg_replication_slots 
                    WHERE slot_name = %s
                """,
                    (settings.subscription_name,),
                )
                res = await cur.fetchone()
                if res:
                    lag_mb = float(res[0])
                    if lag_mb > settings.max_slot_wal_keep_size_mb:
                        logger.critical(
                            f"REPLICATION LAG ({lag_mb:.1f} MB) EXCEEDED SAFETY LIMIT ({settings.max_slot_wal_keep_size_mb} MB)!"
                        )
                        logger.critical(
                            "Emergency shutdown: Dropping subscription to protect Source DB disk space."
                        )
                        await drop_subscription_completely(settings)
                        raise RuntimeError(
                            "Self-destructed to protect Source DB."
                        )
                    elif lag_mb > (settings.max_slot_wal_keep_size_mb * 0.8):
                        logger.warning(
                            f"High replication lag detected: {lag_mb:.1f} MB (Limit: {settings.max_slot_wal_keep_size_mb} MB)"
                        )
        return lag_mb
    except Exception as e:
        if "Self-destructed" in str(e):
            raise
        logger.warning(f"Failed to check replication lag: {e}")
        return lag_mb
