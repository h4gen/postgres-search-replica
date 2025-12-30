import logging
import psycopg
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


async def setup_sink(settings: Settings):
    """Initialize the sink table and subscription."""
    logger.info("Setting up local sink database...")
    async with await get_sink_conn() as conn:
        await conn.set_autocommit(True)
        async with conn.cursor() as cur:
            # Extensions
            await cur.execute("CREATE EXTENSION IF NOT EXISTS vector")

        # Register pgvector ONLY after it exists in the DB
        await register_vector(conn)

        async with conn.cursor() as cur:
            # Tables - Dynamically create all columns defined in publication_columns
            cols_sql = []
            for col in settings.publication_columns:
                if col == settings.id_column:
                    cols_sql.append(f"{col} INT PRIMARY KEY")
                else:
                    # Defaulting to TEXT for all non-ID columns for simplicity
                    cols_sql.append(f"{col} TEXT")

            await cur.execute(
                f"CREATE TABLE IF NOT EXISTS {settings.sink_raw_table} ({', '.join(cols_sql)})"
            )

            # Define the vectorizer instead of creating a replica table manually
            # pgai will automatically create the {settings.sink_raw_table}_embedding table
            try:
                await cur.execute("CREATE EXTENSION IF NOT EXISTS ai CASCADE")
            except Exception:
                logger.info(
                    "Extension 'ai' not found. Attempting to install via pgai python package..."
                )
                import pgai

                # pgai.install expects a database URL string
                pgai.install(settings.resolved_sink_url)
                logger.info("Extension 'ai' installed successfully.")

            # Check if vectorizer already exists
            await cur.execute(
                "SELECT 1 FROM ai.vectorizer WHERE source_table::text = %s",
                (settings.sink_raw_table,),
            )
            if not await cur.fetchone():
                logger.info(
                    f"Creating pgai vectorizer for {settings.sink_raw_table}..."
                )
                # We need to configure the ollama host for the worker
                # In pgai, this is often done via environment variables for the worker,
                # but we can also set it in the session if needed.
                await cur.execute(
                    f"""
                    SELECT ai.create_vectorizer(
                        '{settings.sink_raw_table}'::regclass,
                        loading => ai.loading_column('{settings.content_column}'),
                        embedding => ai.embedding_{settings.embedding_provider}('{settings.embedding_model}', {settings.embedding_dimension}),
                        chunking => ai.chunking_{settings.chunking_strategy}(),
                        formatting => ai.formatting_python_template('{settings.formatting_template}')
                    )
                """
                )

            # Create the compatibility View
            embedding_table = f"{settings.sink_raw_table}_embedding"
            # We use the same concatenation logic as pgai's Python template
            # 'Product: $name Description: $chunk'
            # Note: $chunk corresponds to the content_column after chunking.
            view_content_sql = f"'Product: ' || COALESCE(r.name, '') || ' Description: ' || COALESCE(r.{settings.content_column}, '')"
            await cur.execute(
                f"""
                CREATE OR REPLACE VIEW {settings.sink_replica_table} AS
                SELECT 
                    r.{settings.id_column},
                    {view_content_sql} as {settings.target_content_column},
                    e.{settings.embedding_column}
                FROM {settings.sink_raw_table} r
                LEFT JOIN {embedding_table} e ON r.{settings.id_column} = e.{settings.id_column}
            """
            )

            # IMPORTANT: Enable pgai triggers for native replication
            # We find all triggers starting with _vectorizer_ and enable them ALWAYS
            await cur.execute(
                f"""
                DO $$
                DECLARE
                    trg_name TEXT;
                BEGIN
                    FOR trg_name IN 
                        SELECT trigger_name 
                        FROM information_schema.triggers 
                        WHERE event_object_table = '{settings.sink_raw_table}' 
                        AND trigger_name LIKE '_vectorizer_%'
                    LOOP
                        EXECUTE 'ALTER TABLE {settings.sink_raw_table} ENABLE ALWAYS TRIGGER ' || quote_ident(trg_name);
                    END LOOP;
                END $$;
            """
            )

            # Subscription
            await cur.execute(
                f"SELECT 1 FROM pg_subscription WHERE subname = '{settings.subscription_name}'"
            )
            if not await cur.fetchone():
                options = ", ".join(
                    [
                        f"{k} = {v}"
                        for k, v in settings.subscription_options.items()
                    ]
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


async def drop_subscription_completely(settings: Settings):
    """Drop subscription and slot from source on shutdown."""
    logger.info("Dropping subscription and slot from source...")
    try:
        async with await get_sink_conn() as conn:
            await conn.set_autocommit(True)
            await conn.execute(
                f"DROP SUBSCRIPTION IF EXISTS {settings.subscription_name}"
            )
    except Exception as e:
        logger.warning(f"Failed to drop subscription: {e}")


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
                # We use pg_wal_lsn_diff to get bytes between current WAL and slot's restart_lsn
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
