import logging
import psycopg
from pgvector.psycopg import register_vector_async as register_vector  # type: ignore
from src.config import settings

logger = logging.getLogger(__name__)


async def connect_db(url: str, **kwargs):
    """Connect to database."""
    conn = await psycopg.AsyncConnection.connect(url, **kwargs)
    return conn


async def setup_source():
    """Remotely initialize the source publication."""
    logger.info("Setting up remote source publication...")
    async with await connect_db(settings.source_url, autocommit=True) as conn:
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


async def setup_sink():
    """Initialize the sink table and subscription."""
    logger.info("Setting up local sink database...")
    async with await connect_db(settings.sink_url, autocommit=True) as conn:
        async with conn.cursor() as cur:
            # Extensions
            await cur.execute("CREATE EXTENSION IF NOT EXISTS vector")

        # Register pgvector ONLY after it exists in the DB
        await register_vector(conn)

        async with conn.cursor() as cur:
            # Tables
            await cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {settings.sink_raw_table} (
                    {settings.id_column} INT PRIMARY KEY,
                    {settings.content_column} TEXT,
                    processed BOOLEAN DEFAULT FALSE
                )
            """
            )

            await cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {settings.sink_replica_table} (
                    {settings.id_column} INT PRIMARY KEY,
                    {settings.target_content_column} TEXT,
                    {settings.embedding_column} vector({settings.embedding_dimension}),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Notification Trigger
            await cur.execute(
                f"""
                CREATE OR REPLACE FUNCTION notify_new_raw_data() RETURNS trigger AS $$
                BEGIN
                    IF (TG_OP = 'UPDATE') THEN
                        -- Only reset if actual data changed
                        IF (OLD.{settings.content_column} IS DISTINCT FROM NEW.{settings.content_column}) THEN
                            NEW.processed := FALSE;
                        END IF;
                    END IF;
                    PERFORM pg_notify('{settings.notify_channel}', '');
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            """
            )
            await cur.execute(
                f"""
                DROP TRIGGER IF EXISTS trg_new_raw_data ON {settings.sink_raw_table};
                CREATE TRIGGER trg_new_raw_data 
                BEFORE INSERT OR UPDATE ON {settings.sink_raw_table} 
                FOR EACH ROW EXECUTE FUNCTION notify_new_raw_data();
                
                -- Ensure trigger fires even for native replication
                ALTER TABLE {settings.sink_raw_table} ENABLE ALWAYS TRIGGER trg_new_raw_data;
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
                    CONNECTION '{settings.source_url}' 
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


async def drop_subscription_completely():
    """Drop subscription and slot from source on shutdown."""
    logger.info("Dropping subscription and slot from source...")
    try:
        async with await connect_db(settings.sink_url, autocommit=True) as conn:
            await conn.execute(
                f"DROP SUBSCRIPTION IF EXISTS {settings.subscription_name}"
            )
    except Exception as e:
        logger.warning(f"Failed to drop subscription: {e}")


async def check_and_protect_source():
    """
    Monitor replication lag on Source and self-destruct if it exceeds safety limits.
    Returns True if safe, raises Exception if self-destruct triggered.
    """
    try:
        async with await connect_db(settings.source_url) as conn:
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
                    lag_mb = res[0]
                    if lag_mb > settings.max_slot_wal_keep_size_mb:
                        logger.critical(
                            f"REPLICATION LAG ({lag_mb:.1f} MB) EXCEEDED SAFETY LIMIT ({settings.max_slot_wal_keep_size_mb} MB)!"
                        )
                        logger.critical(
                            "Emergency shutdown: Dropping subscription to protect Source DB disk space."
                        )
                        await drop_subscription_completely()
                        raise RuntimeError(
                            "Self-destructed to protect Source DB."
                        )
                    elif lag_mb > (settings.max_slot_wal_keep_size_mb * 0.8):
                        logger.warning(
                            f"High replication lag detected: {lag_mb:.1f} MB (Limit: {settings.max_slot_wal_keep_size_mb} MB)"
                        )
        return True
    except Exception as e:
        if "Self-destructed" in str(e):
            raise
        logger.warning(f"Failed to check replication lag: {e}")
        return True


async def get_unprocessed_rows(conn):
    """Fetch rows from raw sink table that haven't been transformed yet."""
    cols = ", ".join(settings.publication_columns)
    async with conn.cursor() as cur:
        await cur.execute(
            f"SELECT {cols} FROM {settings.sink_raw_table} WHERE processed = FALSE FOR UPDATE SKIP LOCKED LIMIT %s",
            (settings.batch_size,),
        )
        return await cur.fetchall()


async def mark_rows_processed(conn, ids):
    """Mark a batch of rows as processed in the raw table."""
    async with conn.cursor() as cur:
        await cur.execute(
            f"UPDATE {settings.sink_raw_table} SET processed = TRUE WHERE {settings.id_column} = ANY(%s)",
            (ids,),
        )


async def upsert_replica_batch(conn, batch):
    """Perform a bulk upsert into the final replica table."""
    async with conn.cursor() as cur:
        await cur.executemany(
            f"""
            INSERT INTO {settings.sink_replica_table} ({settings.id_column}, {settings.target_content_column}, {settings.embedding_column}, updated_at)
            VALUES (%({settings.id_column})s, %({settings.target_content_column})s, %({settings.embedding_column})s, CURRENT_TIMESTAMP)
            ON CONFLICT ({settings.id_column}) DO UPDATE SET
                {settings.target_content_column} = EXCLUDED.{settings.target_content_column},
                {settings.embedding_column} = EXCLUDED.{settings.embedding_column},
                updated_at = EXCLUDED.updated_at
            WHERE {settings.sink_replica_table}.{settings.target_content_column} IS DISTINCT FROM EXCLUDED.{settings.target_content_column}
        """,
            batch,
        )
