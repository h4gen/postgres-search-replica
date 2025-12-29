import logging
import psycopg
from pgvector.psycopg import register_vector_async as register_vector
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
                    f"CREATE PUBLICATION {settings.publication_name} FOR TABLE users ({cols}){where_clause}"
                )
            else:
                logger.info(
                    f"Syncing publication {settings.publication_name} with columns ({cols}){where_clause}..."
                )
                await cur.execute(
                    f"ALTER PUBLICATION {settings.publication_name} SET TABLE users ({cols}){where_clause}"
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
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INT PRIMARY KEY,
                    email TEXT,
                    processed BOOLEAN DEFAULT FALSE
                )
            """
            )

            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users_replica (
                    id INT PRIMARY KEY,
                    transformed_email TEXT,
                    embedding vector(3),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Notification Trigger
            await cur.execute(
                """
                CREATE OR REPLACE FUNCTION notify_new_user_raw() RETURNS trigger AS $$
                BEGIN
                    IF (TG_OP = 'UPDATE') THEN
                        -- Only reset if actual data changed
                        IF (OLD.email IS DISTINCT FROM NEW.email) THEN
                            NEW.processed := FALSE;
                        END IF;
                    END IF;
                    PERFORM pg_notify('new_raw_data', '');
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            """
            )
            await cur.execute(
                """
                DROP TRIGGER IF EXISTS trg_new_user_raw ON users;
                CREATE TRIGGER trg_new_user_raw 
                BEFORE INSERT OR UPDATE ON users 
                FOR EACH ROW EXECUTE FUNCTION notify_new_user_raw();
                
                -- Ensure trigger fires even for native replication
                ALTER TABLE users ENABLE ALWAYS TRIGGER trg_new_user_raw;
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


async def get_unprocessed_rows(conn):
    """Fetch rows from users that haven't been transformed yet."""
    cols = ", ".join(settings.publication_columns)
    async with conn.cursor() as cur:
        await cur.execute(f"SELECT {cols} FROM users WHERE processed = FALSE")
        return await cur.fetchall()


async def mark_rows_processed(conn, ids):
    """Mark a batch of rows as processed in the raw table."""
    async with conn.cursor() as cur:
        await cur.execute(
            "UPDATE users SET processed = TRUE WHERE id = ANY(%s)", (ids,)
        )


async def upsert_replica_batch(conn, batch):
    """Perform a bulk upsert into the final replica table."""
    async with conn.cursor() as cur:
        await cur.executemany(
            """
            INSERT INTO users_replica (id, transformed_email, embedding, updated_at)
            VALUES (%(id)s, %(transformed_email)s, %(embedding)s, CURRENT_TIMESTAMP)
            ON CONFLICT (id) DO UPDATE SET
                transformed_email = EXCLUDED.transformed_email,
                embedding = EXCLUDED.embedding,
                updated_at = EXCLUDED.updated_at
            WHERE users_replica.transformed_email IS DISTINCT FROM EXCLUDED.transformed_email
        """,
            batch,
        )
