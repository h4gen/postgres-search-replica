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
            await cur.execute(
                f"SELECT 1 FROM pg_publication WHERE pubname = '{settings.publication_name}'"
            )
            if not await cur.fetchone():
                cols = ", ".join(settings.publication_columns)
                logger.info(
                    f"Creating publication {settings.publication_name} on Source for columns ({cols})..."
                )
                await cur.execute(
                    f"CREATE PUBLICATION {settings.publication_name} FOR TABLE users ({cols})"
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
                AFTER INSERT OR UPDATE ON users 
                FOR EACH ROW EXECUTE FUNCTION notify_new_user_raw();
            """
            )

            # Subscription
            await cur.execute(
                f"SELECT 1 FROM pg_subscription WHERE subname = '{settings.subscription_name}'"
            )
            if not await cur.fetchone():
                logger.info(
                    f"Creating subscription {settings.subscription_name}..."
                )
                await cur.execute(
                    f"""
                    CREATE SUBSCRIPTION {settings.subscription_name} 
                    CONNECTION '{settings.source_url}' 
                    PUBLICATION {settings.publication_name}
                """
                )
            else:
                await cur.execute(
                    f"ALTER SUBSCRIPTION {settings.subscription_name} ENABLE"
                )


async def disable_subscription():
    """Gracefully disable subscription on shutdown."""
    logger.info("Disabling subscription...")
    try:
        async with await connect_db(settings.sink_url, autocommit=True) as conn:
            await conn.execute(
                f"ALTER SUBSCRIPTION {settings.subscription_name} DISABLE"
            )
    except Exception as e:
        logger.warning(f"Failed to disable subscription: {e}")


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
        """,
            batch,
        )
