import asyncio
import os
import logging
import signal
import psycopg
import polars as pl
import numpy as np

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

SOURCE_URL = os.getenv("SOURCE_URL")
SINK_URL = os.getenv("SINK_URL")
PUBLICATION_NAME = "pub_users"
SUBSCRIPTION_NAME = "sub_users"


async def setup_replication():
    """Sets up native logical replication between Source and Sink."""
    logger.info("Setting up native logical replication...")

    # 1. Setup Source (Publication)
    async with await psycopg.AsyncConnection.connect(
        SOURCE_URL, autocommit=True
    ) as conn_src:
        async with conn_src.cursor() as cur:
            await cur.execute(
                f"SELECT 1 FROM pg_publication WHERE pubname = '{PUBLICATION_NAME}'"
            )
            if not await cur.fetchone():
                logger.info(
                    f"Creating publication {PUBLICATION_NAME} on Source..."
                )
                await cur.execute(
                    f"CREATE PUBLICATION {PUBLICATION_NAME} FOR TABLE users (id, email)"
                )

    # 2. Setup Sink (Table, Trigger, Subscription)
    async with await psycopg.AsyncConnection.connect(
        SINK_URL, autocommit=True
    ) as conn_sink:
        async with conn_sink.cursor() as cur:
            # Enable pgvector extension
            await cur.execute("CREATE EXTENSION IF NOT EXISTS vector")

            # Raw table to receive native replication
            # Name must match source table name for native logical replication
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INT PRIMARY KEY,
                    email TEXT,
                    processed BOOLEAN DEFAULT FALSE
                )
            """
            )

            # Final replica table with vector support
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users_replica (
                    id INT PRIMARY KEY,
                    transformed_email TEXT,
                    embedding vector(3), -- Placeholder: 3D vector
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Trigger to notify Python of new rows
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

            # Native Subscription
            await cur.execute(
                f"SELECT 1 FROM pg_subscription WHERE subname = '{SUBSCRIPTION_NAME}'"
            )
            if not await cur.fetchone():
                logger.info(
                    f"Creating subscription {SUBSCRIPTION_NAME} on Sink..."
                )
                await cur.execute(
                    f"""
                    CREATE SUBSCRIPTION {SUBSCRIPTION_NAME} 
                    CONNECTION '{SOURCE_URL}' 
                    PUBLICATION {PUBLICATION_NAME}
                """
                )
            else:
                logger.info(
                    f"Subscription {SUBSCRIPTION_NAME} already exists. Enabling..."
                )
                await cur.execute(
                    f"ALTER SUBSCRIPTION {SUBSCRIPTION_NAME} ENABLE"
                )


async def transform_and_move():
    """Pulls from users, transforms with Polars, and writes to users_replica."""
    async with await psycopg.AsyncConnection.connect(
        SINK_URL, autocommit=True
    ) as conn:
        async with conn.cursor() as cur:
            # 1. Fetch unprocessed rows
            await cur.execute(
                "SELECT id, email FROM users WHERE processed = FALSE"
            )
            rows = await cur.fetchall()

            if not rows:
                return

            # 2. Transform with Polars
            df = pl.DataFrame(rows, schema=["id", "email"], orient="row")

            # Simulate an embedding for each email
            # In a real app, you'd use a model here
            transformed = df.with_columns(
                [
                    pl.col("email")
                    .str.to_lowercase()
                    .str.replace(r"@.*", "@masked-replica.com")
                    .alias("transformed_email"),
                    # Generating a dummy 3D vector [len, first_char_code, last_char_code] normalized
                    pl.col("email")
                    .map_elements(
                        lambda x: np.random.rand(3).tolist(),
                        return_dtype=pl.List(pl.Float64),
                    )
                    .alias("embedding"),
                ]
            ).select(["id", "transformed_email", "embedding"])

            # 3. Batch Upsert to Final Table
            batch = transformed.to_dicts()
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

            # 4. Mark as processed
            ids = transformed["id"].to_list()
            await cur.execute(
                "UPDATE users SET processed = TRUE WHERE id = ANY(%s)",
                (ids,),
            )

            logger.info(
                f"Successfully transformed and moved {len(batch)} rows."
            )


async def run_daemon():
    await setup_replication()

    logger.info("Daemon started. Listening for changes...")

    async with await psycopg.AsyncConnection.connect(
        SINK_URL, autocommit=True
    ) as conn:
        await conn.execute("LISTEN new_raw_data")

        # Initial check for any existing data
        await transform_and_move()

        # Listen loop
        gen = conn.notifies()
        while True:
            try:
                # Wait for a notification (with timeout to occasionally poll just in case)
                await asyncio.wait_for(anext(gen), timeout=10.0)
                await transform_and_move()
            except asyncio.TimeoutError:
                # Periodic check
                await transform_and_move()
            except StopAsyncIteration:
                break


async def cleanup():
    logger.info("Disabling subscription...")
    try:
        async with await psycopg.AsyncConnection.connect(
            SINK_URL, autocommit=True
        ) as conn:
            async with conn.cursor() as cur:
                # We disable instead of dropping to maintain the LSN position
                await cur.execute(
                    f"ALTER SUBSCRIPTION {SUBSCRIPTION_NAME} DISABLE"
                )
    except Exception as e:
        logger.warning(f"Failed to disable subscription: {e}")


if __name__ == "__main__":

    async def main():
        loop = asyncio.get_running_loop()
        task = asyncio.create_task(run_daemon())

        def handle_exit():
            logger.info("Shutdown signal received...")
            task.cancel()

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, handle_exit)

        try:
            await asyncio.sleep(5)  # Wait for DBs to be up
            await task
        except asyncio.CancelledError:
            logger.info("Daemon task cancelled.")
        finally:
            await cleanup()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
