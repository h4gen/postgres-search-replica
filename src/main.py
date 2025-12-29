import asyncio
import logging
import signal
from typing import Callable
from pgvector.psycopg import register_vector_async as register_vector  # type: ignore
from src.config import settings
from src.database import (
    setup_source,
    setup_sink,
    drop_subscription_completely,
    get_unprocessed_rows,
    mark_rows_processed,
    upsert_replica_batch,
    connect_db,
    check_and_protect_source,
)
from src.transformer import transform_data

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def process_cycle():
    """Single cycle of transformation and movement."""
    try:
        # Phase 1: Fetch (Fast, short lock)
        async with await connect_db(settings.sink_url) as conn:
            rows = await get_unprocessed_rows(conn)
            if not rows:
                return

        # Phase 2: Transform (Slow, external APIs, NO DB connection held)
        batch = transform_data(rows)

        # Phase 3: Atomic Commit (Fast)
        async with await connect_db(settings.sink_url) as conn:
            await register_vector(conn)
            async with conn.transaction():
                # Upsert and Mark processed succeed or fail together
                await upsert_replica_batch(conn, batch)
                ids = [r[settings.id_column] for r in batch]
                await mark_rows_processed(conn, ids)

            logger.info(f"Successfully processed {len(batch)} rows.")
    except Exception as e:
        logger.error(f"Error in processing cycle: {e}")


async def run_daemon(
    loop: asyncio.AbstractEventLoop, handle_exit: Callable[[], None]
):
    """Main loop for the replicator daemon."""
    await setup_source()
    await setup_sink()

    logger.info("Daemon started. Listening for notifications...")

    # We use two tasks: one for NOTIFY and one for periodic Polling (Heartbeat)
    # This is much more robust than a single wait_for.

    stop_event = asyncio.Event()

    async def notification_worker():
        async with await connect_db(settings.sink_url, autocommit=True) as conn:
            await conn.execute(f"LISTEN {settings.notify_channel}")
            async for _ in conn.notifies():
                await process_cycle()
                if stop_event.is_set():
                    break

    async def polling_worker():
        while not stop_event.is_set():
            try:
                # Watchdog: Protect source from lag
                await check_and_protect_source()
                await process_cycle()
            except RuntimeError as e:
                if "Self-destructed" in str(e):
                    logger.critical(f"Daemon stopping: {e}")
                    stop_event.set()
                    # Trigger shutdown of the other task
                    loop.call_soon(handle_exit)
                    break
            except Exception as e:
                logger.error(f"Error in polling worker: {e}")
            await asyncio.sleep(30)  # Polling every 30s as fallback

    try:
        await asyncio.gather(notification_worker(), polling_worker())
    except asyncio.CancelledError:
        stop_event.set()
        raise


async def main():
    loop = asyncio.get_running_loop()

    # Run setup before starting the daemon
    await setup_source()
    await setup_sink()

    def handle_exit():
        logger.info("Shutdown signal received...")
        task.cancel()

    task = asyncio.create_task(run_daemon(loop, handle_exit))

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_exit)

    try:
        await task
    except asyncio.CancelledError:
        logger.info("Daemon task cancelled.")
    finally:
        await drop_subscription_completely()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
