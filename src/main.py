import asyncio
import logging
import signal
from src.config import settings
from src.database import (
    setup_source,
    setup_sink,
    disable_subscription,
    get_unprocessed_rows,
    mark_rows_processed,
    upsert_replica_batch,
    connect_db,
)
from src.transformer import transform_user_data

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def process_cycle():
    """Single cycle of transformation and movement."""
    try:
        async with await connect_db(settings.sink_url, autocommit=True) as conn:
            rows = await get_unprocessed_rows(conn)
            if not rows:
                return

            # Transform (isolated logic)
            batch = transform_user_data(rows)

            # Upsert & Mark
            await upsert_replica_batch(conn, batch)
            ids = [r["id"] for r in batch]
            await mark_rows_processed(conn, ids)

            logger.info(f"Successfully processed {len(batch)} rows.")
    except Exception as e:
        logger.error(f"Error in processing cycle: {e}")


async def run_daemon():
    """Main loop for the replicator daemon."""
    await setup_source()
    await setup_sink()

    logger.info("Daemon started. Listening for notifications...")

    # We use two tasks: one for NOTIFY and one for periodic Polling (Heartbeat)
    # This is much more robust than a single wait_for.

    stop_event = asyncio.Event()

    async def notification_worker():
        async with await connect_db(settings.sink_url, autocommit=True) as conn:
            await conn.execute("LISTEN new_raw_data")
            async for _ in conn.notifies():
                await process_cycle()
                if stop_event.is_set():
                    break

    async def polling_worker():
        while not stop_event.is_set():
            await process_cycle()
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

    task = asyncio.create_task(run_daemon())

    def handle_exit():
        logger.info("Shutdown signal received...")
        task.cancel()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_exit)

    try:
        await task
    except asyncio.CancelledError:
        logger.info("Daemon task cancelled.")
    finally:
        await disable_subscription()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
