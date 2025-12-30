import asyncio
import logging
import signal
from typing import Callable
from src.config import settings
from src.database import (
    setup_source,
    setup_sink,
    drop_subscription_completely,
    connect_db,
    check_and_protect_source,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def log_pgai_status():
    """Poll pgai status and log progress or errors."""
    try:
        async with await connect_db(settings.sink_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT source_table, pending_items FROM ai.vectorizer_status"
                )
                results = await cur.fetchall()
                for table, pending in results:
                    logger.info(
                        f"pgai Status for {table}: {pending} items pending"
                    )
    except Exception as e:
        logger.warning(f"Failed to fetch pgai status: {e}")


async def run_daemon(
    loop: asyncio.AbstractEventLoop, handle_exit: Callable[[], None]
) -> None:
    """Main loop for the replicator daemon."""
    await setup_source()
    await setup_sink()

    logger.info("Daemon started. Monitoring source health and pgai status...")

    stop_event = asyncio.Event()

    async def monitoring_worker():
        while not stop_event.is_set():
            try:
                # Watchdog: Protect source from lag
                await check_and_protect_source()
                # Observability: Log pgai status
                await log_pgai_status()
            except RuntimeError as e:
                if "Self-destructed" in str(e):
                    logger.critical(f"Daemon stopping: {e}")
                    stop_event.set()
                    loop.call_soon(handle_exit)
                    break
            except Exception as e:
                logger.error(f"Error in monitoring worker: {e}")
            await asyncio.sleep(30)

    try:
        await monitoring_worker()
    except asyncio.CancelledError:
        stop_event.set()
        raise


async def main():
    loop = asyncio.get_running_loop()

    # Run setup before starting the daemon with retries
    # This handles cases where Postgres is starting or in recovery
    max_retries = 5
    for i in range(max_retries):
        try:
            await setup_source()
            await setup_sink()
            break
        except Exception as e:
            if i == max_retries - 1:
                logger.critical(
                    f"Failed to setup Source/Sink after {max_retries} attempts: {e}"
                )
                return
            logger.warning(
                f"Setup attempt {i+1} failed (Postgres might be in recovery). Retrying in 5s... Error: {e}"
            )
            await asyncio.sleep(5)

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
