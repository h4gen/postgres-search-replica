import asyncio
import logging
import signal
from datetime import timedelta
from pgai.vectorizer.worker import Worker
from .config import settings

# Configure logging to match the main daemon
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def run_worker():
    """Run the official pgai worker logic in a lightweight loop."""
    logger.info(
        f"Starting lightweight pgai worker (db: {settings.sink_url})..."
    )

    # Initialize the official pgai Worker
    # Note: We use the URL from our settings.
    # The Worker class handles the polling and API calls to Ollama/etc.
    worker = Worker(
        db_url=settings.sink_url, poll_interval=timedelta(seconds=2.0)
    )

    # Handle graceful shutdown
    stop_event = asyncio.Event()

    def handle_exit():
        logger.info("Shutdown signal received for pgai worker...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_exit)

    # run() is a long-running task that polls for work
    worker_task = asyncio.create_task(worker.run())

    # Wait until we are told to stop
    await stop_event.wait()

    # Attempt graceful shutdown of the worker task
    logger.info("Stopping pgai worker task...")
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        logger.info("pgai worker task stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Worker daemon failed: {e}")
        exit(1)
