import asyncio
import logging
import signal
import sys
from datetime import timedelta
from pgai.vectorizer.worker import Worker
from pythonjsonlogger.json import JsonFormatter
from .config import settings

# Configure structured JSON logging
logHandler = logging.StreamHandler(sys.stdout)
formatter = JsonFormatter(
    "%(asctime)s %(levelname)s %(name)s %(message)s"
)
logHandler.setFormatter(formatter)

# Clear existing handlers and set up our JSON handler
root_logger = logging.getLogger()
root_logger.handlers = [logHandler]
root_logger.setLevel(logging.INFO)

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
        db_url=settings.resolved_sink_url, poll_interval=timedelta(seconds=2.0)
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
