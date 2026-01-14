import asyncio
import logging
import signal
import sys
import uvicorn
from typing import Callable
from pythonjsonlogger.json import JsonFormatter
from .config import settings
from .orchestrator import Orchestrator
from .database import (
    drop_subscription_completely,
    connect_db,
    check_and_protect_source,
    init_pools,
    close_pools,
)
from .observability import (
    update_replication_lag,
    update_pgai_pending,
    app as observability_app,
)
from .utils import wait_until

# Configure structured JSON logging
logHandler = logging.StreamHandler(sys.stdout)
formatter = JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
logHandler.setFormatter(formatter)

# Clear existing handlers and set up our JSON handler
root_logger = logging.getLogger()
root_logger.handlers = [logHandler]
root_logger.setLevel(logging.INFO)

logger = logging.getLogger(__name__)


async def log_pgai_status():
    """Poll pgai status and log progress or errors."""
    try:
        async with await connect_db(settings.resolved_sink_url) as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT name, source_table, pending_items FROM ai.vectorizer_status")
                results = await cur.fetchall()
                for row in results:
                    logger.info(f"pgai Status for {row['name']} ({row['source_table']}): {row['pending_items']} items pending")
                    update_pgai_pending(row["name"], int(row["pending_items"]))
    except Exception: pass


async def run_daemon(loop: asyncio.AbstractEventLoop, handle_exit: Callable[[], None]) -> None:
    """Main loop for the replicator daemon."""
    logger.info("Daemon started. Monitoring multi-table source health...")
    stop_event = asyncio.Event()

    async def monitoring_worker():
        while not stop_event.is_set():
            for name in list(settings.pipelines.keys()):
                try:
                    lag_mb = await check_and_protect_source(settings, name)
                    update_replication_lag(name, lag_mb)
                except RuntimeError as e:
                    if "Self-destructed" in str(e):
                        logger.critical(f"Daemon target {name} stopping: {e}")
                except Exception as e:
                    logger.error(f"Error in monitoring worker for {name}: {e}")
            
            await log_pgai_status()
            await asyncio.sleep(30)

    try:
        await monitoring_worker()
    except asyncio.CancelledError:
        stop_event.set()
        raise


async def main():
    loop = asyncio.get_running_loop()
    
    # 1. Start Control Plane API
    config = uvicorn.Config(observability_app, host=settings.observability_host, port=settings.observability_port, log_level="error")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())

    # 2. Start Orchestrator (Manages DB, Workers, Reconciliation)
    orchestrator = Orchestrator(settings)
    
    # Handle Shutdown
    def handle_exit():
        logger.info("Shutdown signal received...")
        server.should_exit = True
        asyncio.create_task(orchestrator.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_exit)

    # 3. Running
    logger.info("Starting PGSearchReplica Orchestrator...")
    try:
        await orchestrator.start()
        # Wait for API server to finish (it finishes on SIGTERM)
        await server_task
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.critical(f"Orchestrator failed: {e}")
        await orchestrator.stop()
        sys.exit(1)
    finally:
        await orchestrator.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
