import asyncio
import logging
import signal
import sys
import subprocess
import uvicorn
from typing import Callable, Optional
from pythonjsonlogger.json import JsonFormatter
from .config import settings
from .reconciler import Reconciler
from .database import (
    drop_subscription_completely,
    connect_db,
    check_and_protect_source,
    init_pools,
    close_pools,
)
from .observability import app as observability_app
from .metrics import update_replication_lag, update_pgai_pending_items as update_pgai_pending
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
            for name in list(settings.tables.keys()):
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


async def _is_port_open(port: int) -> bool:
    try:
        _, writer = await asyncio.open_connection("localhost", port)
        writer.close()
        await writer.wait_closed()
        return True
    except Exception:
        return False

async def main():
    loop = asyncio.get_running_loop()
    
    pg_process: Optional[subprocess.Popen] = None
    if settings.sink_url == "local":
        data_dir = settings.data_dir
        if not (data_dir / "PG_VERSION").exists():
            logger.info(f"Initializing new Postgres data directory at {data_dir}...")
            data_dir.mkdir(parents=True, exist_ok=True)
            subprocess.run(["initdb", "-D", str(data_dir)], check=True, capture_output=True)
            with open(data_dir / "pg_hba.conf", "a") as f:
                f.write("\nhost all all all trust\n")

        port = settings.local_port
        logger.info(f"Starting local Postgres on port {port}...")
        run_dir = settings.base_dir / "run"
        run_dir.mkdir(parents=True, exist_ok=True)

        pg_process = subprocess.Popen([
            "postgres", "-D", str(data_dir), "-p", str(port), "-k", str(run_dir),
            "-c", "max_connections=100", "-c", "shared_preload_libraries=vector", "-c", "listen_addresses=*"
        ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        
        async def wait_for_pg():
            for _ in range(30):
                if await _is_port_open(port):
                    try:
                        async with await connect_db(settings.resolved_sink_url):
                            return True
                    except Exception: pass
                await asyncio.sleep(1)
            return False

        if not await wait_for_pg():
            logger.critical("Failed to start local Postgres")
            return

    await init_pools(settings)

    config = uvicorn.Config(observability_app, host=settings.observability_host, port=settings.observability_port, log_level="info")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())

    reconciler = Reconciler(settings)
    
    async def try_reconcile():
        try:
            await reconciler.reconcile()
            return True
        except Exception as e:
            logger.warning(f"Reconciliation attempt failed: {e}")
            return False

    try:
        await wait_until(try_reconcile, timeout=60.0, interval=5.0, message="Failed to reconcile search infrastructure")
    except asyncio.TimeoutError as e:
        logger.critical(str(e))
        await close_pools()
        return

    def handle_exit():
        logger.info("Shutdown signal received...")
        task.cancel()
        server.should_exit = True

    task = asyncio.create_task(run_daemon(loop, handle_exit))
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_exit)

    try:
        await task
    except asyncio.CancelledError:
        logger.info("Daemon task cancelled.")
    finally:
        for name, config_obj in settings.tables.items():
            try:
                await drop_subscription_completely(settings, config_obj, name)
            except Exception: pass
        await close_pools()
        await server_task
        if pg_process:
            logger.info("Stopping local Postgres...")
            pg_process.terminate()
            pg_process.wait(timeout=10)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
