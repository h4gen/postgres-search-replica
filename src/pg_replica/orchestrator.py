import asyncio
import logging
import subprocess
from datetime import timedelta
from typing import Optional

from .config import Settings
from .reconciler import Reconciler
from .database import (
    drop_subscription_completely,
    check_and_protect_source,
    connect_db,
    init_pools,
    close_pools,
)
from .observability import update_replication_lag
from pgai.vectorizer.worker import Worker
from .mirror_worker import MirrorWorker
from .utils import wait_until

logger = logging.getLogger(__name__)


class Orchestrator:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._pg_process: Optional[subprocess.Popen] = None
        self._tasks: list[asyncio.Task] = []
        self._stop_event = asyncio.Event()

    async def _is_port_open(self, port: int) -> bool:
        try:
            _, writer = await asyncio.open_connection("localhost", port)
            writer.close()
            await writer.wait_closed()
            return True
        except Exception:
            return False

    async def _wait_for_pg(self, port: int, timeout: int = 30):
        async def pg_ready():
            if await self._is_port_open(port):
                try:
                    async with await connect_db(self.settings.resolved_sink_url):
                        return True
                except Exception:
                    pass
            return False

        logger.info(f"Waiting for Postgres on port {port}...")
        try:
            await wait_until(pg_ready, timeout=timeout, interval=1.0)
            return True
        except asyncio.TimeoutError:
            return False

    def _start_local_postgres(self):
        """Starts a local Postgres process if sink_url is 'local'."""
        if self.settings.sink_url != "local":
            return

        data_dir = self.settings.data_dir
        if not (data_dir / "PG_VERSION").exists():
            logger.info(
                f"Initializing new Postgres data directory at {data_dir}..."
            )
            data_dir.mkdir(parents=True, exist_ok=True)
            subprocess.run(
                ["initdb", "-D", str(data_dir)], check=True, capture_output=True
            )
            # Allow all connections for dev/container usage
            with open(data_dir / "pg_hba.conf", "a") as f:
                f.write("\nhost all all all trust\n")

        port = self.settings.local_port
        logger.info(f"Starting local Postgres on port {port}...")

        # We use a custom unix socket directory to avoid collisions
        run_dir = self.settings.base_dir / "run"
        run_dir.mkdir(parents=True, exist_ok=True)

        self._pg_process = subprocess.Popen(
            [
                "postgres",
                "-D",
                str(data_dir),
                "-p",
                str(port),
                "-k",
                str(run_dir),
                # Ensure we have enough connections and extensions can load
                "-c",
                "max_connections=100",
                "-c",
                "shared_preload_libraries=vector",
                "-c",
                "listen_addresses=*",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        # Start a thread to pipe Postgres logs to our logger
        def pipe_logs(proc, logger):
            for line in iter(proc.stdout.readline, ""):
                if line:
                    logger.info(f"[Postgres] {line.strip()}")
        
        import threading
        threading.Thread(target=pipe_logs, args=(self._pg_process, logger), daemon=True).start()

    async def _log_pgai_status(self):
        """Poll pgai status and log progress."""
        try:
            async with await connect_db(self.settings.resolved_sink_url) as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute("SELECT name, source_table, pending_items FROM ai.vectorizer_status")
                    results = await cur.fetchall()
                    for row in results:
                        logger.info(f"pgai Status for {row['name']} ({row['source_table']}): {row['pending_items']} items pending")
        except Exception: pass

    async def _replication_loop(self):
        """Main loop for the replicator daemon logic."""
        logger.info("Starting replication watchdog...")
        while not self._stop_event.is_set():
            for name in list(self.settings.pipelines.keys()):
                try:
                    lag_mb = await check_and_protect_source(self.settings, name)
                    update_replication_lag(name, lag_mb)
                except RuntimeError as e:
                    if "Self-destructed" in str(e):
                        logger.critical(f"Replicator target {name} stopped: {e}")
                        logger.info(f"Attempting to auto-heal {name} in 2s...")
                        try:
                            await asyncio.sleep(2.0)
                            await self.reconciler.reconcile()
                            logger.info(f"Auto-heal for {name} successful.")
                        except Exception as re:
                            logger.error(f"Auto-heal failed: {re}")
                except Exception as e:
                    logger.error(f"Error in watchdog for {name}: {e}")
            
            await self._log_pgai_status()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=2)
            except asyncio.TimeoutError:
                continue

    async def _supervised_run(self, name: str, factory):
        """Supervises a worker task, restarting it on failure."""
        logger.info(f"Starting supervised worker: {name}")
        while not self._stop_event.is_set():
            try:
                worker = factory()
                # Run the worker. If it returns, it finished (unexpected for long-running).
                # If it raises, we catch and restart.
                await worker.run()
            except asyncio.CancelledError:
                logger.info(f"Worker {name} cancelled.")
                break
            except Exception as e:
                logger.error(f"Worker {name} crashed: {e}. Restarting in 2s...", exc_info=True)
                try:
                    await asyncio.sleep(2.0)
                except asyncio.CancelledError:
                    break
        logger.info(f"Worker {name} stopped.")

    async def start(self):
        """Start all managed services."""
        if self.settings.sink_url == "local":
            self._start_local_postgres()
            if not await self._wait_for_pg(self.settings.local_port):
                raise RuntimeError("Failed to start local Postgres")
            logger.info("Local Postgres is ready.")

        await init_pools(self.settings)
        
        # 1. Ensure outbox infrastructure exists globally before starting workers
        from .database import ensure_outbox_infrastructure
        await ensure_outbox_infrastructure(self.settings)

        self.reconciler = Reconciler(self.settings)
        
        async def try_reconcile():
            try:
                await self.reconciler.reconcile()
                return True
            except Exception as e:
                logger.warning(f"Reconciliation attempt failed: {e}")
                return False

        try:
            await wait_until(try_reconcile, timeout=60.0, interval=5.0, message="Failed to reconcile search infrastructure")
        except asyncio.TimeoutError as e:
            raise RuntimeError(str(e))

        # Supervise pgai worker
        self._tasks.append(asyncio.create_task(
            self._supervised_run(
                "pgai_worker", 
                lambda: Worker(db_url=self.settings.resolved_sink_url, poll_interval=timedelta(seconds=2.0))
            ), 
            name="pgai_worker_supervisor"
        ))
        
        # Supervise mirror worker
        self._tasks.append(asyncio.create_task(
            self._supervised_run(
                "mirror_worker",
                lambda: MirrorWorker(self.settings)
            ),
            name="mirror_worker_supervisor"
        ))
        
         self._tasks.append(asyncio.create_task(self._replication_loop(), name="watchdog"))

    async def stop(self):
        """Gracefully stop all services."""
        logger.info("Shutting down orchestrator...")
        self._stop_event.set()

        if self._tasks:
            for task in self._tasks: task.cancel()
            try:
                await asyncio.wait_for(asyncio.gather(*self._tasks, return_exceptions=True), timeout=15.0)
            except asyncio.TimeoutError: pass
            self._tasks = []

        # Drop infrastructure for ALL tables
        for name in list(self.settings.pipelines.keys()):
            try:
                config = self.settings.pipelines.get(name)
                if config:
                    await asyncio.wait_for(drop_subscription_completely(self.settings, config, name), timeout=20.0)
            except Exception as e:
                logger.debug(f"Failed to drop {name}: {e}")

        await close_pools()
        if self._pg_process:
            logger.info("Stopping local Postgres...")
            self._pg_process.terminate()
            try:
                self._pg_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._pg_process.kill()
            self._pg_process = None
            logger.info("Local Postgres stopped.")
